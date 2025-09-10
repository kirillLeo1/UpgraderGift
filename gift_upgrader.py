import asyncio
import json
import logging
import os
import random
import signal
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv
from prometheus_client import Counter, Gauge, start_http_server
from telethon import TelegramClient, functions, types, events
from telethon.errors import FloodWaitError, RPCError

# ────────────────────────────────────────────────────────────────────────────────
# ENV
# ────────────────────────────────────────────────────────────────────────────────

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "stars_upgrader")

# Кого сканируем: "me", "@my_channel", "-100123..."
PEERS = [p.strip() for p in os.getenv("PEERS", "me").split(",") if p.strip()]

# Период цикла и джиттер
CHECK_EVERY_SEC = float(os.getenv("CHECK_EVERY_SEC", "600"))
JITTER_MAX_SEC = float(os.getenv("JITTER_MAX_SEC", "0"))     # например 2.0

# Реактивный триггер по новым сообщениям в канале
FAST_ON_NEW_MSG = os.getenv("FAST_ON_NEW_MSG", "1") == "1"

# Сухой прогон (ничего не платим, только логика)
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

# Куда слать короткий отчёт
REPORT_PEER = os.getenv("REPORT_PEER", "me")

# Порт метрик Prometheus (0 — отключить)
PROM_PORT = int(os.getenv("PROM_PORT", "8008"))

# Логи
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path(os.getenv("LOG_DIR", "./logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Пагинация
PAGE_LIMIT = max(1, min(100, int(os.getenv("PAGE_LIMIT", "100"))))

# Сохранять детали исходника при апгрейде (если TL-слой поддерживает)
KEEP_ORIGINAL_DETAILS = os.getenv("KEEP_ORIGINAL_DETAILS", "1") == "1"

# Файлики состояния/аудита
AUDIT_JSONL = LOG_DIR / "audit.jsonl"
STATE_DB = LOG_DIR / "upgraded_state.json"

# ────────────────────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("gift_upgrader")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S%z")
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
logger.addHandler(ch)
fh = RotatingFileHandler(LOG_DIR / "gift_upgrader.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus metrics
# ────────────────────────────────────────────────────────────────────────────────

MET_CHECKS = Counter("tg_gift_checks_total", "Scan cycles")
MET_GIFTS_SCANNED = Counter("tg_gift_scanned_total", "Saved gifts scanned")
MET_GIFTS_UPGRADABLE = Counter("tg_gift_upgradable_total", "Gifts upgradable")
MET_UPGR_ATTEMPTS = Counter("tg_gift_upgrade_attempts_total", "Upgrade attempts")
MET_UPGR_SUCCESS = Counter("tg_gift_upgrade_success_total", "Upgrades ok")
MET_ERRORS = Counter("tg_gift_errors_total", "Errors")
MET_FLOODWAIT = Counter("tg_gift_floodwait_seconds_total", "FLOOD_WAIT seconds")
G_BALANCE = Gauge("tg_stars_balance", "Stars balance (XTR)")
G_LAST_RUN_TS = Gauge("tg_last_run_timestamp", "Last scan UNIX ts")

# ────────────────────────────────────────────────────────────────────────────────
# Tiny KV state
# ────────────────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_DB.exists():
        try:
            return json.loads(STATE_DB.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_state(d: dict) -> None:
    tmp = STATE_DB.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_DB)

STATE = load_state()

def state_key(peer_id, gift_key: str) -> str:
    return f"{peer_id}:{gift_key}"

def already_upgraded(peer_id, gift_key: str) -> bool:
    return state_key(peer_id, gift_key) in STATE

def mark_upgraded(peer_id, gift_key: str) -> None:
    STATE[state_key(peer_id, gift_key)] = int(datetime.now(timezone.utc).timestamp())
    save_state(STATE)

def append_audit(event: dict) -> None:
    event["ts"] = datetime.now(timezone.utc).isoformat()
    with AUDIT_JSONL.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _require_tl_class(name: str):
    obj = getattr(types, name, None)
    if obj is None:
        raise RuntimeError(
            f"Telethon не содержит {name}. Обнови пакет:  pip install -U telethon"
        )
    return obj

def as_int_stars(x) -> int:
    """
    Приводит StarsAmount/число/строку к int.
    """
    if x is None:
        return 0
    if isinstance(x, (int, float)):
        return int(x)
    if isinstance(x, str):
        try:
            return int(float(x))
        except Exception:
            return 0
    for attr in ("amount", "value", "units"):
        v = getattr(x, attr, None)
        if v is not None:
            try:
                return int(v)
            except Exception:
                pass
    to_dict = getattr(x, "to_dict", None)
    if callable(to_dict):
        d = to_dict()
        for k in ("amount", "value", "units"):
            if k in d:
                try:
                    return int(d[k])
                except Exception:
                    pass
    try:
        return int(x)
    except Exception:
        return 0

# Динамически тянем нужные TL-классы (совместимость разных слоёв)
InputPeerSelf = _require_tl_class("InputPeerSelf")
InputSavedStarGiftUser = _require_tl_class("InputSavedStarGiftUser")
InputSavedStarGiftChat = _require_tl_class("InputSavedStarGiftChat")
InputInvoiceStarGiftUpgrade = _require_tl_class("InputInvoiceStarGiftUpgrade")

# ────────────────────────────────────────────────────────────────────────────────
# Upgrader
# ────────────────────────────────────────────────────────────────────────────────

class Upgrader:
    def __init__(self, client: TelegramClient):
        self.client = client

    async def report(self, text: str) -> None:
        try:
            await self.client.send_message(REPORT_PEER, text, link_preview=False)
        except Exception as e:
            logger.warning(f"Report failed: {e}")

    async def get_balance(self) -> int:
        """
        Баланс звёзд: поддерживаем обе сигнатуры GetStarsStatusRequest (с peer и без)
        и разные представления суммы (int / StarsAmount).
        """
        try:
            st = await self.client(functions.payments.GetStarsStatusRequest(
                peer=InputPeerSelf()
            ))
        except TypeError:
            st = await self.client(functions.payments.GetStarsStatusRequest())
        except RPCError as e:
            logger.warning(f"getStarsStatus RPC error: {e}; balance=0")
            G_BALANCE.set(0)
            return 0
        except Exception as e:
            logger.warning(f"getStarsStatus unexpected: {e}; balance=0")
            G_BALANCE.set(0)
            return 0

        raw_bal = getattr(st, "balance", 0)
        bal = as_int_stars(raw_bal)
        if bal == 0 and raw_bal not in (0, None):
            logger.info(f"Balance came as {type(raw_bal).__name__}: {raw_bal!r} -> parsed 0")
        G_BALANCE.set(bal)
        return bal

    async def resolve_peer(self, p: str):
        if str(p).lower() == "me":
            return InputPeerSelf()
        try:
            if isinstance(p, str) and (p.startswith("-") or p.isdigit()):
                p = int(p)
        except Exception:
            pass
        return await self.client.get_input_entity(p)

    async def iter_saved_gifts(self, peer, limit: int = PAGE_LIMIT):
        """
        Пагинация: payments.getSavedStarGifts(peer, offset, limit, exclude_unique=True)
        """
        offset = ""
        while True:
            res = await self.client(functions.payments.GetSavedStarGiftsRequest(
                peer=peer,
                offset=offset,
                limit=limit,
                exclude_unique=True  # уже уникальные (апгрейженные) не нужны
            ))
            gifts = getattr(res, "gifts", []) or []
            for g in gifts:
                yield g
            MET_GIFTS_SCANNED.inc(len(gifts))
            offset = getattr(res, "next_offset", None)
            if not offset:
                break

    @staticmethod
    def _gift_need_and_flags(saved) -> tuple[int, bool, bool]:
        # upgrade_stars может лежать и в saved, и внутри saved.gift
        need = as_int_stars(getattr(saved, "upgrade_stars", 0))
        sg = getattr(saved, "gift", None)
        if sg is not None:
            need = as_int_stars(getattr(sg, "upgrade_stars", need))
        prepaid = bool(getattr(saved, "prepaid_upgrade_hash", None))
        can_upgrade = bool(getattr(saved, "can_upgrade", False))
        return need, prepaid, can_upgrade

    @staticmethod
    def _gift_keys(saved, peer) -> tuple[str, object, str]:
        """
        Возвращает (gift_key, input_saved_stargift, key_type)
        """
        msg_id = getattr(saved, "msg_id", None)
        saved_id = getattr(saved, "saved_id", None)

        if saved_id:
            inp = InputSavedStarGiftChat(peer=peer, saved_id=saved_id)
            return (f"chat_saved:{saved_id}", inp, "chat_saved")
        if msg_id:
            inp = InputSavedStarGiftUser(msg_id=msg_id)
            return (f"user_msg:{msg_id}", inp, "user_msg")

        raise RuntimeError("Не удалось собрать InputSavedStarGift (нет msg_id/saved_id)")

    async def _upgrade_prepaid(self, inp, keep_original: bool) -> None:
        """
        Аккуратно дергаем UpgradeStarGiftRequest.
        В некоторых слоях нет параметра keep_original_details — пробуем обе сигнатуры.
        """
        try:
            await self.client(functions.payments.UpgradeStarGiftRequest(
                stargift=inp,
                keep_original_details=keep_original
            ))
        except TypeError:
            # слой без параметра — второй заход
            await self.client(functions.payments.UpgradeStarGiftRequest(
                stargift=inp
            ))

    async def _upgrade_paid(self, inp, need: int, keep_original: bool) -> None:
        """
        Платный апгрейд: собираем invoice -> paymentForm -> sendStarsForm
        """
        invoice = InputInvoiceStarGiftUpgrade(
            stargift=inp,
            keep_original_details=keep_original
        )
        try:
            payform = await self.client(functions.payments.GetPaymentFormRequest(invoice=invoice))
        except TypeError:
            # редкий слой: если ругнётся на поле, пробуем без него (сохранность деталей не критична)
            invoice = InputInvoiceStarGiftUpgrade(stargift=inp)
            payform = await self.client(functions.payments.GetPaymentFormRequest(invoice=invoice))

        await self.client(functions.payments.SendStarsFormRequest(
            form_id=payform.form_id,
            invoice=invoice
        ))

    async def try_upgrade_one(self, saved, peer, stars_balance: int):
        """
        Возвращает (ok: bool, msg: str, spent: int)
        """
        need, prepaid, can_up = self._gift_need_and_flags(saved)
        key, inp, key_type = self._gift_keys(saved, peer)
        append_audit({"ev": "consider", "key": key, "peer": str(peer), "need": need, "prepaid": prepaid, "can_up": can_up})

        # Дедупликация одной и той же штуки
        if already_upgraded(str(peer), key):
            return False, f"skip: already_done ({key})", 0

        if not can_up and not prepaid and need <= 0:
            return False, f"skip: not_upgradable ({key})", 0

        MET_GIFTS_UPGRADABLE.inc()

        # 1) Предоплаченный апгрейд
        if prepaid:
            MET_UPGR_ATTEMPTS.inc()
            if DRY_RUN:
                logger.info(f"[{key}] DRY_RUN prepaid upgradeStarGift")
                append_audit({"ev": "dry_upgrade_prepaid", "key": key})
                mark_upgraded(str(peer), key)
                return True, "prepaid-upgrade (dry)", 0
            try:
                await self._upgrade_prepaid(inp, KEEP_ORIGINAL_DETAILS)
                MET_UPGR_SUCCESS.inc()
                append_audit({"ev": "upgrade_prepaid_ok", "key": key})
                mark_upgraded(str(peer), key)
                return True, "prepaid-upgrade OK", 0
            except RPCError as e:
                logger.warning(f"[{key}] prepaid failed: {e}; trying paid flow")

        # 2) Платный апгрейд
        if need > 0:
            if stars_balance < need:
                return False, f"no-balance: need {need}, have {stars_balance}", 0

            MET_UPGR_ATTEMPTS.inc()
            if DRY_RUN:
                logger.info(f"[{key}] DRY_RUN paid upgrade need={need}")
                append_audit({"ev": "dry_upgrade_paid", "key": key, "need": need})
                mark_upgraded(str(peer), key)
                return True, f"paid-upgrade (dry) need={need}", 0

            try:
                await self._upgrade_paid(inp, need, KEEP_ORIGINAL_DETAILS)
                MET_UPGR_SUCCESS.inc()
                append_audit({"ev": "upgrade_paid_ok", "key": key, "need": need})
                mark_upgraded(str(peer), key)
                return True, f"paid-upgrade OK need={need}", need
            except RPCError as e:
                MET_ERRORS.inc()
                append_audit({"ev": "upgrade_paid_err", "key": key, "err": str(e)})
                return False, f"paid-upgrade ERROR: {e}", 0

        return False, f"skip: uncertain ({key})", 0

    async def scan_and_upgrade_cycle(self) -> None:
        G_LAST_RUN_TS.set_to_current_time()
        MET_CHECKS.inc()

        # Баланс перед стартом
        try:
            balance = await self.get_balance()
            logger.info(f"Stars balance: {balance}")
        except RPCError as e:
            MET_ERRORS.inc()
            logger.error(f"getStarsStatus failed: {e}")
            balance = 0

        total_found = 0
        total_upgraded = 0
        total_spent = 0

        for p in PEERS:
            try:
                peer = await self.resolve_peer(p)
            except Exception as e:
                MET_ERRORS.inc()
                logger.error(f"resolve peer '{p}' failed: {e}")
                continue

            logger.info(f"Scanning peer: {p}")
            any_found = False
            async for saved in self.iter_saved_gifts(peer, PAGE_LIMIT):
                any_found = True
                total_found += 1
                try:
                    ok, msg, spent = await self.try_upgrade_one(saved, peer, balance)
                    if ok:
                        total_upgraded += 1
                        total_spent += spent
                        if spent > 0:
                            balance = max(0, balance - spent)
                    logger.info(f"[{p}] {msg}")
                except FloodWaitError as fw:
                    MET_FLOODWAIT.inc(fw.seconds)
                    logger.warning(f"FLOOD_WAIT {fw.seconds}s on peer {p}; sleeping…")
                    await asyncio.sleep(fw.seconds + 1)
                except RPCError as e:
                    MET_ERRORS.inc()
                    logger.error(f"[{p}] RPC error: {e}")
                except Exception as e:
                    MET_ERRORS.inc()
                    logger.exception(f"[{p}] Unexpected: {e}")

            if not any_found:
                logger.info(f"No gifts found for peer {p}")

        # Короткий отчёт
        msg = (
            f"🟦 Gift Upgrader\n"
            f"Peers: {', '.join(PEERS)}\n"
            f"Found: {total_found} | Upgraded: {total_upgraded}\n"
            f"Spent (XTR): {total_spent}\n"
            f"DRY_RUN: {'ON' if DRY_RUN else 'OFF'}\n"
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await self.report(msg)

# ────────────────────────────────────────────────────────────────────────────────
# main + reactive trigger
# ────────────────────────────────────────────────────────────────────────────────

STOP = asyncio.Event()
SCAN_LOCK = asyncio.Lock()

def _setup_signals():
    def handler(sig, frame):
        logger.info(f"Got signal {sig}, exiting…")
        STOP.set()
    try:
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
    except Exception:
        # Windows может ругаться на SIGTERM — ок
        pass

async def _delay_with_jitter():
    base = max(0.0, CHECK_EVERY_SEC)
    jitter = random.uniform(0, JITTER_MAX_SEC) if JITTER_MAX_SEC > 0 else 0.0
    delay = base + jitter
    try:
        await asyncio.wait_for(STOP.wait(), timeout=delay)
    except asyncio.TimeoutError:
        pass

async def main():
    if API_ID <= 0 or not API_HASH:
        logger.error("Set API_ID and API_HASH in .env")
        sys.exit(2)

    if PROM_PORT > 0:
        try:
            start_http_server(PROM_PORT)
            logger.info(f"Prometheus metrics on :{PROM_PORT}/metrics")
        except Exception as e:
            logger.warning(f"Prometheus start failed: {e}")

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    upgr = Upgrader(client)

    _setup_signals()
    async with client:
        logger.info("Gift Upgrader started")

        # Подготовим список каналов для реактивного триггера (me слушать смысла нет)
        watch_list = []
        for p in PEERS:
            if str(p).lower() == "me":
                continue
            try:
                ent = await client.get_entity(p)
                watch_list.append(ent)
            except Exception as e:
                logger.warning(f"Fast trigger: cannot resolve '{p}': {e}")

        if FAST_ON_NEW_MSG and watch_list:
            @client.on(events.NewMessage(chats=watch_list))
            async def _fast_trigger(event):
                if SCAN_LOCK.locked():
                    return
                logger.info("Fast trigger: new message detected -> immediate scan")
                try:
                    async with SCAN_LOCK:
                        await upgr.scan_and_upgrade_cycle()
                except Exception as e:
                    logger.warning(f"Fast trigger scan error: {e}")

        # Основной цикл
        while not STOP.is_set():
            try:
                async with SCAN_LOCK:
                    await upgr.scan_and_upgrade_cycle()
            except FloodWaitError as fw:
                MET_FLOODWAIT.inc(fw.seconds)
                logger.warning(f"CYCLE FLOOD_WAIT {fw.seconds}s; sleeping…")
                await asyncio.sleep(fw.seconds + 1)
            except Exception as e:
                MET_ERRORS.inc()
                logger.exception(f"CYCLE error: {e}")

            await _delay_with_jitter()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
