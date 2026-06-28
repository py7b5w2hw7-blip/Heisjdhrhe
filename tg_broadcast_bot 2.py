#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Бот рассылки по личным диалогам Telegram - один файл.

Запуск самого бота: один bot_token. Каждый пользователь привязывает свой
аккаунт в чате с ботом (телефон + код), общие api_id/api_hash уже зашиты.

Зависимости:
    pip install "telethon>=1.36,<2" "aiogram>=3.13,<4" "aiohttp-socks>=0.8" "python-socks[asyncio]>=2.4"

Запуск:
    python tg_broadcast_bot.py
"""

import asyncio
import json
import os
import random
import sys
from datetime import datetime, timezone, timedelta

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, PeerFloodError, UserIsBlockedError,
    UserPrivacyRestrictedError, UserDeactivatedError, UserDeactivatedBanError,
    InputUserDeactivatedError, ForbiddenError, ChatWriteForbiddenError, RPCError,
    SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError,
    PhoneNumberInvalidError, ApiIdInvalidError,
)
from telethon.tl.types import User

from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
)

# ===========================================================================
#  НАСТРОЙКИ - заполните токен бота (api_id/api_hash уже вписаны)
# ===========================================================================
BOT_TOKEN = "8995219026:AAGROJzMs26Yu3zirlLWvE7MN8Jhw9H2LqU"                                   # токен от @BotFather
API_ID = 30769068                                # общий api_id для аккаунтов
API_HASH = "05d4dca24d5b77edfac75e281eaaeb6d"    # общий api_hash
PROXY = ""        # для аккаунтов (Telethon), можно MTProxy; пусто = напрямую/VPN
BOT_PROXY = ""    # для самого бота (socks5/http); пусто = напрямую/VPN
# Эти значения можно переопределить файлом config.json рядом со скриптом.
# ===========================================================================

if getattr(sys, "frozen", False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
SENT_PATH = os.path.join(BASE_DIR, "sent.json")
BLACKLIST_PATH = os.path.join(BASE_DIR, "blacklist.txt")

CONNECT_RETRIES = 3
CONNECT_TIMEOUT = 10


# ---------------------------------------------------------------------------
# Ядро рассылки
# ---------------------------------------------------------------------------

def parse_proxy(value):
    """Строка прокси -> (proxy_arg, connection_class) для Telethon."""
    if not value or not str(value).strip():
        return None, None
    s = str(value).strip()

    if "t.me/proxy" in s or s.startswith("tg://proxy"):
        from urllib.parse import urlparse, parse_qs
        q = parse_qs(urlparse(s).query)
        server = (q.get("server") or [None])[0]
        port = (q.get("port") or [None])[0]
        secret = (q.get("secret") or [None])[0]
        if not (server and port and secret):
            raise ValueError("В ссылке MTProxy нет server/port/secret")
        from telethon.network import ConnectionTcpMTProxyRandomizedIntermediate
        return (server, int(port), secret), \
            ConnectionTcpMTProxyRandomizedIntermediate

    if "://" in s:
        from urllib.parse import urlparse
        u = urlparse(s)
        proxy = {"proxy_type": u.scheme, "addr": u.hostname,
                 "port": int(u.port), "rdns": True}
        if u.username:
            proxy["username"] = u.username
            proxy["password"] = u.password or ""
        return proxy, None

    parts = s.split(":")
    if len(parts) == 3:
        from telethon.network import ConnectionTcpMTProxyRandomizedIntermediate
        return (parts[0], int(parts[1]), parts[2]), \
            ConnectionTcpMTProxyRandomizedIntermediate
    if len(parts) == 2:
        return {"proxy_type": "socks5", "addr": parts[0],
                "port": int(parts[1]), "rdns": True}, None

    raise ValueError(f"Не понял формат прокси: {s!r}")


def load_sent():
    if not os.path.exists(SENT_PATH):
        return set()
    try:
        with open(SENT_PATH, "r", encoding="utf-8") as f:
            return set(json.load(f).get("sent_ids", []))
    except (json.JSONDecodeError, OSError):
        return set()


def save_sent(sent_ids):
    tmp = SENT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"sent_ids": sorted(sent_ids)}, f, ensure_ascii=False, indent=2)
    os.replace(tmp, SENT_PATH)


def load_blacklist():
    if not os.path.exists(BLACKLIST_PATH):
        return set()
    out = set()
    try:
        with open(BLACKLIST_PATH, "r", encoding="utf-8") as f:
            for line in f:
                item = line.strip().lstrip("@").lower()
                if item and not item.startswith("#"):
                    out.add(item)
    except OSError:
        pass
    return out


def is_target_user(dialog, skip_non_contacts):
    if not dialog.is_user:
        return False
    entity = dialog.entity
    if not isinstance(entity, User):
        return False
    if entity.bot or entity.is_self or getattr(entity, "deleted", False) \
            or entity.support:
        return False
    if skip_non_contacts and not entity.contact:
        return False
    return True


def display_name(entity):
    parts = [entity.first_name, entity.last_name]
    name = " ".join(p for p in parts if p)
    if entity.username:
        name = f"{name} (@{entity.username})".strip()
    return name or f"id{entity.id}"


async def _sleep_interruptible(seconds, should_stop):
    remaining = float(seconds)
    while remaining > 0:
        if should_stop and should_stop():
            return
        chunk = 0.5 if remaining > 0.5 else remaining
        await asyncio.sleep(chunk)
        remaining -= chunk


def _segment(cfg):
    seg = cfg.get("segment")
    if seg:
        return str(seg).lower()
    return "unread" if cfg.get("only_unread") else "all"


async def collect_targets(client, cfg, should_stop=None):
    blacklist = load_blacklist()
    segment = _segment(cfg)
    days = int(cfg.get("active_within_days") or 0)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days) if days > 0 else None

    targets = []
    async for dialog in client.iter_dialogs():
        if should_stop and should_stop():
            break
        if not is_target_user(dialog, cfg.get("skip_non_contacts")):
            continue
        entity = dialog.entity
        uname = (entity.username or "").lower()
        if uname in blacklist or str(entity.id) in blacklist:
            continue
        unread = bool(dialog.unread_count)
        if segment == "unread" and not unread:
            continue
        if segment == "read" and unread:
            continue
        if cutoff is not None and dialog.date and dialog.date < cutoff:
            continue
        targets.append(entity)
    return targets


async def list_unread(client, cfg=None, should_stop=None):
    skip_non_contacts = bool(cfg.get("skip_non_contacts")) if cfg else False
    out = []
    async for dialog in client.iter_dialogs():
        if should_stop and should_stop():
            break
        if not is_target_user(dialog, skip_non_contacts):
            continue
        if dialog.unread_count:
            out.append((dialog.entity, dialog.unread_count))
    return out


async def _send_one(client, entity, cfg):
    photo = cfg.get("photo")
    if photo and os.path.exists(photo):
        await client.send_file(entity, photo, caption=cfg["message"])
    else:
        await client.send_message(entity, cfg["message"])


async def broadcast(client, cfg, sent_ids, on_log, on_progress=None,
                    should_stop=None, targets=None, save_fn=None):
    persist = save_fn or save_sent

    def emit(stats):
        if on_progress:
            on_progress(stats)

    client.parse_mode = cfg.get("parse_mode")
    if targets is None:
        targets = await collect_targets(client, cfg, should_stop)

    total = len(targets)
    pending = [e for e in targets if e.id not in sent_ids]
    pending_count = len(pending)
    already = total - pending_count
    on_log(f"Найдено диалогов: {total}. Уже было: {already}. "
           f"К отправке: {pending_count}.")

    stats = {"total": total, "pending": pending_count, "sent": 0,
             "already": already, "skipped": 0, "error": 0,
             "done": 0, "stopped": False}
    emit(stats)

    for i, entity in enumerate(pending, start=1):
        if should_stop and should_stop():
            stats["stopped"] = True
            on_log("Остановлено пользователем.")
            break

        name = display_name(entity)
        prefix = f"[{i}/{pending_count}] {name}"
        stats["done"] = i
        abort = False
        try:
            if cfg["dry_run"]:
                on_log(f"{prefix} - [DRY-RUN]")
                stats["sent"] += 1
            else:
                await _send_one(client, entity, cfg)
                on_log(f"{prefix} - отправлено")
                stats["sent"] += 1
                sent_ids.add(entity.id)
                persist(sent_ids)
        except FloodWaitError as e:
            if e.seconds > cfg["max_floodwait_seconds"]:
                stats["stopped"] = True
                abort = True
                on_log(f"{prefix} - FloodWait {e.seconds} сек > лимита. Стоп.")
            else:
                wait = e.seconds + 5
                on_log(f"{prefix} - FloodWait: ждём {wait} сек...")
                await _sleep_interruptible(wait, should_stop)
                try:
                    if not cfg["dry_run"]:
                        await _send_one(client, entity, cfg)
                        sent_ids.add(entity.id)
                        persist(sent_ids)
                    stats["sent"] += 1
                    on_log(f"{prefix} - отправлено (после ожидания)")
                except PeerFloodError:
                    stats["error"] += 1
                    stats["stopped"] = True
                    abort = True
                    on_log(f"{prefix} - PeerFloodError после ожидания!")
                except RPCError as e2:
                    stats["error"] += 1
                    on_log(f"{prefix} - ошибка: {e2.__class__.__name__}")
        except PeerFloodError:
            stats["error"] += 1
            stats["stopped"] = True
            abort = True
            on_log(f"{prefix} - PeerFloodError! Telegram ограничил рассылку.")
        except (UserIsBlockedError, UserPrivacyRestrictedError,
                ChatWriteForbiddenError, ForbiddenError):
            stats["skipped"] += 1
            on_log(f"{prefix} - нельзя писать, пропуск.")
        except (UserDeactivatedError, UserDeactivatedBanError,
                InputUserDeactivatedError):
            stats["skipped"] += 1
            on_log(f"{prefix} - аккаунт удалён, пропуск.")
        except RPCError as e:
            stats["error"] += 1
            on_log(f"{prefix} - ошибка Telegram: {e.__class__.__name__}")
        except Exception as e:
            stats["error"] += 1
            on_log(f"{prefix} - непредвиденная ошибка: {e!r}")

        emit(stats)
        if abort:
            on_log("!!! ОСТАНОВКА ради безопасности аккаунта. Сделайте паузу.")
            break
        if (i < pending_count and not cfg["dry_run"]
                and not (should_stop and should_stop())):
            delay = random.uniform(cfg["min_delay_seconds"],
                                   cfg["max_delay_seconds"])
            on_log(f"    ждём {delay:.1f} сек...")
            await _sleep_interruptible(delay, should_stop)

    emit(stats)
    return stats


# алиас, чтобы код бота мог обращаться к ядру как core.<функция>
core = sys.modules.get(__name__) or sys.modules.get("__main__")


# ---------------------------------------------------------------------------
# Бот (aiogram)
# ---------------------------------------------------------------------------

USERS_PATH = os.path.join(BASE_DIR, "users.json")
SEGMENTS = ["all", "unread", "read"]
SEGMENT_RU = {"all": "Все", "unread": "Непрочитанные", "read": "Прочитанные"}

DEFAULT_DRAFT = {
    "message": "", "photo": "", "segment": "all",
    "min_delay_seconds": 30, "max_delay_seconds": 60,
    "dry_run": True, "skip_non_contacts": False, "active_within_days": 0,
    "max_floodwait_seconds": 300, "parse_mode": None,
}

CFG = {}
USERS = {}
CLIENTS = {}
DRAFTS = {}
RUN = {}

dp = Dispatcher(storage=MemoryStorage())
BOT = None


class Flow(StatesGroup):
    phone = State()
    code = State()
    password = State()
    text = State()
    photo = State()
    delay = State()


def _has_console():
    return bool(getattr(sys, "stdin", None)) and sys.stdin is not None \
        and sys.stdin.isatty()


def _save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_bot_config():
    cfg = {"bot_token": BOT_TOKEN, "api_id": API_ID, "api_hash": API_HASH,
           "bot_proxy": BOT_PROXY, "proxy": PROXY}
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                for k, v in json.load(f).items():
                    if v not in ("", None):
                        cfg[k] = v
        except (json.JSONDecodeError, OSError):
            pass
    if not cfg.get("bot_token"):
        if _has_console():
            val = input("Введите токен бота (от @BotFather): ").strip()
            while not val:
                val = input("Поле не может быть пустым. Повторите: ").strip()
            cfg["bot_token"] = val
        else:
            raise SystemExit("Укажите BOT_TOKEN в начале файла или в config.json.")
    return cfg


def load_users():
    if not os.path.exists(USERS_PATH):
        return {}
    try:
        with open(USERS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_users():
    _save_json(USERS_PATH, USERS)


def user_sent_path(uid):
    return os.path.join(BASE_DIR, f"sent_{uid}.json")


def load_user_sent(uid):
    p = user_sent_path(uid)
    if not os.path.exists(p):
        return set()
    try:
        with open(p, "r", encoding="utf-8") as f:
            return set(json.load(f).get("sent_ids", []))
    except (json.JSONDecodeError, OSError):
        return set()


def save_user_sent(uid, ids):
    _save_json(user_sent_path(uid), {"sent_ids": sorted(ids)})


def draft(uid):
    if uid not in DRAFTS:
        d = dict(DEFAULT_DRAFT)
        saved = USERS.get(str(uid)) or {}
        for k in d:
            if k in saved:
                d[k] = saved[k]
        DRAFTS[uid] = d
    return DRAFTS[uid]


def persist_user(uid):
    rec = USERS.get(str(uid), {})
    rec.update(draft(uid))
    USERS[str(uid)] = rec
    save_users()


def account_session(uid):
    return os.path.join(BASE_DIR, f"account_{uid}")


def build_account_client(uid):
    proxy, connection = core.parse_proxy(CFG.get("proxy"))
    kwargs = {"connection_retries": CONNECT_RETRIES,
              "retry_delay": 2, "timeout": CONNECT_TIMEOUT}
    if proxy is not None:
        kwargs["proxy"] = proxy
    if connection is not None:
        kwargs["connection"] = connection
    return TelegramClient(account_session(uid),
                          int(CFG["api_id"]), CFG["api_hash"], **kwargs)


async def authorized_client(uid):
    if uid in CLIENTS:
        try:
            if await CLIENTS[uid].is_user_authorized():
                return CLIENTS[uid]
        except Exception:
            pass
    if not os.path.exists(account_session(uid) + ".session"):
        return None
    client = build_account_client(uid)
    try:
        await client.connect()
        if await client.is_user_authorized():
            CLIENTS[uid] = client
            return client
    except Exception:
        return None
    return None


def menu_text(uid):
    d = draft(uid)
    msg = d.get("message") or "не задан"
    preview = (msg[:60] + "…") if len(msg) > 60 else msg
    seg = SEGMENT_RU.get(core._segment(d), "Все")
    return ("Рассылка по личным диалогам\n\n"
            f"Текст: {preview}\n"
            f"Фото: {'есть' if d.get('photo') else 'нет'}\n"
            f"Сегмент: {seg}\n"
            f"Задержка: {d['min_delay_seconds']}-{d['max_delay_seconds']} сек\n"
            f"Режим: {'ПРОБНЫЙ' if d.get('dry_run') else 'БОЕВОЙ'}")


def menu_kb(uid):
    d = draft(uid)
    seg = SEGMENT_RU.get(core._segment(d), "Все")
    dry = "вкл" if d.get("dry_run") else "выкл"
    b = InlineKeyboardButton
    return InlineKeyboardMarkup(inline_keyboard=[
        [b(text="Текст", callback_data="text"),
         b(text="Фото", callback_data="photo")],
        [b(text=f"Сегмент: {seg}", callback_data="seg")],
        [b(text="Задержка", callback_data="delay"),
         b(text=f"Пробный: {dry}", callback_data="dry")],
        [b(text="Список непрочитанных", callback_data="unread")],
        [b(text="Старт", callback_data="start"),
         b(text="Стоп", callback_data="stop")],
    ])


async def show_menu(uid, chat_id):
    await BOT.send_message(chat_id, menu_text(uid), reply_markup=menu_kb(uid))


@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    uid = message.from_user.id
    await state.clear()
    if await authorized_client(uid):
        await message.answer(menu_text(uid), reply_markup=menu_kb(uid))
        return
    await state.set_state(Flow.phone)
    await message.answer(
        "Привязка аккаунта.\n\n"
        "Введите номер телефона вашего аккаунта в формате +79991234567.\n"
        "На него придёт код в Telegram.")


@dp.message(Command("logout"))
async def cmd_logout(message: Message, state: FSMContext):
    uid = message.from_user.id
    await state.clear()
    client = CLIENTS.pop(uid, None)
    try:
        if client:
            await client.log_out()
    except Exception:
        pass
    path = account_session(uid) + ".session"
    if os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass
    await message.answer("Аккаунт отвязан. /start - привязать заново.")


@dp.message(Flow.phone)
async def on_phone(message: Message, state: FSMContext):
    uid = message.from_user.id
    phone = (message.text or "").strip()
    client = build_account_client(uid)
    CLIENTS[uid] = client
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
    except ApiIdInvalidError:
        await state.clear()
        await message.answer("Ошибка api_id/api_hash. Сообщите администратору.")
        return
    except PhoneNumberInvalidError:
        await message.answer("Неверный номер. Введите ещё раз (+7...).")
        return
    except FloodWaitError as e:
        await state.clear()
        await message.answer(f"Слишком часто. Подождите {e.seconds} сек.")
        return
    except Exception as e:
        await state.clear()
        await message.answer(f"Ошибка подключения: {e!r}")
        return
    await state.update_data(phone=phone, phone_code_hash=sent.phone_code_hash)
    await state.set_state(Flow.code)
    await message.answer(
        "Код отправлен в Telegram. Введите его.\n"
        "Совет: вводите с пробелами, например 1 2 3 4 5.")


@dp.message(Flow.code)
async def on_code(message: Message, state: FSMContext):
    uid = message.from_user.id
    data = await state.get_data()
    code = (message.text or "").replace(" ", "").strip()
    client = CLIENTS.get(uid)
    try:
        await client.sign_in(data["phone"], code,
                             phone_code_hash=data["phone_code_hash"])
    except SessionPasswordNeededError:
        await state.set_state(Flow.password)
        await message.answer("Включена двухфакторная защита. Введите пароль.")
        return
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        await message.answer("Неверный или просроченный код. Введите ещё раз "
                             "(или /start - запросить новый).")
        return
    except Exception as e:
        await state.clear()
        await message.answer(f"Ошибка входа: {e!r}")
        return
    await _finish_login(message, state)


@dp.message(Flow.password)
async def on_password(message: Message, state: FSMContext):
    uid = message.from_user.id
    client = CLIENTS.get(uid)
    try:
        await client.sign_in(password=(message.text or "").strip())
    except Exception:
        await message.answer("Неверный пароль. Введите ещё раз.")
        return
    await _finish_login(message, state)


async def _finish_login(message: Message, state: FSMContext):
    uid = message.from_user.id
    await state.clear()
    persist_user(uid)
    me = await CLIENTS[uid].get_me()
    await message.answer(f"Аккаунт привязан: {core.display_name(me)}")
    await show_menu(uid, message.chat.id)


@dp.message(Flow.text)
async def on_text(message: Message, state: FSMContext):
    uid = message.from_user.id
    draft(uid)["message"] = message.text or ""
    await state.clear()
    persist_user(uid)
    await message.answer("Текст сохранён.")
    await show_menu(uid, message.chat.id)


@dp.message(Flow.photo)
async def on_photo(message: Message, state: FSMContext):
    uid = message.from_user.id
    if not message.photo:
        await message.answer("Это не фото. Пришлите изображение.")
        return
    path = os.path.join(BASE_DIR, f"photo_{uid}.jpg")
    await BOT.download(message.photo[-1], destination=path)
    draft(uid)["photo"] = path
    await state.clear()
    persist_user(uid)
    await message.answer("Фото сохранено.")
    await show_menu(uid, message.chat.id)


@dp.message(Flow.delay)
async def on_delay(message: Message, state: FSMContext):
    uid = message.from_user.id
    rng = _parse_delay(message.text or "")
    if not rng:
        await message.answer("Формат: мин-макс, например 30-60.")
        return
    d = draft(uid)
    d["min_delay_seconds"], d["max_delay_seconds"] = rng
    await state.clear()
    persist_user(uid)
    await message.answer(f"Задержка: {rng[0]}-{rng[1]} сек.")
    await show_menu(uid, message.chat.id)


@dp.callback_query()
async def on_callback(cq: CallbackQuery, state: FSMContext):
    uid = cq.from_user.id
    client = await authorized_client(uid)
    if client is None:
        await cq.answer("Сначала привяжите аккаунт: /start", show_alert=True)
        return
    data = cq.data
    d = draft(uid)

    if data == "text":
        await state.set_state(Flow.text)
        await cq.answer()
        await cq.message.answer("Пришлите текст сообщения.")
    elif data == "photo":
        await state.set_state(Flow.photo)
        await cq.answer()
        await cq.message.answer("Пришлите фото (уйдёт с текстом как подпись).")
    elif data == "seg":
        cur = core._segment(d)
        nxt = SEGMENTS[(SEGMENTS.index(cur) + 1) % len(SEGMENTS)] \
            if cur in SEGMENTS else "all"
        d["segment"] = nxt
        persist_user(uid)
        await cq.answer(f"Сегмент: {SEGMENT_RU[nxt]}")
        await _edit_menu(cq)
    elif data == "delay":
        await state.set_state(Flow.delay)
        await cq.answer()
        await cq.message.answer("Пришлите задержку, формат мин-макс (30-60).")
    elif data == "dry":
        d["dry_run"] = not d.get("dry_run")
        persist_user(uid)
        await cq.answer()
        await _edit_menu(cq)
    elif data == "unread":
        await cq.answer("Собираю список...")
        await _show_unread(uid, cq.message.chat.id, client)
    elif data == "start":
        await cq.answer()
        asyncio.create_task(_run_broadcast(uid, cq.message.chat.id, client))
    elif data == "stop":
        r = RUN.get(uid)
        if r and r["running"]:
            r["stop"].set()
            await cq.answer("Останавливаю...")
        else:
            await cq.answer("Рассылка не запущена.")


async def _edit_menu(cq: CallbackQuery):
    try:
        await cq.message.edit_text(menu_text(cq.from_user.id),
                                   reply_markup=menu_kb(cq.from_user.id))
    except Exception:
        pass


async def _show_unread(uid, chat_id, client):
    rows = await core.list_unread(client, draft(uid))
    if not rows:
        await BOT.send_message(chat_id, "Непрочитанных диалогов нет.")
        return
    lines = [f"Непрочитанные ({len(rows)}):"]
    for entity, cnt in rows[:60]:
        lines.append(f"- {core.display_name(entity)}: {cnt}")
    if len(rows) > 60:
        lines.append(f"...и ещё {len(rows) - 60}")
    await BOT.send_message(chat_id, "\n".join(lines))


async def _run_broadcast(uid, chat_id, client):
    r = RUN.get(uid)
    if r and r["running"]:
        await BOT.send_message(chat_id, "Рассылка уже идёт.")
        return
    d = draft(uid)
    if not d.get("message") and not d.get("photo"):
        await BOT.send_message(chat_id, "Сначала задайте текст или фото.")
        return

    stop = asyncio.Event()
    status = await BOT.send_message(chat_id, "Собираю получателей...")
    RUN[uid] = {"running": True, "stop": stop,
                "chat": chat_id, "msg": status.message_id}
    try:
        targets = await core.collect_targets(client, d, stop.is_set)
        sent_ids = load_user_sent(uid)
        pending = [t for t in targets if t.id not in sent_ids]
        if not pending:
            note = " (остальные уже обработаны)" if sent_ids else ""
            await _edit(chat_id, status.message_id, f"Получателей нет{note}.")
            return
        if d.get("dry_run"):
            await _preview(chat_id, status.message_id, uid, pending)
            return
        await _edit(chat_id, status.message_id,
                    f"Рассылка: получателей {len(pending)}. Поехали...")
        stats = await core.broadcast(
            client, d, sent_ids,
            on_log=lambda t: None,
            on_progress=lambda s: _progress(uid, dict(s)),
            should_stop=stop.is_set,
            targets=targets,
            save_fn=lambda ids: save_user_sent(uid, ids),
        )
        await _final(chat_id, status.message_id, uid, stats)
    except Exception as e:
        await BOT.send_message(chat_id, f"Ошибка: {e!r}")
    finally:
        RUN[uid]["running"] = False


async def _preview(chat_id, msg_id, uid, pending):
    seg = SEGMENT_RU.get(core._segment(draft(uid)), "Все")
    await _edit(chat_id, msg_id,
                f"ПРОБНЫЙ прогон. Сегмент: {seg}. Уйдёт получателям: "
                f"{len(pending)}\n(ничего не отправлено)")
    lines = [f"{i}. {core.display_name(e)}" for i, e in enumerate(pending, 1)]
    chunk, size = [], 0
    for line in lines:
        if size + len(line) + 1 > 3500:
            await BOT.send_message(chat_id, "\n".join(chunk))
            chunk, size = [], 0
        chunk.append(line)
        size += len(line) + 1
    if chunk:
        await BOT.send_message(chat_id, "\n".join(chunk))
    await show_menu(uid, chat_id)


def _progress(uid, stats):
    done = stats.get("done", 0)
    if done and done % 3 != 0:
        return
    asyncio.create_task(_update_status(uid, stats))


async def _update_status(uid, stats):
    r = RUN.get(uid)
    if not r:
        return
    text = ("Идёт рассылка...\n"
            f"Отправлено: {stats.get('sent', 0)}   "
            f"Пропущено: {stats.get('skipped', 0) + stats.get('already', 0)}   "
            f"Ошибки: {stats.get('error', 0)}\n"
            f"{stats.get('done', 0)}/{stats.get('pending', 0)}")
    await _edit(r["chat"], r["msg"], text)


async def _final(chat_id, msg_id, uid, stats):
    text = ("Готово.\n"
            f"Всего: {stats['total']}\n"
            f"Отправлено: {stats['sent']}\n"
            f"Уже было: {stats['already']}\n"
            f"Пропущено: {stats['skipped']}\n"
            f"Ошибок: {stats['error']}")
    if stats.get("stopped"):
        text += "\n(остановлено)"
    await _edit(chat_id, msg_id, text)
    await show_menu(uid, chat_id)


async def _edit(chat_id, msg_id, text):
    try:
        await BOT.edit_message_text(text, chat_id=chat_id, message_id=msg_id)
    except Exception:
        pass


def _parse_delay(text):
    try:
        a, b = text.replace(" ", "").split("-")
        a, b = float(a), float(b)
        if a < 0 or b < 0:
            return None
        return (min(a, b), max(a, b))
    except (ValueError, IndexError):
        return None


async def _main():
    global BOT
    bp = CFG.get("bot_proxy")
    session = AiohttpSession(proxy=bp) if bp else None
    BOT = Bot(token=CFG["bot_token"], session=session)
    me = await BOT.get_me()
    print(f"Бот запущен: @{me.username}. /start в чате с ботом "
          "(только телефон и код).")
    await dp.start_polling(BOT)


def main():
    global CFG, USERS
    CFG = load_bot_config()
    USERS = load_users()
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        print("\nОстановлено.")


if __name__ == "__main__":
    main()
