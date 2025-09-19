import asyncio
import sqlite3
import random
from contextlib import closing
from pathlib import Path
import time
from dotenv import load_dotenv
import os
import html
import pyrogram

from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, FileReferenceExpired, RPCError, Unauthorized, AuthKeyUnregistered
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from pyrogram.raw.types import UpdateNewChannelMessage 


# ===== Gemini: новый SDK =====
from google import genai
from google.genai import types

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "userbot_session")
WORKDIR = os.getenv("WORKDIR", ".")
SESSION_STRING = os.getenv("SESSION_STRING")

DEBUG_GEMINI = os.getenv("DEBUG_GEMINI", "1") == "1"   # 1 = подробные логи Gemini
DEBUG_REPLY  = os.getenv("DEBUG_REPLY",  "1") == "1"   # 1 = подробные логи ответов
ENABLE_DISCUSSION_POLLER = os.getenv("ENABLE_DISCUSSION_POLLER", "1") == "1"  # резервный поллер (по умолчанию выкл)

def _short(s: str | None, n: int = 350) -> str:
    if not s:
        return ""
    return s if len(s) <= n else s[:n] + "…[cut]"

def dbg_gemini(msg: str):
    if DEBUG_GEMINI:
        print(msg)

def dbg_reply(msg: str):
    if DEBUG_REPLY:
        print(msg)


def _channel_to_chat_id(channel_id: int) -> int:
    # Telegram raw channel_id -> привычный chat_id (-100xxxxxxxxxx)
    return int(f"-100{channel_id}")

RAW_REPLY = True

# >>> ИСТОЧНИКИ И ЦЕЛЬ <<<
SOURCE_CHATS = [
    _channel_to_chat_id(1423363475), _channel_to_chat_id(1304740791), _channel_to_chat_id(1628148774),
    _channel_to_chat_id(2092838245), _channel_to_chat_id(1096054832), _channel_to_chat_id(1334218632),
    _channel_to_chat_id(1431200947), _channel_to_chat_id(1268741369), _channel_to_chat_id(1647745905),
    _channel_to_chat_id(1980097656), _channel_to_chat_id(1544919663)
]
TARGET_CHAT_ID = -1001676356290
EFFECTIVE_SOURCE_CHATS = [c for c in SOURCE_CHATS if c != TARGET_CHAT_ID]

LINKED_DISCUSSION_ID = None
REPLY_PROBABILITY = float(os.getenv("REPLY_PROBABILITY", "1.0"))  # 0..1

# >>> ЧАСТОТА <<<
ENABLE_LIVE_STREAM = True
POST_EVERY_SECONDS = 80 * 60
PER_CHAT_SCAN_LIMIT = 500

# >>> КОММЕНТАРИИ <<<
ENABLE_AUTO_COMMENTS = True
COMMENT_EVERY_N = 10
CHANNEL_POLL_SECONDS = 10  # как часто сканировать канал на новые посты

# ===== Gemini =====
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client: genai.Client | None = None
if GEMINI_API_KEY:
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)  # новый клиент
        print("✅ Gemini (google-genai) инициализирован")
    except Exception as e:
        print(f"❌ Gemini init error: {e}")
else:
    print("⚠️ GEMINI_API_KEY не найден (будет fallback)")

# ===== антидубли и мета =====
DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "seen.db"

def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_media(
            file_unique_id TEXT PRIMARY KEY,
            ts INTEGER NOT NULL
        )""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS meta(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )""")
        conn.commit()

def is_seen(file_unique_id: str) -> bool:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT 1 FROM seen_media WHERE file_unique_id=?", (file_unique_id,))
        return cur.fetchone() is not None

def mark_seen(file_unique_id: str):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO seen_media(file_unique_id, ts) VALUES(?, ?)",
            (file_unique_id, int(time.time()))
        )
        conn.commit()

def incr_counter(name: str, delta: int = 1) -> int:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT value FROM meta WHERE key=?", (name,))
        row = cur.fetchone()
        val = int(row[0]) if row else 0
        val += delta
        if row:
            conn.execute("UPDATE meta SET value=? WHERE key=?", (str(val), name))
        else:
            conn.execute("INSERT INTO meta(key, value) VALUES(?, ?)", (name, str(val)))
        conn.commit()
        return val

def get_meta(key: str, default: str | None = None) -> str | None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT value FROM meta WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else default

def set_meta(key: str, value: str):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (key, value))
        conn.commit()

# ===== клиент userbot =====
app = Client(
    name=SESSION_NAME,
    api_id=API_ID,
    api_hash=API_HASH,
    workdir=WORKDIR,
    session_string=SESSION_STRING
)

async def send_with_retry(func, *args, **kwargs):
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            print(f"⏰ FloodWait: {e.value}s")
            await asyncio.sleep(e.value + 1)

def _clamp_caption(text: str | None) -> str | None:
    if not text:
        return None
    # лимит подписи к медиа — 1024 символа
    return text[:1024] if len(text) > 1024 else text

def match_image(msg: Message) -> bool:
    return bool(
        msg and (
            msg.photo or
            (msg.document and msg.document.mime_type and msg.document.mime_type.startswith("image/"))
        )
    )

async def send_clean(app: Client, msg: Message, target_id: int | str) -> Message | None:
    caption = _clamp_caption(msg.caption or None)
    if msg.photo:
        return await send_with_retry(
            app.send_photo, chat_id=target_id, photo=msg.photo.file_id,
            caption=caption, parse_mode=ParseMode.HTML
        )
    if msg.document and msg.document.mime_type and msg.document.mime_type.startswith("image/"):
        return await send_with_retry(
            app.send_document, chat_id=target_id, document=msg.document.file_id,
            caption=caption, parse_mode=ParseMode.HTML
        )
    return None

# ---------- Gemini helpers ----------
FALLBACK_SNIPPET = "Тель Хай Сион!"

def _gen_text_sync(prompt: str, max_tokens=200, temperature=0.8) -> str:
    """Синхронный вызов нового SDK google-genai с подробным логом."""
    if not client:
        dbg_gemini("⚠️ [GEMINI] client отсутствует, верну fallback")
        return FALLBACK_SNIPPET
    try:
        dbg_gemini(f"[GEMINI] ⇢ prompt: {_short(prompt)}")
        # В новом SDK достаточно передать СТРОКУ
        resp = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=prompt,
            config=types.GenerateContentConfig(
                max_output_tokens=max_tokens,
                temperature=temperature
            ),
        )
        # извлекаем текст
        txt = (getattr(resp, "text", None) or getattr(resp, "output_text", None) or "").strip()
        if not txt and getattr(resp, "candidates", None):
            parts = []
            for p in getattr(resp.candidates[0].content, "parts", []) or []:
                t = getattr(p, "text", None)
                if t:
                    parts.append(t)
            txt = "\n".join(parts).strip()

        usage  = getattr(resp, "usage_metadata", None)
        dbg_gemini(f"[GEMINI] ⇠ text: {_short(txt)} | tokens={getattr(usage,'total_token_count',None)}")
        return txt or FALLBACK_SNIPPET
    except Exception as e:
        dbg_gemini(f"❌ [GEMINI] exception: {e}")
        return FALLBACK_SNIPPET

async def build_random_code_comment() -> str:
    txt = await asyncio.to_thread(
        _gen_text_sync,
        # Ты просил не убирать стиль — оставляю нейтральную «мудрость»; это комментарии под постами.
        "Генерируйте мудрые мысли, как будто вы еврейский раввин, дающий совет о деньгах, женщинах, мойшах и жизни под солнцем"
    )
    return html.escape(txt)

async def build_reply_for_comment(user_text: str) -> str:
    """Короткий ответ на комментарий с прокидыванием текста комментария в промпт."""
    if not user_text:
        user_text = "."
    prompt = (
        "Ты пишешь короткие остроумные ответы (3–5 предложения) на русскоязычные комментарии. "
        "Твои ответ должен быть анекдотом в стиле про евреев, но не оскорбительным. "
        "Анекдот может быть в формате еврей, русский и немец (или две любых других национальность, но еврей должен быть и он самый мудрый и хитрый) сидят где-то (заходят куда-то)... "
        "Формат: прямой ответ без преамбул и смайлов\n\n"
        f"Комментарий: {user_text}\nОтвет:"
    )
    dbg_gemini(f"[REPLY] build for: {_short(user_text, 200)}")
    txt = await asyncio.to_thread(_gen_text_sync, prompt, max_tokens=80, temperature=0.9)
    dbg_gemini(f"[REPLY] built: {_short(txt, 200)}")
    return html.escape(txt)[:1000] if txt else "Окей."

# ---------- Комментирование поста канала ----------
async def add_comment_to_post(target_msg: Message):
    if not ENABLE_AUTO_COMMENTS:
        return
    text = await build_random_code_comment()
    try:
        discussion_msg = await app.get_discussion_message(target_msg.chat.id, target_msg.id)
        if not discussion_msg:
            print("❌ Нет обсуждения у поста — пропустил")
            return
        await send_with_retry(discussion_msg.reply, text, parse_mode=ParseMode.HTML)
        print(f"✅ Комментарий отправлен к посту {target_msg.id}")
    except RPCError as e:
        print(f"❌ Ошибка при комментировании: {e}")


# ---------- поток из источников ----------
@app.on_message(filters.chat(EFFECTIVE_SOURCE_CHATS) & (filters.photo | filters.document))
async def handler(_, msg: Message):
    if not ENABLE_LIVE_STREAM or not match_image(msg):
        return
    uid = msg.photo.file_unique_id if msg.photo else (msg.document.file_unique_id if msg.document else None)
    if uid and is_seen(uid):
        return
    try:
        sent = await send_clean(app, msg, TARGET_CHAT_ID)
        if uid:
            mark_seen(uid)
        if sent:
            print(f"📤 Отправлено в цель: message_id={sent.id}")
    except FileReferenceExpired:
        fresh = await app.get_messages(msg.chat.id, msg.id)
        sent = await send_clean(app, fresh, TARGET_CHAT_ID)
        if uid:
            mark_seen(uid)
        print(f"📤 Отправлено (refreshed): message_id={sent.id if sent else 'None'}")

# ---------- динамические хендлеры для обсуждения ----------
_HANDLERS_BOUND = False
async def bind_discussion_handlers():
    global _HANDLERS_BOUND
    if _HANDLERS_BOUND or not LINKED_DISCUSSION_ID:
        return

    async def discussion_tap(_, m: Message):
        # Логируем принятие комментария
        txt = (m.text or m.caption or "").strip()
        print(f"[DISCUSSION] id={m.id} reply_to={m.reply_to_message_id} text={_short(txt, 200)}")

    async def discussion_autoreply(_, m: Message):
        if m.from_user and m.from_user.is_self:
            return
        txt = (m.text or m.caption or "").strip()
        if not txt:
            return
        if random.random() > REPLY_PROBABILITY:
            return
        reply_text = await build_reply_for_comment(txt)
        try:
            await app.send_message(
                chat_id=m.chat.id,
                text=reply_text,
                reply_to_message_id=m.id,
                parse_mode=ParseMode.HTML
            )
        except RPCError as e:
            print(f"❌ [REPLY] send failed: {e}")

    app.add_handler(
        # Узкий фильтр — решает маршрутизацию на уровне Pyrogram!
        pyrogram.handlers.MessageHandler(
            discussion_tap,
            filters.chat(LINKED_DISCUSSION_ID) & ~filters.service
        ),
        group=0  # приоритет выше прочих on_message
    )
    app.add_handler(
        pyrogram.handlers.MessageHandler(
            discussion_autoreply,
            filters.chat(LINKED_DISCUSSION_ID) & ~filters.service & ~filters.me
        ),
        group=0
    )

    _HANDLERS_BOUND = True
    print("🔗 Discussion handlers bound (narrow filter)")

# ---------- резервный опрос обсуждения ----------
async def discussion_poll_loop():
    """
    Резервный поллер обсуждения:
    - читает новые сообщения из LINKED_DISCUSSION_ID
    - логирует только непустые
    - по вероятности REPLY_PROBABILITY отвечает
    - всегда двигает оффсет (чтобы не зацикливаться)
    """
    if not LINKED_DISCUSSION_ID:
        print("⚠️ [POLL] LINKED_DISCUSSION_ID не задан — поллер выключен")
        return

    last_id = int(get_meta("last_disc_msg_id", "0") or 0)
    if last_id == 0:
        async for m in app.get_chat_history(LINKED_DISCUSSION_ID, limit=1):
            last_id = m.id
            set_meta("last_disc_msg_id", str(last_id))
            break

    while True:
        try:
            batch = []
            async for m in app.get_chat_history(LINKED_DISCUSSION_ID, limit=50):
                if m.id <= last_id:
                    break
                batch.append(m)

            for m in reversed(batch):
                txt = (m.text or m.caption or "").strip()
                if txt:
                    dbg_reply(f"[DISCUSSION] id={m.id} reply_to={m.reply_to_message_id} text={_short(txt, 200)}")

                # не отвечаем на себя
                if not (m.from_user and m.from_user.is_self) and txt:
                    rnd = random.random()
                    if rnd <= REPLY_PROBABILITY:
                        dbg_reply(f"💬 [POLL] generating for msg_id={m.id}: {_short(txt, 200)}")
                        reply_text = await build_reply_for_comment(txt)
                        dbg_reply(f"💬 [POLL] ready -> {_short(html.unescape(reply_text), 200)}")
                        try:
                            sent = await app.send_message(
                                chat_id=m.chat.id,
                                text=reply_text,
                                reply_to_message_id=m.id,
                                parse_mode=ParseMode.HTML
                            )
                            dbg_reply(f"✅ [POLL] sent reply_id={sent.id}")
                        except FloodWait as e:
                            dbg_reply(f"⏳ [POLL] FloodWait {e.value}s on send; sleeping")
                            await asyncio.sleep(e.value + 1)
                        except RPCError as e:
                            dbg_reply(f"❌ [POLL] send failed: {e}")

                # ВСЕГДА сдвигаем оффсет
                last_id = max(last_id, m.id)
                set_meta("last_disc_msg_id", str(last_id))

        except FloodWait as e:
            dbg_reply(f"⏳ [POLL] FloodWait {e.value}s on fetch; sleeping")
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            dbg_reply(f"[discussion_poll] error: {e}")

        await asyncio.sleep(3)

# ---------- вотчер канала: комментит каждый N-й ----------
async def comment_watcher_loop():
    await asyncio.sleep(5)
    last_scanned = int(get_meta("last_scanned_msg_id", "0") or 0)
    if last_scanned == 0:
        async for m in app.get_chat_history(TARGET_CHAT_ID, limit=1):
            last_scanned = m.id
            set_meta("last_scanned_msg_id", str(last_scanned))
            break
    while True:
        try:
            new_msgs = []
            async for m in app.get_chat_history(TARGET_CHAT_ID, limit=50):
                if m.id <= last_scanned:
                    break
                if m.reply_to_message:
                    continue
                if not (m.text or m.photo or m.document):
                    continue
                new_msgs.append(m)
            for m in reversed(new_msgs):
                cnt = incr_counter("channel_posts_count", 1)
                print(f"📊 Новый пост #{cnt} id={m.id}")
                if ENABLE_AUTO_COMMENTS and COMMENT_EVERY_N > 0 and (cnt % COMMENT_EVERY_N == 0):
                    await add_comment_to_post(m)
                last_scanned = max(last_scanned, m.id)
                set_meta("last_scanned_msg_id", str(last_scanned))
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            print(f"[comment_watcher] error: {e}")
        await asyncio.sleep(CHANNEL_POLL_SECONDS)

# ---------- планировщик постинга ----------
async def scheduler_loop():
    await asyncio.sleep(5)
    while True:
        try:
            if not EFFECTIVE_SOURCE_CHATS:
                await asyncio.sleep(POST_EVERY_SECONDS)
                continue
            m, uid = await pick_random_candidate(EFFECTIVE_SOURCE_CHATS, per_chat_limit=PER_CHAT_SCAN_LIMIT, prefer_unseen=True)
            if m:
                try:
                    await send_clean(app, m, TARGET_CHAT_ID)
                except FileReferenceExpired:
                    fresh = await app.get_messages(m.chat.id, m.id)
                    await send_clean(app, fresh, TARGET_CHAT_ID)
                if uid:
                    mark_seen(uid)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            print(f"[scheduler] error: {e}")
        await asyncio.sleep(POST_EVERY_SECONDS)

# ---------- выбор кандидата ----------
async def pick_random_candidate(sources, per_chat_limit=500, prefer_unseen=True):
    async def collect(chat, include_seen: bool):
        out = []
        async for m in app.get_chat_history(chat, limit=per_chat_limit):
            if not match_image(m):
                continue
            uid = m.photo.file_unique_id if m.photo else (
                m.document.file_unique_id if (m.document and m.document.mime_type and m.document.mime_type.startswith("image/")) else None
            )
            if not uid:
                continue
            if include_seen or not is_seen(uid):
                out.append((m, uid))
        return out

    candidates = []
    if prefer_unseen:
        for c in sources:
            candidates += await collect(c, include_seen=False)
        if not candidates:
            for c in sources:
                candidates += await collect(c, include_seen=True)
    else:
        for c in sources:
            candidates += await collect(c, include_seen=True)

    return random.choice(candidates) if candidates else (None, None)


@app.on_raw_update(group=-1)  # обрабатываем раньше остальных
async def raw_discussion_diag(client: Client, update, users, chats):
    # пропускаем всё, пока не знаем связку
    if not LINKED_DISCUSSION_ID:
        return

    # интересуют только новые сообщения в каналах/супергруппах (в т.ч. комменты)
    if not isinstance(update, UpdateNewChannelMessage):
        return

    raw = update.message
    ch_id = getattr(getattr(raw, "peer_id", None), "channel_id", None)
    if ch_id is None:
        return
    chat_id = _channel_to_chat_id(ch_id)

    # логируем только обсуждение целевого канала
    if chat_id != LINKED_DISCUSSION_ID:
        return

    # 1) «сырые» поля апдейта (видны даже без high-level Message)
    print(
        f"🧩 [RAW] upd in discussion: raw_mid={getattr(raw,'id',None)} "
        f"pts={getattr(update,'pts',None)} pts_count={getattr(update,'pts_count',None)} "
        f"chat_id={chat_id}"
    )

    # 2) пробуем поднять high-level Message (удобнее дальше работать)
    try:
        m: Message = await client.get_messages(chat_id, raw.id)
    except Exception as e:
        print(f"❌ [RAW] get_messages failed chat={chat_id} id={getattr(raw,'id',None)}: {e}")
        return

    txt = (m.text or m.caption or "").strip()
    print(
        f"💡 [RAW] got comment HL id={m.id} out={m.outgoing} "
        f"reply_to={m.reply_to_message_id} top={getattr(m,'reply_to_top_message_id',None)} "
        f"has_text={bool(txt)}"
    )
    if txt:
        print(f"📝 [RAW] text: {_short(txt, 200)}")

    # (необязательно) Ответ прямо из RAW — чтобы зафиксировать полный цикл
    if RAW_REPLY and not m.outgoing and txt:
        try:
            reply_text = await build_reply_for_comment(txt)  # твой промпт не трогаем
            sent = await client.send_message(
                chat_id=m.chat.id,
                text=reply_text,
                reply_to_message_id=m.id,
                parse_mode=ParseMode.HTML
            )
            print(f"✅ [RAW] replied with msg_id={sent.id}")
        except FloodWait as e:
            print(f"⏳ [RAW] FloodWait {e.value}s on send; sleeping")
            await asyncio.sleep(e.value + 1)
        except RPCError as e:
            print(f"❌ [RAW] send failed: {e}")


@app.on_message(filters.me & filters.command("diag", prefixes=[".", "/"]))
async def diag_cmd(_, msg: Message):
    lines = []
    lines.append("🔎 DIAG")
    lines.append(f"TARGET_CHAT_ID={TARGET_CHAT_ID}")
    lines.append(f"LINKED_DISCUSSION_ID={LINKED_DISCUSSION_ID}")
    lines.append(f"REPLY_PROBABILITY={REPLY_PROBABILITY}")
    lines.append(f"ENABLE_DISCUSSION_POLLER={ENABLE_DISCUSSION_POLLER}")
    lines.append(f"GEMINI={'✅' if client else '❌'}")

    # проверка доступа и статуса в обсуждении
    if LINKED_DISCUSSION_ID:
        try:
            me = await app.get_chat_member(LINKED_DISCUSSION_ID, "me")
            lines.append(f"status_in_discussion={getattr(me,'status',None)}")
        except Exception as e:
            lines.append(f"status_in_discussion=ERR {e}")

        # топ последних сообщений
        try:
            cnt = 0
            async for m in app.get_chat_history(LINKED_DISCUSSION_ID, limit=3):
                cnt += 1
                lines.append(f"last[{cnt}] id={m.id} out={m.outgoing} "
                             f"reply_to={m.reply_to_message_id} "
                             f"top={getattr(m,'reply_to_top_message_id',None)} "
                             f"text={_short((m.text or m.caption or ''),60)}")
        except Exception as e:
            lines.append(f"history=ERR {e}")

    await msg.reply_text("\n".join(lines))

# ---------- команды ----------
@app.on_message(filters.me & filters.command("random", prefixes=[".", "/"]))
async def random_cmd(_, msg: Message):
    sources = [msg.command[1]] if len(msg.command) >= 2 else EFFECTIVE_SOURCE_CHATS.copy()
    if not sources:
        await msg.reply_text("EFFECTIVE_SOURCE_CHATS пуст.")
        return
    m, uid = await pick_random_candidate(sources, per_chat_limit=PER_CHAT_SCAN_LIMIT, prefer_unseen=True)
    if not m:
        await msg.reply_text("Кандидатов не нашёл.")
        return
    try:
        await send_clean(app, m, TARGET_CHAT_ID)
        if uid:
            mark_seen(uid)
        await msg.reply_text("Ок, отправил случайный пост.")
    except FileReferenceExpired:
        fresh = await app.get_messages(m.chat.id, m.id)
        await send_clean(app, fresh, TARGET_CHAT_ID)
        if uid:
            mark_seen(uid)
        await msg.reply_text("Ок, отправил (refresh).")

@app.on_message(filters.me & filters.command("test_discussion", prefixes=[".", "/"]))
async def test_discussion_cmd(_, msg: Message):
    """Тестирует обработку комментариев"""
    info = []
    info.append(f"🎯 TARGET_CHAT_ID: {TARGET_CHAT_ID}")
    info.append(f"💬 LINKED_DISCUSSION_ID: {LINKED_DISCUSSION_ID}")
    info.append(f"🎲 REPLY_PROBABILITY: {REPLY_PROBABILITY}")
    info.append(f"🤖 Gemini client: {'✅' if client else '❌'}")
    info.append(f"🔗 Handlers bound: {'✅' if _HANDLERS_BOUND else '❌'}")
    
    if LINKED_DISCUSSION_ID:
        try:
            # Проверяем доступ к группе
            chat = await app.get_chat(LINKED_DISCUSSION_ID)
            info.append(f"✅ Группа: {chat.title}")
            
            # Проверяем последние сообщения
            count = 0
            async for m in app.get_chat_history(LINKED_DISCUSSION_ID, limit=5):
                count += 1
                if m.text or m.caption:
                    info.append(f"📝 Msg {m.id}: {_short(m.text or m.caption or '', 50)}")
            info.append(f"📊 Последних сообщений: {count}")
            
        except Exception as e:
            info.append(f"❌ Ошибка доступа к группе: {e}")
    else:
        info.append("❌ Группа обсуждения не найдена")
    
    await msg.reply_text("\n".join(info))

@app.on_message(filters.me & filters.command("rebind_handlers", prefixes=[".", "/"]))
async def rebind_handlers_cmd(_, msg: Message):
    """Принудительно перепривязывает обработчики"""
    global _HANDLERS_BOUND
    _HANDLERS_BOUND = False
    
    if LINKED_DISCUSSION_ID:
        await bind_discussion_handlers()
        await msg.reply_text(f"✅ Обработчики перепривязаны к {LINKED_DISCUSSION_ID}")
    else:
        await msg.reply_text("❌ LINKED_DISCUSSION_ID не установлен")        

# ---------- запуск ----------
if __name__ == "__main__":
    init_db()
    print("🚀 Starting userbot (interval repost + watcher comments + Gemini)…")

    async def resolve_linked_discussion(ensure_join: bool = True, test_read: bool = True) -> int | None:
        """Ищем связанную группу обсуждений канала и (при необходимости) входим туда."""
        global LINKED_DISCUSSION_ID
        
        try:
            ch = await app.get_chat(TARGET_CHAT_ID)
        except RPCError as e:
            print(f"❌ Не смог получить канал {TARGET_CHAT_ID}: {e}")
            LINKED_DISCUSSION_ID = None
            return None

        linked = getattr(ch, "linked_chat", None)
        if not linked:
            print("❌ У канала нет связанной группы (включи «Обсуждения»).")
            LINKED_DISCUSSION_ID = None
            return None

        linked_id = linked.id
        print(f"✅ Linked discussion ID: {linked_id}")

        # ✅ СНАЧАЛА устанавливаем ID
        LINKED_DISCUSSION_ID = linked_id

        if ensure_join:
            try:
                me = await app.get_chat_member(linked_id, "me")
                status = getattr(me, "status", None)
                print(f"👤 Мой статус в обсуждении: {status}")
            except RPCError:
                status = None
            if not status or str(status).endswith("LEFT") or str(status).endswith("KICKED"):
                try:
                    await app.join_chat(linked_id)
                    print("✅ Вступил в обсуждение")
                except RPCError as e:
                    print(f"⚠️ Не смог вступить: {e}")

        if test_read:
            try:
                async for _ in app.get_chat_history(linked_id, limit=1):
                    pass
                print("📚 Историю обсуждения читаю ок")
            except RPCError as e:
                print(f"⚠️ Не смог прочитать историю обсуждения: {e}")

        # ✅ ТЕПЕРЬ привязываем обработчики (когда ID уже установлен)
        await bind_discussion_handlers()
        
        return LINKED_DISCUSSION_ID

    async def main():
        try:
            await app.start()
            await resolve_linked_discussion()

            # фоновые задачи
            asyncio.create_task(scheduler_loop())
            asyncio.create_task(comment_watcher_loop())
            if ENABLE_DISCUSSION_POLLER:
                # включай только если апдейты из обсуждения реально не приходят
                asyncio.create_task(discussion_poll_loop())

            await idle()
        except (Unauthorized, AuthKeyUnregistered) as e:
            print(f"❌ Ошибка авторизации: {e}")
        finally:
            try:
                await app.stop()
            except:
                pass

    asyncio.run(main())
