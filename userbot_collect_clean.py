import asyncio
import sqlite3
import random
from contextlib import closing
from pathlib import Path
import time
from dotenv import load_dotenv
import os
import html

from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, FileReferenceExpired, RPCError, Unauthorized, AuthKeyUnregistered
from pyrogram.types import Message
from pyrogram.enums import ParseMode
import pyrogram

# ===== Gemini: новый SDK =====
from google import genai
from google.genai import types

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "userbot_session")
WORKDIR = os.getenv("WORKDIR", ".")
SESSION_STRING = os.getenv("SESSION_STRING")


DEBUG_GEMINI = os.getenv("DEBUG_GEMINI", "1") == "1"   # 1 = включить подробные логи
DEBUG_REPLY  = os.getenv("DEBUG_REPLY",  "1") == "1"

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

# >>> ИСТОЧНИКИ И ЦЕЛЬ <<<
SOURCE_CHATS = [
    -1001423363475, -1001304740791, -1001628148774, -1002092838245, -1001096054832,
    -1001334218632, -1001431200947, -1001268741369, -1001647745905, -1001980097656, -1001544919663
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
    if len(text) > 1024:
        text = text[:1024]
    return text

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
        resp = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=[types.Content(role="user", parts=[types.Part.from_text(prompt)])],
            config=types.GenerateContentConfig(
                max_output_tokens=max_tokens,
                temperature=temperature
            ),
        )
        # основные поля ответа
        txt = (getattr(resp, "text", "") or "").strip()
        cand = resp.candidates[0] if getattr(resp, "candidates", None) else None
        finish = getattr(cand, "finish_reason", None)
        safety = getattr(cand, "safety_ratings", None)
        usage  = getattr(resp, "usage_metadata", None)

        dbg_gemini(f"[GEMINI] ⇠ text: {_short(txt)}")
        dbg_gemini(f"[GEMINI]    finish={finish} tokens={getattr(usage,'total_token_count',None)} safety={safety}")

        return txt or FALLBACK_SNIPPET
    except Exception as e:
        dbg_gemini(f"❌ [GEMINI] exception: {e}")
        return FALLBACK_SNIPPET

async def build_random_code_comment() -> str:
    txt = await asyncio.to_thread(
        _gen_text_sync,
        "Генерируйте мудрые мысли, как будто вы еврейский раввин, дающий совет о деньгах, женщинах, мойшах и жизни под солнцем"
    )
    return html.escape(txt)

async def build_reply_for_comment(user_text: str) -> str:
    """Строим короткий ответ на комментарий, с логами до/после."""
    if not user_text:
        user_text = "."
    prompt = (
        "Ты пишешь короткие остроумные ответы (3–5 предложения) на русскоязычные комментарии. "
        "Твои ответ должен быть анекдотом в стиле про евреев, но не оскорбительным. "
        "Анекдот может быть в формате еврей, русский и немец (или две любых других национальность, но еврей должен быть и он самый мудрый и хитрый) сидят где-то (заходят куда-то)... "
        "Формат: прямой ответ без преамбул и смайлов"
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
        print(f"[DISCUSSION] id={m.id} reply_to={m.reply_to_message_id} "
              f"text={(m.text or m.caption or '')[:80]}")

    async def discussion_autoreply(_, m: Message):
        # не отвечаем на себя
        if m.from_user and m.from_user.is_self:
            return

        txt = (m.text or m.caption or "").strip()
        if not txt:
            return

        # вероятность ответа
        rnd = random.random()
        if rnd > REPLY_PROBABILITY:
            dbg_reply(f"⏭️ [REPLY] skip by probability rnd={rnd:.2f} > p={REPLY_PROBABILITY}")
            return

        dbg_reply(f"💬 [REPLY] generating for msg_id={m.id}: {_short(txt, 200)}")
        reply_text = await build_reply_for_comment(txt)
        dbg_reply(f"💬 [REPLY] ready -> {_short(html.unescape(reply_text), 200)}")

        try:
            sent = await app.send_message(
                chat_id=m.chat.id,
                text=reply_text,
                reply_to_message_id=m.id,
                parse_mode=ParseMode.HTML
            )
            dbg_reply(f"✅ [REPLY] sent reply_id={sent.id} to chat={sent.chat.id}")
        except RPCError as e:
            dbg_reply(f"❌ [REPLY] send failed: {e}")

    app.add_handler(
        # только сообщения из связанной группы, без сервисных, и не свои
        pyrogram.handlers.MessageHandler(discussion_tap, filters.chat(LINKED_DISCUSSION_ID) & ~filters.service)
    )
    app.add_handler(
        pyrogram.handlers.MessageHandler(discussion_autoreply, filters.chat(LINKED_DISCUSSION_ID) & ~filters.service & ~filters.me)
    )
    _HANDLERS_BOUND = True
    print("🔗 Discussion handlers bound")

# ---------- резервный опрос обсуждения ----------
async def discussion_poll_loop():
    """
    Резервный поллер обсуждения:
    - Читает новые сообщения из связанной группы (LINKED_DISCUSSION_ID)
    - Логирует каждое входящее
    - По вероятности REPLY_PROBABILITY генерит ответ через Gemini
    - Поддерживает FloodWait и сохраняет offset в meta(last_disc_msg_id)
    """
    if not LINKED_DISCUSSION_ID:
        print("⚠️ [POLL] LINKED_DISCUSSION_ID не задан — поллер выключен")
        return

    # стартовый оффсет
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

            # от старых к новым
            for m in reversed(batch):
                # базовый лог
                dbg_reply(f"[DISCUSSION] id={m.id} reply_to={m.reply_to_message_id} "
                          f"text={_short((m.text or m.caption or ''), 200)}")

                # не отвечаем на себя
                if m.from_user and m.from_user.is_self:
                    continue

                text = (m.text or m.caption or "").strip()
                if not text:
                    continue

                # вероятность ответа
                rnd = random.random()
                if rnd > REPLY_PROBABILITY:
                    dbg_reply(f"⏭️ [POLL] skip by probability rnd={rnd:.2f} > p={REPLY_PROBABILITY}")
                    last_id = max(last_id, m.id)
                    set_meta("last_disc_msg_id", str(last_id))
                    continue

                # генерация ответа
                dbg_reply(f"💬 [POLL] generating for msg_id={m.id}: {_short(text, 200)}")
                reply_text = await build_reply_for_comment(text)
                dbg_reply(f"💬 [POLL] ready -> {_short(html.unescape(reply_text), 200)}")

                # отправка
                try:
                    sent = await app.send_message(
                        chat_id=m.chat.id,
                        text=reply_text,
                        reply_to_message_id=m.id,
                        parse_mode=ParseMode.HTML
                    )
                    dbg_reply(f"✅ [POLL] sent reply_id={sent.id} to chat={sent.chat.id}")
                except FloodWait as e:
                    dbg_reply(f"⏳ [POLL] FloodWait {e.value}s on send; sleeping")
                    await asyncio.sleep(e.value + 1)
                except RPCError as e:
                    dbg_reply(f"❌ [POLL] send failed: {e}")

                # сдвигаем оффсет
                last_id = max(last_id, m.id)
                set_meta("last_disc_msg_id", str(last_id))

        except FloodWait as e:
            dbg_reply(f"⏳ [POLL] FloodWait {e.value}s on fetch; sleeping")
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            dbg_reply(f"[discussion_poll] error: {e}")

        # частота опроса
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

        LINKED_DISCUSSION_ID = linked_id
        await bind_discussion_handlers()  # повесить хендлеры на конкретную группу
        return LINKED_DISCUSSION_ID

    async def main():
        try:
            await app.start()
            await resolve_linked_discussion()
            asyncio.create_task(scheduler_loop())
            asyncio.create_task(comment_watcher_loop())
            # резерв на случай, если апдейты в обсуждении не приходят
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
