# userbot_collect_clean.py
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

# Gemini (официальный SDK Google)
import google.generativeai as genai  # https://ai.google.dev/gemini-api/docs/migrate

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "userbot_session")
WORKDIR = os.getenv("WORKDIR", ".")
SESSION_STRING = os.getenv("SESSION_STRING")

# >>> ИСТОЧНИКИ И ЦЕЛЬ <<<
SOURCE_CHATS = [
    -1001423363475, -1001304740791, -1001628148774, -1002092838245, -1001096054832, -1001334218632,
    -1001431200947, -1001268741369, -1001647745905, -1001980097656, -1001544919663
]
TARGET_CHAT_ID = -1001676356290
EFFECTIVE_SOURCE_CHATS = [c for c in SOURCE_CHATS if c != TARGET_CHAT_ID]

LINKED_DISCUSSION_ID = -1001636680420  
REPLY_PROBABILITY = float(os.getenv("REPLY_PROBABILITY", "1.0"))  # 0..1 — как часто отвечать

# >>> ЧАСТОТА <<<
ENABLE_LIVE_STREAM = True
POST_EVERY_SECONDS = 80 * 60
PER_CHAT_SCAN_LIMIT = 500

# >>> КОММЕНТАРИИ <<<
ENABLE_AUTO_COMMENTS = True
COMMENT_EVERY_N = 10
CHANNEL_POLL_SECONDS = 10   # как часто сканировать канал на новые посты

# ====== Gemini ======
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
gemini_model = None
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel("gemini-2.5-flash-lite")
        print("✅ Gemini API инициализирован")
    except Exception as e:
        print(f"❌ Gemini init error: {e}")
        gemini_model = None
else:
    print("⚠️ GEMINI_API_KEY не найден (будет fallback)")

# ====== антидубли и мета ======
DATA_DIR = Path("./data"); DATA_DIR.mkdir(exist_ok=True)
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

# ====== клиент userbot ======
app = Client(
    name=SESSION_NAME,          # в Pyrogram v2 это параметр "name"
    api_id=API_ID,
    api_hash=API_HASH,
    workdir=WORKDIR,
    session_string=SESSION_STRING   # ← ключевая строка, отключает интерактивный вход
)

async def send_with_retry(func, *args, **kwargs):
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            print(f"⏰ FloodWait: {e.value}s")
            await asyncio.sleep(e.value + 1)

def match_image(msg: Message) -> bool:
    return bool(
        msg and (
            msg.photo or
            (msg.document and msg.document.mime_type and msg.document.mime_type.startswith("image/"))
        )
    )

async def send_clean(app: Client, msg: Message, target_id: int | str) -> Message | None:
    caption = msg.caption or None
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

async def resolve_linked_discussion():
    global LINKED_DISCUSSION_ID
    try:
        chat = await app.get_chat(TARGET_CHAT_ID)
        linked = getattr(chat, "linked_chat", None)
        if linked:
            LINKED_DISCUSSION_ID = linked.id
            print(f"✅ Linked discussion ID: {LINKED_DISCUSSION_ID}")
            return LINKED_DISCUSSION_ID
        else:
            print("⚠️ У канала нет связанной группы (включи Обсуждения)")
            LINKED_DISCUSSION_ID = None
            return None
    except Exception as e:
        print(f"❌ resolve_linked_discussion error: {e}")
        LINKED_DISCUSSION_ID = None
        return None

# ---------- Gemini: генерация кусочка «кода» как простого текста ----------
FALLBACK_SNIPPET = """<div>
  <button id="prev-button">Previous</button>
  <img id="carousel-image" src="" alt="Carousel Image">
  <button id="next-button">Next</button>
</div>"""

def _gen_code_snippet_sync() -> str:
    if not gemini_model:
        return FALLBACK_SNIPPET
    prompt = (
        "Generate a wise thought like you are a Jewish rabbi"
    )
    try:
        resp = gemini_model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(max_output_tokens=200, temperature=0.8)
        )
        text = (resp.text or "").strip()
        return text or FALLBACK_SNIPPET
    except Exception as e:
        print(f"❌ Gemini API error: {e}")
        return FALLBACK_SNIPPET

async def gen_code_snippet() -> str:
    try:
        text = await asyncio.to_thread(_gen_code_snippet_sync)
        return (text or FALLBACK_SNIPPET)[:1000]
    except Exception as e:
        print(f"❌ gen_code_snippet error: {e}")
        return FALLBACK_SNIPPET

async def build_random_code_comment() -> str:
    raw = await gen_code_snippet()
    return html.escape(raw)  # просто текст, без <pre><code>

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


async def build_reply_for_comment(user_text: str) -> str:
    """
    Короткий ответ-шутка на комментарий пользователя.
    """
    if not user_text:
        user_text = "."

    system_hint = (
        "Ты пишешь очень короткие остроумные ответы (1–2 предложения) на русскоязычные комментарии. "
        "Твои ответ должен быть анекдотом в стиле про евреев, но не оскорбительным. "
        "Анекдот может быть в формате еврей, русский и немец сидят где-то... "
        "Формат: прямой ответ без преамбул и смайлов"
    )
    user_prompt = f"Комментарий:\n{user_text}\n\nОтвет:"

    # умеренно «либеральные» пороги блокировки — позволят безобидные шутки,
    # но отсекут токс и жесть (см. Safety settings в Gemini API).
    safety = [
        genai.types.SafetySetting(
            category=genai.types.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=genai.types.HarmBlockThreshold.BLOCK_NONE
        ),
        genai.types.SafetySetting(
            category=genai.types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold=genai.types.HarmBlockThreshold.BLOCK_NONE
        ),
        genai.types.SafetySetting(
            category=genai.types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold=genai.types.HarmBlockThreshold.BLOCK_NONE
        ),
        genai.types.SafetySetting(
            category=genai.types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=genai.types.HarmBlockThreshold.BLOCK_NONE
        ),
    ]

    try:
        resp = gemini_model.generate_content(
            [system_hint, user_prompt],
            generation_config=genai.types.GenerationConfig(
                max_output_tokens=80,
                temperature=0.9,
                top_p=0.95
            ),
            safety_settings=safety  # см. гайд по safety_settings
        )
        text = (resp.text or "").strip()
        return html.escape(text)[:1000] if text else "Окей 🙂"
    except Exception as e:
        print(f"Gemini error: {e}")
        return "Окей 🙂"

# ---------- LIVE-хендлер: поток из источников ----------
@app.on_message(filters.chat(EFFECTIVE_SOURCE_CHATS) & (filters.photo | filters.document))
async def handler(_, msg: Message):
    if not ENABLE_LIVE_STREAM:
        return
    if not match_image(msg):
        return

    uid = msg.photo.file_unique_id if msg.photo else (
        msg.document.file_unique_id if msg.document else None
    )
    if uid and is_seen(uid):
        print(f"⏭️ Дубликат: {uid}")
        return

    try:
        sent = await send_clean(app, msg, TARGET_CHAT_ID)
        if uid: mark_seen(uid)
        # Комментарий НЕ делаем тут — все посты (в т.ч. чужие) обработает вотчер ниже
        if sent:
            print(f"📤 Отправлено в цель: message_id={sent.id}")
    except FileReferenceExpired:
        fresh = await app.get_messages(msg.chat.id, msg.id)
        sent = await send_clean(app, fresh, TARGET_CHAT_ID)
        if uid: mark_seen(uid)
        print(f"📤 Отправлено (refreshed): message_id={sent.id if sent else 'None'}")


@app.on_message(~filters.service & ~filters.me)
async def on_discussion_message(_, msg: Message):
    # Диагностика всех сообщений
    if msg.chat and msg.chat.id == LINKED_DISCUSSION_ID:
        print(f"[DISCUSSION] got id={msg.id} reply_to={msg.reply_to_message_id} "
              f"thread={getattr(msg,'message_thread_id',None)} "
              f"text={(msg.text or msg.caption or '')[:60]}")
    
    # Проверяем, что это связанная группа
    if not LINKED_DISCUSSION_ID or not msg.chat or msg.chat.id != LINKED_DISCUSSION_ID:
        return

    # Получаем текст комментария
    text = msg.text or msg.caption or ""
    if not text.strip():
        print("⏭️ Пустой комментарий, пропускаем")
        return

    # Проверяем вероятность ответа
    import random
    if random.random() > REPLY_PROBABILITY:
        print(f"⏭️ Пропускаем ответ (вероятность {REPLY_PROBABILITY})")
        return

    print(f"💬 Генерируем ответ на: {text[:50]}...")
    reply_text = await build_reply_for_comment(text)

    # Отвечаем на комментарий
    try:
        await send_with_retry(
            app.send_message,
            chat_id=msg.chat.id,
            text=reply_text,
            reply_to_message_id=msg.id,
            parse_mode=ParseMode.HTML
        )
        print(f"✅ Ответил на комментарий {msg.id}")
    except RPCError as e:
        print(f"❌ Не смог ответить: {e}")

# ---------- Выбор случайного кандидата из истории ----------
async def pick_random_candidate(sources, per_chat_limit=500, prefer_unseen=True):
    async def collect(chat, include_seen: bool):
        out = []
        async for m in app.get_chat_history(chat, limit=per_chat_limit):
            if not match_image(m): continue
            uid = m.photo.file_unique_id if m.photo else (
                m.document.file_unique_id if (m.document and m.document.mime_type and m.document.mime_type.startswith("image/")) else None
            )
            if not uid: continue
            if include_seen or not is_seen(uid):
                out.append((m, uid))
        return out

    candidates = []
    if prefer_unseen:
        for c in sources: candidates += await collect(c, include_seen=False)
        if not candidates:
            for c in sources: candidates += await collect(c, include_seen=True)
    else:
        for c in sources: candidates += await collect(c, include_seen=True)

    return random.choice(candidates) if candidates else (None, None)

@app.on_message(filters.me & filters.command("test_comments", prefixes=[".", "/"]))
async def test_comments_cmd(_, msg: Message):
    """Тестирует работу с комментариями"""
    info = []
    
    # Проверяем целевой канал
    try:
        chat = await app.get_chat(TARGET_CHAT_ID)
        info.append(f"✅ Целевой канал: {chat.title} ({TARGET_CHAT_ID})")
        
        linked = getattr(chat, "linked_chat", None)
        if linked:
            info.append(f"✅ Связанная группа: {linked.title} ({linked.id})")
            
            # Проверяем членство
            try:
                member = await app.get_chat_member(linked.id, "me")
                info.append(f"✅ Статус в группе: {member.status}")
            except Exception as e:
                info.append(f"❌ Ошибка членства: {e}")
                
            # Проверяем последние сообщения в группе
            try:
                count = 0
                async for m in app.get_chat_history(linked.id, limit=5):
                    count += 1
                info.append(f"✅ Последних сообщений в группе: {count}")
            except Exception as e:
                info.append(f"❌ Ошибка чтения группы: {e}")
                
        else:
            info.append("❌ Связанная группа не найдена")
            
    except Exception as e:
        info.append(f"❌ Ошибка получения канала: {e}")
    
    # Состояние переменных
    info.append(f"LINKED_DISCUSSION_ID: {LINKED_DISCUSSION_ID}")
    info.append(f"REPLY_PROBABILITY: {REPLY_PROBABILITY}")
    info.append(f"Gemini: {'✅' if gemini_model else '❌'}")
    
    await msg.reply_text("\n".join(info))


# ---------- Команда .random ----------
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
        sent = await send_clean(app, m, TARGET_CHAT_ID)
        if uid: mark_seen(uid)
        await msg.reply_text("Ок, отправил случайный пост.")
    except FileReferenceExpired:
        fresh = await app.get_messages(m.chat.id, m.id)
        sent = await send_clean(app, fresh, TARGET_CHAT_ID)
        if uid: mark_seen(uid)
        await msg.reply_text("Ок, отправил (refresh).")

# ---------- Диагностика ----------
@app.on_message(filters.me & filters.command("diag_comments", prefixes=[".", "/"]))
async def diag_comments(_, msg: Message):
    chat = await app.get_chat(TARGET_CHAT_ID)
    info = [f"Target: {chat.id} ({chat.type})"]
    linked = getattr(chat, "linked_chat", None)
    if linked:
        info.append(f"Linked discussion: {linked.id} ({linked.type})")
        try:
            me = await app.get_chat_member(linked.id, "me")
            info.append(f"Membership in linked: {me.status}")
        except RPCError as e:
            info.append(f"get_chat_member error: {e}")
    else:
        info.append("No linked discussion group (comments disabled)")

    # текущее состояние счётчиков
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("SELECT value FROM meta WHERE key='channel_posts_count'")
        row = cur.fetchone()
        count = int(row[0]) if row else 0
    info.append(f"Current posts count: {count}")
    info.append(f"Every N: {COMMENT_EVERY_N}; Auto: {'ON' if ENABLE_AUTO_COMMENTS else 'OFF'}")
    await msg.reply_text("\n".join(info))

# ---------- Планировщик постинга по интервалу ----------
async def scheduler_loop():
    await asyncio.sleep(5)
    while True:
        try:
            if not EFFECTIVE_SOURCE_CHATS:
                await asyncio.sleep(POST_EVERY_SECONDS); continue
            m, uid = await pick_random_candidate(EFFECTIVE_SOURCE_CHATS, per_chat_limit=PER_CHAT_SCAN_LIMIT, prefer_unseen=True)
            if m:
                try:
                    sent = await send_clean(app, m, TARGET_CHAT_ID)
                except FileReferenceExpired:
                    fresh = await app.get_messages(m.chat.id, m.id)
                    sent = await send_clean(app, fresh, TARGET_CHAT_ID)
                if uid: mark_seen(uid)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            print(f"[scheduler] error: {e}")
        await asyncio.sleep(POST_EVERY_SECONDS)

# ---------- Вотчер канала: комментит любой новый пост ----------
async def comment_watcher_loop():
    """
    Сканируем канал, находим новые посты, считаем счётчик и комментируем каждый N-й.
    Комментарий — это reply на discussion-message в связанной группе.  :contentReference[oaicite:3]{index=3}
    """
    await asyncio.sleep(5)
    # Инициализация last_scanned: если не задан, возьмём самый свежий пост и начнём «с нуля»
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
                # берём только «основные» посты (без реплаев/сервисок)
                if m.reply_to_message:   # это уже чьи-то комментарии
                    continue
                if not (m.text or m.photo or m.document):
                    continue
                new_msgs.append(m)

            # обрабатываем в хронологическом порядке
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

# ---------- Прочие утилиты ----------
@app.on_message(filters.me & filters.command("id", prefixes=[".", "/"]))
async def get_id_cmd(_, msg: Message):
    if len(msg.command) < 2:
        await msg.reply_text("Использование: .id @username_or_link"); return
    target = msg.command[1]
    try:
        chat = await app.get_chat(target)
        title = getattr(chat, "title", "") or ""
        await msg.reply_text(f"ID: {chat.id}\nTitle: {title}")
    except Exception as e:
        await msg.reply_text(f"Ошибка: {e}")

@app.on_message(filters.me & filters.command("ping", prefixes=[".", "/"]))
async def ping_cmd(_, msg: Message):
    await msg.reply_text("pong")

# ---------- Запуск ----------
if __name__ == "__main__":
    init_db()
    print("🚀 Starting userbot (interval repost + watcher comments + Gemini)…")

    async def main():
        try:
            print("🔄 Запускаем userbot...")
            await app.start()
            print("✅ Userbot запущен")
            
            # Получаем информацию о себе
            try:
                me = await app.get_me()
                print(f"👤 Подключен как: {me.first_name} (@{me.username})")
            except Exception as e:
                print(f"⚠️ Не удалось получить информацию о себе: {e}")
            
            # Проверяем связанную группу
            discussion_id = await resolve_linked_discussion()
            if discussion_id:
                print(f"✅ Найдена связанная группа: {discussion_id}")
                
                # Проверяем членство в группе
                try:
                    member = await app.get_chat_member(discussion_id, "me")
                    print(f"✅ Статус в группе: {member.status}")
                except Exception as e:
                    print(f"❌ Не состою в группе обсуждений: {e}")
                    print("💡 Нужно вступить в группу для ответов на комментарии")
            else:
                print("❌ Связанная группа не найдена")
                print("💡 Включите обсуждения в настройках канала")
            
            # Запускаем фоновые задачи
            asyncio.create_task(scheduler_loop())
            asyncio.create_task(comment_watcher_loop())
            
            print("🎯 Все системы запущены, бот готов к работе!")
            await idle()
            
        except (Unauthorized, AuthKeyUnregistered) as e:
            print(f"❌ Ошибка авторизации: {e}")
            print("💡 Проверьте SESSION_STRING или удалите файл сессии")
        except Exception as e:
            print(f"❌ Критическая ошибка: {e}")
        finally:
            try:
                await app.stop()
            except:
                pass

    asyncio.run(main())
