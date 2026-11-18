import asyncio
import contextlib
import html
import json
import logging
import os
import re
import secrets
import shutil
import signal
import sys
import tempfile
import time

from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from telethon import Button, TelegramClient, events
from telethon.errors import FloodWaitError, MessageNotModifiedError
from telethon.sessions import MemorySession
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeVideo,
)


# =========================================================
# Configuration
# =========================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("media_bot")


def env_int(name, default, minimum=1, maximum=None):
    value = int(os.getenv(name, str(default)))

    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")

    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be <= {maximum}")

    return value


API_ID = env_int("API_ID", 0, minimum=0)
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

WORKERS = env_int("MAX_CONCURRENT_DOWNLOADS", 1, maximum=4)
MAX_QUEUE = env_int("MAX_QUEUE_SIZE", 20, maximum=200)

MAX_FILE_MB = env_int("MAX_FILE_SIZE_MB", 1900, maximum=1900)
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024

MAX_DURATION_MINUTES = env_int(
    "MAX_DURATION_MINUTES", 180, minimum=0
)

DOWNLOAD_TIMEOUT = env_int("DOWNLOAD_TIMEOUT_SECONDS", 1800)
INFO_TIMEOUT = env_int("INFO_TIMEOUT_SECONDS", 120)

CACHE_TTL = env_int("CACHE_TTL_SECONDS", 3600)
CACHE_LIMIT = env_int("CACHE_LIMIT", 300)

DOWNLOAD_ROOT = Path(
    os.getenv("DOWNLOAD_DIR", "/tmp/media-bot")
).resolve()

# Optional, trusted Netscape cookies file mounted on the server.
COOKIE_FILE = os.getenv("YTDLP_COOKIE_FILE", "").strip()

# Restrict supported platforms instead of accepting arbitrary URLs.
ALLOWED_DOMAINS = (
    "youtube.com",
    "youtu.be",
    "youtube-nocookie.com",
    "tiktok.com",
    "facebook.com",
    "fb.watch",
    "fb.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "vimeo.com",
    "dailymotion.com",
    "dai.ly",
    "reddit.com",
    "redd.it",
    "pinterest.com",
    "pin.it",
    "soundcloud.com",
    "twitch.tv",
    "snapchat.com",
)

ALLOWED_USERS = {
    int(value.strip())
    for value in os.getenv("ALLOWED_USER_IDS", "").split(",")
    if value.strip()
}

URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)

VIDEO_CHOICES = {"360", "480", "720", "1080", "1440", "2160", "best"}
AUDIO_CHOICES = {"128", "192", "320"}


@dataclass
class Media:
    token: str
    owner_id: int
    chat_id: int
    url: str
    title: str
    uploader: str
    duration: int
    source: str
    views: object
    heights: tuple
    has_audio: bool
    created: float


@dataclass
class Job:
    media: Media
    mode: str
    quality: str
    message_id: int


class ProcessError(Exception):
    pass


# =========================================================
# Helpers
# =========================================================

def human_size(value):
    value = float(value or 0)

    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}"
        value /= 1024


def duration_text(seconds):
    if not seconds:
        return "غير معروفة"

    hours, rest = divmod(int(seconds), 3600)
    minutes, seconds = divmod(rest, 60)

    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"

    return f"{minutes}:{seconds:02d}"


def plain(value, limit=180):
    return " ".join(str(value or "").split())[:limit]


def extract_url(text):
    match = URL_RE.search(text or "")

    if not match:
        raise ValueError("أرسل رابط فيديو يبدأ بـ https://")

    url = match.group(0).rstrip(".,،؛!؟)]}")

    if len(url) > 4096:
        raise ValueError("الرابط طويل جدًا.")

    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower().rstrip(".")

    if parsed.username or parsed.password:
        raise ValueError("روابط تسجيل الدخول غير مقبولة.")

    if parsed.scheme not in {"http", "https"}:
        raise ValueError("نوع الرابط غير مقبول.")

    if parsed.port not in (None, 80, 443):
        raise ValueError("منفذ الرابط غير مسموح.")

    if not any(
        host == domain or host.endswith("." + domain)
        for domain in ALLOWED_DOMAINS
    ):
        raise ValueError(
            "المنصة غير مضافة لقائمة المواقع المسموح بها في البوت."
        )

    return url


def friendly_error(error):
    text = str(error)
    lower = text.lower()

    if "sign in to confirm" in lower and "bot" in lower:
        return (
            "يوتيوب يطلب التحقق من الطلب القادم من السيرفر.\n"
            "هذا لا يعني أن الفيديو خاص.\n"
            "تحديث المكتبات لا يضمن حل قيود عنوان IP."
        )

    if any(value in lower for value in (
        "private video",
        "login required",
        "login_required",
        "sign in",
        "cookies",
    )):
        return (
            "الموقع يطلب تسجيل دخول أو جلسة مصادقة صالحة "
            "لهذا المحتوى."
        )

    if "unsupported url" in lower:
        return "هذا النوع من الروابط غير مدعوم حاليًا."

    if "requested format" in lower:
        return "الجودة المطلوبة غير متاحة. جرّب أفضل جودة أو جودة أخرى."

    if "403" in lower or "forbidden" in lower:
        return (
            "الموقع رفض طلب التحميل. قد تكون هناك قيود على "
            "اتصال السيرفر أو متطلبات مصادقة إضافية."
        )

    if "429" in lower or "too many requests" in lower:
        return "الموقع قيّد الطلبات مؤقتًا. حاول لاحقًا."

    if isinstance(error, (ValueError, TimeoutError)):
        return text

    return (
        "تعذرت معالجة الرابط أو إرسال الملف. "
        "التفاصيل موجودة في Railway Logs."
    )


async def stop_process(process):
    if process.returncode is not None:
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)

        await process.wait()


async def run_process(
    args,
    timeout,
    line_callback=None,
    monitor=None,
):
    process = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=True,
        limit=32 * 1024 * 1024,
    )

    tail = []

    async def consume():
        while True:
            raw = await process.stdout.readline()

            if not raw:
                break

            line = raw.decode("utf-8", errors="replace").strip()

            tail.append(line)
            if len(tail) > 60:
                del tail[0]

            if line_callback:
                await line_callback(line)

        await process.wait()

    async def guard():
        while True:
            await asyncio.sleep(2)

            if monitor:
                monitor()

    reader = asyncio.create_task(consume())
    watcher = asyncio.create_task(guard())

    try:
        done, _ = await asyncio.wait(
            {reader, watcher},
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )

        if not done:
            raise TimeoutError(
                "انتهت مهلة المعالجة. جرّب جودة أقل أو فيديو أقصر."
            )

        if watcher in done:
            await watcher

        await reader

        if process.returncode:
            raise ProcessError("\n".join(tail)[-6000:])

        return tail

    finally:
        await stop_process(process)

        for task in (reader, watcher):
            if not task.done():
                task.cancel()

        await asyncio.gather(
            reader, watcher, return_exceptions=True
        )


def ytdlp_args():
    args = [
        sys.executable,
        "-m",
        "yt_dlp",
        "--ignore-config",
        "--no-playlist",
        "--no-warnings",
        "--socket-timeout",
        "30",
        "--retries",
        "3",
        "--fragment-retries",
        "3",
        "--js-runtimes",
        "deno",
    ]

    if COOKIE_FILE:
        args += ["--cookies", COOKIE_FILE]

    return args


class Progress:
    def __init__(self, message):
        self.message = message
        self.last_update = 0.0
        self.last_text = None
        self.disabled = False
        self.next_allowed = 0.0

    async def update(self, text, force=False):
        if self.disabled:
            return

        now = time.monotonic()

        if now < self.next_allowed:
            return

        if text == self.last_text:
            return

        if not force and now - self.last_update < 5:
            return

        self.last_update = now
        self.last_text = text

        try:
            await self.message.edit(text, parse_mode=None)
        except MessageNotModifiedError:
            pass
        except FloodWaitError as error:
            self.next_allowed = now + error.seconds + 1
        except Exception:
            self.disabled = True


# =========================================================
# Bot
# =========================================================

class MediaBot:
    def __init__(self):
        if not API_ID or not API_HASH or not BOT_TOKEN:
            raise RuntimeError(
                "Set API_ID, API_HASH and BOT_TOKEN in Railway Variables."
            )

        if not shutil.which("ffmpeg"):
            raise RuntimeError("FFmpeg is not installed.")

        if not shutil.which("deno"):
            raise RuntimeError("Deno is not installed.")

        if COOKIE_FILE and not Path(COOKIE_FILE).is_file():
            raise RuntimeError("YTDLP_COOKIE_FILE does not exist.")

        DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)

        self.client = TelegramClient(
            MemorySession(),
            API_ID,
            API_HASH,
        )

        self.queue = asyncio.Queue(maxsize=MAX_QUEUE)
        self.cache = OrderedDict()

        self.busy_users = set()
        self.inspecting_users = set()
        self.info_slots = asyncio.Semaphore(2)

    def authorized(self, user_id):
        return not ALLOWED_USERS or user_id in ALLOWED_USERS

    def trim_cache(self):
        now = time.monotonic()

        for key, media in list(self.cache.items()):
            if now - media.created > CACHE_TTL:
                self.cache.pop(key, None)

        while len(self.cache) > CACHE_LIMIT:
            self.cache.popitem(last=False)

    async def inspect(self, url):
        result = {}

        async def receive(line):
            if line.startswith("{"):
                try:
                    result.update(json.loads(line))
                except json.JSONDecodeError:
                    pass

        await run_process(
            ytdlp_args() + [
                "--quiet",
                "--skip-download",
                "--dump-single-json",
                "--",
                url,
            ],
            timeout=INFO_TIMEOUT,
            line_callback=receive,
        )

        if not result:
            raise ValueError("لم يتمكن البوت من قراءة معلومات الرابط.")

        if result.get("_type") in {"playlist", "multi_video"}:
            raise ValueError(
                "أرسل رابط فيديو واحد، وليس قائمة تشغيل أو مجموعة فيديوهات."
            )

        if result.get("is_live"):
            raise ValueError("البث المباشر غير مدعوم.")

        duration = int(result.get("duration") or 0)

        if (
            MAX_DURATION_MINUTES
            and duration > MAX_DURATION_MINUTES * 60
        ):
            raise ValueError(
                f"مدة الفيديو تتجاوز الحد: "
                f"{MAX_DURATION_MINUTES} دقيقة."
            )

        return result

    def make_media(self, info, url, event):
        formats = info.get("formats") or []

        heights = sorted({
            int(item["height"])
            for item in formats
            if item.get("height")
            and item.get("vcodec") not in (None, "none")
            and not item.get("has_drm")
        })

        has_audio = any(
            item.get("acodec") not in (None, "none")
            for item in formats
        )

        return Media(
            token=secrets.token_hex(8),
            owner_id=event.sender_id,
            chat_id=event.chat_id,
            url=url,
            title=plain(info.get("title") or "Media", 200),
            uploader=plain(
                info.get("uploader")
                or info.get("channel")
                or "غير معروف",
                100,
            ),
            duration=int(info.get("duration") or 0),
            source=plain(
                info.get("extractor_key") or "غير معروف", 50
            ),
            views=info.get("view_count"),
            heights=tuple(heights),
            has_audio=has_audio,
            created=time.monotonic(),
        )

    def keyboard(self, media):
        rows = []

        if media.heights:
            buttons = []

            for height in media.heights:
                if str(height) not in VIDEO_CHOICES:
                    continue

                buttons.append(Button.inline(
                    f"فيديو {height}p",
                    f"get|{media.token}|v|{height}".encode(),
                ))

            for index in range(0, len(buttons), 3):
                rows.append(buttons[index:index + 3])

            rows.append([
                Button.inline(
                    "فيديو: أفضل جودة",
                    f"get|{media.token}|v|best".encode(),
                )
            ])

        elif not media.has_audio:
            rows.append([
                Button.inline(
                    "تحميل الفيديو",
                    f"get|{media.token}|v|best".encode(),
                )
            ])

        if media.has_audio:
            rows.append([
                Button.inline(
                    f"MP3 {quality} kbps",
                    f"get|{media.token}|a|{quality}".encode(),
                )
                for quality in ("128", "192", "320")
            ])

        return rows

    async def on_message(self, event):
        if not event.is_private or not event.sender_id:
            return

        if not self.authorized(event.sender_id):
            await event.reply(
                "هذا البوت غير متاح لهذا الحساب.",
                parse_mode=None,
            )
            return

        text = event.raw_text or ""

        if not URL_RE.search(text):
            await event.reply(
                "أرسل رابط الفيديو مباشرةً، "
                "وسأعرض معلوماته وأزرار التحميل.",
                parse_mode=None,
            )
            return

        if event.sender_id in self.inspecting_users:
            await event.reply(
                "انتظر انتهاء فحص الرابط السابق.",
                parse_mode=None,
            )
            return

        self.inspecting_users.add(event.sender_id)
        status = None

        try:
            url = extract_url(text)

            status = await event.reply(
                "جاري قراءة معلومات الفيديو...",
                parse_mode=None,
            )

            async with self.info_slots:
                info = await self.inspect(url)

            media = self.make_media(info, url, event)

            self.trim_cache()
            self.cache[media.token] = media
            self.trim_cache()

            views = (
                f"{int(media.views):,}"
                if media.views is not None
                else "غير متاح"
            )

            details = (
                f"<b>{html.escape(media.title)}</b>\n\n"
                f"الناشر: {html.escape(media.uploader)}\n"
                f"المدة: {duration_text(media.duration)}\n"
                f"المشاهدات: {views}\n"
                f"المنصة: {html.escape(media.source)}\n\n"
                "اختر الفيديو أو الصوت من الأزرار.\n"
                "قيم MP3 هي معدل ترميز الملف الناتج؛ "
                "رفعها لا يحسّن جودة المصدر."
            )

            await status.edit(
                details,
                parse_mode="html",
                buttons=self.keyboard(media),
                link_preview=False,
            )

        except Exception as error:
            logger.exception("Metadata extraction failed")

            if status:
                with contextlib.suppress(Exception):
                    await status.edit(
                        friendly_error(error),
                        parse_mode=None,
                    )

        finally:
            self.inspecting_users.discard(event.sender_id)

    async def on_button(self, event):
        try:
            _, token, mode, quality = event.data.decode().split("|")
        except (ValueError, UnicodeDecodeError):
            await event.answer("زر غير صالح.", alert=True)
            return

        self.trim_cache()
        media = self.cache.get(token)

        if not media:
            await event.answer(
                "انتهت صلاحية الأزرار. أرسل الرابط مرة أخرى.",
                alert=True,
            )
            return

        if (
            event.sender_id != media.owner_id
            or event.chat_id != media.chat_id
            or not self.authorized(event.sender_id)
        ):
            await event.answer(
                "هذه الأزرار تخص صاحب الطلب.",
                alert=True,
            )
            return

        if (
            mode not in {"v", "a"}
            or (mode == "v" and quality not in VIDEO_CHOICES)
            or (mode == "a" and quality not in AUDIO_CHOICES)
        ):
            await event.answer("اختيار غير صالح.", alert=True)
            return

        if media.owner_id in self.busy_users:
            await event.answer(
                "لديك طلب قيد المعالجة. انتظر انتهاءه.",
                alert=True,
            )
            return

        if self.queue.full():
            await event.answer(
                "الطابور ممتلئ. حاول بعد قليل.",
                alert=True,
            )
            return

        self.busy_users.add(media.owner_id)

        self.queue.put_nowait(Job(
            media=media,
            mode=mode,
            quality=quality,
            message_id=event.message_id,
        ))

        await event.answer("تمت إضافة الطلب إلى طابور التحميل.")

    async def download(self, job, folder, progress):
        output = folder / "%(id).80s.%(ext)s"

        args = ytdlp_args() + [
            "--newline",
            "--no-color",
            "--progress",
            "--progress-template",
            "download:PROGRESS %(progress._percent_str)s "
            "%(progress._speed_str)s ETA %(progress._eta_str)s",
            "--concurrent-fragments", "3",
            "--max-filesize", str(MAX_FILE_BYTES),
            "--output", str(output),
            "--print", "after_move:FINAL:%(filepath)s",
        ]

        if job.mode == "a":
            args += [
                "--format", "bestaudio/best",
                "--extract-audio",
                "--audio-format", "mp3",
                "--audio-quality", f"{job.quality}K",
            ]

        else:
            cap = (
                ""
                if job.quality == "best"
                else f"[height<={job.quality}]"
            )

            args += [
                "--format",
                f"bestvideo{cap}+bestaudio/best{cap}",
                "--merge-output-format", "mkv",
            ]

        args += ["--", job.media.url]

        final_paths = []

        async def receive(line):
            if line.startswith("PROGRESS "):
                await progress.update(
                    "جاري التنزيل:\n"
                    + line.removeprefix("PROGRESS ")
                )

            elif line.startswith("FINAL:"):
                final_paths.append(
                    Path(line.removeprefix("FINAL:")).resolve()
                )

        def check_resources():
            size = sum(
                path.stat().st_size
                for path in folder.rglob("*")
                if path.is_file()
            )

            # Allow room for separate streams and merging.
            if size > MAX_FILE_BYTES * 3:
                raise ValueError(
                    "تجاوزت الملفات المؤقتة الحد المسموح."
                )

            if shutil.disk_usage(folder).free < 256 * 1024 * 1024:
                raise ValueError(
                    "مساحة السيرفر المتاحة غير كافية."
                )

        await run_process(
            args,
            timeout=DOWNLOAD_TIMEOUT,
            line_callback=receive,
            monitor=check_resources,
        )

        if not final_paths:
            raise ValueError(
                "لم ينتج ملف نهائي. قد يكون الحجم أكبر من الحد."
            )

        path = final_paths[-1]

        if folder.resolve() not in path.parents or not path.is_file():
            raise ValueError("مسار الملف الناتج غير صالح.")

        if path.stat().st_size == 0:
            raise ValueError("الملف الناتج فارغ.")

        if path.stat().st_size > MAX_FILE_BYTES:
            raise ValueError(
                f"الملف أكبر من الحد المسموح: {MAX_FILE_MB} MB. "
                "اختر جودة أقل."
            )

        return path

    async def probe(self, path):
        result = await run_process(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries",
                "format=duration:"
                "stream=codec_type,width,height,duration",
                "-of", "json",
                str(path),
            ],
            timeout=30,
        )

        try:
            return json.loads("\n".join(result))
        except json.JSONDecodeError:
            return {}

    async def process_job(self, job):
        folder = Path(tempfile.mkdtemp(
            prefix="job-",
            dir=DOWNLOAD_ROOT,
        ))

        status = None

        try:
            if shutil.disk_usage(folder).free < 512 * 1024 * 1024:
                raise ValueError(
                    "مساحة السيرفر منخفضة. حاول لاحقًا."
                )

            status = await self.client.send_message(
                job.media.chat_id,
                "جاري تجهيز التحميل...",
                reply_to=job.message_id,
                parse_mode=None,
            )

            progress = Progress(status)
            path = await self.download(job, folder, progress)

            await progress.update(
                "انتهى التنزيل. جاري تجهيز رفع الملف...",
                force=True,
            )

            metadata = await self.probe(path)

            duration = int(float(
                metadata.get("format", {}).get("duration")
                or job.media.duration
                or 0
            ))

            attributes = []

            if job.mode == "a":
                attributes.append(DocumentAttributeAudio(
                    duration=duration,
                    title=job.media.title[:64],
                    performer=job.media.uploader[:64],
                ))

            else:
                video = next(
                    (
                        stream
                        for stream in metadata.get("streams", [])
                        if stream.get("codec_type") == "video"
                    ),
                    {},
                )

                if video:
                    attributes.append(DocumentAttributeVideo(
                        duration=duration,
                        w=int(video.get("width") or 0),
                        h=int(video.get("height") or 0),
                        supports_streaming=path.suffix.lower() == ".mp4",
                    ))

            caption = (
                f"{job.media.title}\n"
                f"الناشر: {job.media.uploader}\n"
                f"المدة: {duration_text(duration)}\n"
                f"الحجم: {human_size(path.stat().st_size)}"
            )

            async def upload_progress(current, total):
                percent = current * 100 / total if total else 0

                await progress.update(
                    f"جاري الرفع إلى تيليجرام: {percent:.1f}%\n"
                    f"{human_size(current)} / {human_size(total)}"
                )

            # MKV is sent as a document to preserve the downloaded
            # codecs without an expensive server-side re-encode.
            await self.client.send_file(
                job.media.chat_id,
                str(path),
                caption=caption,
                parse_mode=None,
                reply_to=job.message_id,
                attributes=attributes or None,
                supports_streaming=(
                    job.mode == "v"
                    and path.suffix.lower() == ".mp4"
                ),
                force_document=(
                    job.mode == "v"
                    and path.suffix.lower() != ".mp4"
                ),
                progress_callback=upload_progress,
            )

            await progress.update(
                "تم إرسال الملف بنجاح.",
                force=True,
            )

        except Exception as error:
            logger.exception("Job failed")

            message = friendly_error(error)

            with contextlib.suppress(Exception):
                if status:
                    await status.edit(message, parse_mode=None)
                else:
                    await self.client.send_message(
                        job.media.chat_id,
                        message,
                        parse_mode=None,
                    )

        finally:
            shutil.rmtree(folder, ignore_errors=True)
            self.busy_users.discard(job.media.owner_id)

    async def worker(self, number):
        logger.info("Worker %s started", number)

        while True:
            job = await self.queue.get()

            try:
                await self.process_job(job)
            finally:
                self.queue.task_done()

    async def run(self):
        await self.client.start(bot_token=BOT_TOKEN)

        self.client.add_event_handler(
            self.on_message,
            events.NewMessage(incoming=True),
        )

        self.client.add_event_handler(
            self.on_button,
            events.CallbackQuery(pattern=rb"^get\|"),
        )

        workers = [
            asyncio.create_task(self.worker(number))
            for number in range(1, WORKERS + 1)
        ]

        me = await self.client.get_me()
        logger.info("Bot started: @%s", me.username)

        try:
            await self.client.run_until_disconnected()
        finally:
            for task in workers:
                task.cancel()

            await asyncio.gather(
                *workers,
                return_exceptions=True,
            )

            await self.client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(MediaBot().run())
    except KeyboardInterrupt:
        pass
