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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from telethon import Button, TelegramClient, events
from telethon.errors import FloodWaitError, MessageNotModifiedError
from telethon.sessions import MemorySession
from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeVideo

# The original single-video workflow is kept in MediaBot.
# Collection handling is added separately in CollectionMediaBot.
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("media_bot")


def env_int(name, default, minimum=1, maximum=None):
    value = int(os.getenv(name, str(default)))
    if value < minimum or (maximum is not None and value > maximum):
        raise ValueError(f"Invalid value for {name}: {value}")
    return value


API_ID = env_int("API_ID", 0, minimum=0)
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
WORKERS = env_int("MAX_CONCURRENT_DOWNLOADS", 1, maximum=4)
MAX_QUEUE = env_int("MAX_QUEUE_SIZE", 20, maximum=200)
MAX_FILE_MB = env_int("MAX_FILE_SIZE_MB", 1900, maximum=1900)
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024
MAX_DURATION_MINUTES = env_int("MAX_DURATION_MINUTES", 180, minimum=0)
DOWNLOAD_TIMEOUT = env_int("DOWNLOAD_TIMEOUT_SECONDS", 1800)
INFO_TIMEOUT = env_int("INFO_TIMEOUT_SECONDS", 120)
CACHE_TTL = env_int("CACHE_TTL_SECONDS", 3600)
CACHE_LIMIT = env_int("CACHE_LIMIT", 300)
DOWNLOAD_ROOT = Path(os.getenv("DOWNLOAD_DIR", "/tmp/media-bot")).resolve()
COOKIE_FILE = os.getenv("YTDLP_COOKIE_FILE", "").strip()
ALLOWED_DOMAINS = (
    "youtube.com", "youtu.be", "youtube-nocookie.com", "tiktok.com",
    "facebook.com", "fb.watch", "fb.com", "instagram.com", "twitter.com", "x.com",
    "vimeo.com", "dailymotion.com", "dai.ly", "reddit.com", "redd.it", "pinterest.com",
    "pin.it", "soundcloud.com", "twitch.tv", "snapchat.com",
)
ALLOWED_USERS = {int(x.strip()) for x in os.getenv("ALLOWED_USER_IDS", "").split(",") if x.strip()}
URL_RE = re.compile(r"https?://[^\s<>\"']+", re.I)
VIDEO_CHOICES = {"360", "480", "720", "1080", "1440", "2160", "best"}
AUDIO_CHOICES = {"128", "192", "320"}

# New settings; existing Railway variables above remain supported.
MAX_COLLECTION_ITEMS = env_int("MAX_COLLECTION_ITEMS", 2000, maximum=10000)
COLLECTION_INFO_TIMEOUT = env_int("COLLECTION_INFO_TIMEOUT_SECONDS", 900)
MAX_ATTEMPTS = env_int("MAX_ATTEMPTS", 3, maximum=5)
RETRY_DELAY = env_int("RETRY_DELAY_SECONDS", 5)
BETWEEN_VIDEOS = env_int("BETWEEN_VIDEOS_SECONDS", 3, minimum=0)
FAILED_CACHE_TTL = env_int("FAILED_CACHE_TTL_SECONDS", 1800)


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


@dataclass
class Entry:
    url: str
    title: str
    timestamp: object = None


@dataclass
class Collection:
    token: str
    owner_id: int
    chat_id: int
    message_id: int
    title: str = "مجموعة فيديوهات"
    uploader: str = "غير متاح"
    entries: list = field(default_factory=list)
    selected: list = field(default_factory=list)
    failed: list = field(default_factory=list)
    stage: str = "scan"
    count: int = 0
    mode: str = "v"
    quality: str = "best"
    order: str = "ترتيب المصدر"
    tiktok: bool = False
    truncated: bool = False
    chronological: bool = False
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    touched: float = field(default_factory=time.monotonic)


@dataclass
class CollectionJob:
    collection: Collection


class ProcessError(Exception):
    pass


class TaskCancelled(Exception):
    pass


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
    return f"{hours}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes}:{seconds:02d}"


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
    if not any(host == d or host.endswith("." + d) for d in ALLOWED_DOMAINS):
        raise ValueError("المنصة غير مضافة لقائمة المواقع المسموح بها في البوت.")
    return url


def friendly_error(error):
    lower = str(error).lower()
    if isinstance(error, TaskCancelled):
        return "تم إلغاء المهمة."
    if "sign in to confirm" in lower and "bot" in lower:
        return ("يوتيوب يطلب التحقق من الطلب القادم من السيرفر.\n"
                "هذا لا يعني أن الفيديو خاص.\nتحديث المكتبات لا يضمن حل قيود عنوان IP.")
    if any(x in lower for x in ("private video", "login required", "login_required", "sign in", "cookies")):
        return "الموقع يطلب تسجيل دخول أو جلسة مصادقة صالحة لهذا المحتوى."
    if "unsupported url" in lower:
        return "هذا النوع من الروابط غير مدعوم حاليًا."
    if "requested format" in lower:
        return "الجودة المطلوبة غير متاحة. جرّب أفضل جودة أو جودة أخرى."
    if "403" in lower or "forbidden" in lower:
        return "الموقع رفض طلب التحميل؛ قد تكون هناك قيود على اتصال السيرفر أو متطلبات مصادقة."
    if "429" in lower or "too many requests" in lower:
        return "الموقع قيّد الطلبات مؤقتًا. حاول لاحقًا."
    if isinstance(error, (ValueError, TimeoutError)):
        return str(error)
    return "تعذرت معالجة الرابط أو إرسال الملف. التفاصيل في Railway Logs."


def permanent_error(error):
    return isinstance(error, ValueError) or any(x in str(error).lower() for x in (
        "private video", "video unavailable", "has been removed", "unsupported url",
        "requested format is not available", "sign in to confirm", "http error 429"))


async def stop_process(process):
    if process.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        await process.wait()


async def run_process(args, timeout, line_callback=None, monitor=None, cancel_event=None):
    process = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        start_new_session=True, limit=32 * 1024 * 1024)
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
            if cancel_event and cancel_event.is_set():
                raise TaskCancelled()
            if monitor:
                monitor()
            await asyncio.sleep(1)

    reader = asyncio.create_task(consume())
    watcher = asyncio.create_task(guard())
    try:
        done, _ = await asyncio.wait({reader, watcher}, timeout=timeout,
                                     return_when=asyncio.FIRST_COMPLETED)
        if not done:
            raise TimeoutError("انتهت مهلة المعالجة. جرّب جودة أقل أو عددًا أقل.")
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
        await asyncio.gather(reader, watcher, return_exceptions=True)


def ytdlp_args():
    args = [sys.executable, "-m", "yt_dlp", "--ignore-config", "--no-playlist", "--no-warnings",
            "--socket-timeout", "30", "--retries", "3", "--fragment-retries", "3",
            "--js-runtimes", "deno"]
    if COOKIE_FILE:
        args += ["--cookies", COOKIE_FILE]
    return args


class Progress:
    def __init__(self, message, buttons=None):
        self.message = message
        self.buttons = buttons
        self.last_update = 0.0
        self.last_text = None
        self.disabled = False
        self.next_allowed = 0.0

    async def update(self, text, force=False):
        if self.disabled:
            return
        now = time.monotonic()
        if now < self.next_allowed or text == self.last_text:
            return
        if not force and now - self.last_update < 5:
            return
        self.last_update = now
        try:
            await self.message.edit(text, parse_mode=None, **({"buttons": self.buttons} if self.buttons is not None else {}))
            self.last_text = text
        except MessageNotModifiedError:
            self.last_text = text
        except FloodWaitError as error:
            self.next_allowed = now + error.seconds + 1
        except Exception:
            self.disabled = True


class PrefixProgress:
    def __init__(self, progress, prefix):
        self.progress = progress
        self.prefix = prefix

    async def update(self, text, force=False):
        await self.progress.update(self.prefix + "\n" + text, force=force)


# =========================================================
# Existing single-video functionality
# =========================================================

class MediaBot:
    def __init__(self):
        if not API_ID or not API_HASH or not BOT_TOKEN:
            raise RuntimeError("Set API_ID, API_HASH and BOT_TOKEN in Railway Variables.")
        for tool in ("ffmpeg", "ffprobe", "deno"):
            if not shutil.which(tool):
                raise RuntimeError(f"{tool} is not installed.")
        if COOKIE_FILE and not Path(COOKIE_FILE).is_file():
            raise RuntimeError("YTDLP_COOKIE_FILE does not exist.")
        DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
        self.client = TelegramClient(MemorySession(), API_ID, API_HASH)
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

    async def inspect(self, url, cancel_event=None):
        result = {}
        async def receive(line):
            if line.startswith("{"):
                with contextlib.suppress(json.JSONDecodeError):
                    result.update(json.loads(line))
        await run_process(ytdlp_args() + ["--quiet", "--skip-download", "--dump-single-json", "--", url],
                          INFO_TIMEOUT, receive, cancel_event=cancel_event)
        if not result:
            raise ValueError("لم يتمكن البوت من قراءة معلومات الرابط.")
        if result.get("_type") in {"playlist", "multi_video"}:
            raise ValueError("هذا الرابط يشير إلى مجموعة وليس فيديو واحدًا.")
        if result.get("is_live"):
            raise ValueError("البث المباشر غير مدعوم.")
        length = int(result.get("duration") or 0)
        if MAX_DURATION_MINUTES and length > MAX_DURATION_MINUTES * 60:
            raise ValueError(f"مدة الفيديو تتجاوز الحد: {MAX_DURATION_MINUTES} دقيقة.")
        return result

    def make_media(self, info, url, event):
        formats = info.get("formats") or []
        heights = sorted({int(f["height"]) for f in formats if f.get("height")
                          and f.get("vcodec") not in (None, "none") and not f.get("has_drm")})
        has_audio = any(f.get("acodec") not in (None, "none") for f in formats)
        return Media(secrets.token_hex(8), event.sender_id, event.chat_id, url,
                     plain(info.get("title") or "Media", 200),
                     plain(info.get("uploader") or info.get("channel") or "غير معروف", 100),
                     int(info.get("duration") or 0), plain(info.get("extractor_key") or "غير معروف", 50),
                     info.get("view_count"), tuple(heights), has_audio, time.monotonic())

    def keyboard(self, media):
        rows = []
        if media.heights:
            buttons = [Button.inline(f"فيديو {h}p", f"get|{media.token}|v|{h}".encode())
                       for h in media.heights if str(h) in VIDEO_CHOICES]
            rows.extend(buttons[i:i + 3] for i in range(0, len(buttons), 3))
            rows.append([Button.inline("فيديو: أفضل جودة", f"get|{media.token}|v|best".encode())])
        elif not media.has_audio:
            rows.append([Button.inline("تحميل الفيديو", f"get|{media.token}|v|best".encode())])
        if media.has_audio:
            rows.append([Button.inline(f"MP3 {q} kbps", f"get|{media.token}|a|{q}".encode())
                         for q in ("128", "192", "320")])
        return rows

    async def on_message(self, event):
        if not event.is_private or not event.sender_id:
            return
        if not self.authorized(event.sender_id):
            await event.reply("هذا البوت غير متاح لهذا الحساب.", parse_mode=None)
            return
        raw = event.raw_text or ""
        if not URL_RE.search(raw):
            await event.reply("أرسل رابط فيديو أو بلاي ليست يوتيوب أو حساب تيك توك مباشرةً.", parse_mode=None)
            return
        if event.sender_id in self.inspecting_users:
            await event.reply("انتظر انتهاء فحص الرابط السابق.", parse_mode=None)
            return
        self.inspecting_users.add(event.sender_id)
        status = None
        try:
            url = extract_url(raw)
            status = await event.reply("جاري قراءة معلومات الفيديو...", parse_mode=None)
            async with self.info_slots:
                info = await self.inspect(url)
            media = self.make_media(info, url, event)
            self.trim_cache()
            self.cache[media.token] = media
            self.trim_cache()
            views = f"{int(media.views):,}" if media.views is not None else "غير متاح"
            details = (f"<b>{html.escape(media.title)}</b>\n\n"
                       f"الناشر: {html.escape(media.uploader)}\nالمدة: {duration_text(media.duration)}\n"
                       f"المشاهدات: {views}\nالمنصة: {html.escape(media.source)}\n\n"
                       "اختر الفيديو أو الصوت من الأزرار.\nقيم MP3 تحدد ترميز الناتج ولا تحسّن المصدر.")
            await status.edit(details, parse_mode="html", buttons=self.keyboard(media), link_preview=False)
        except Exception as error:
            logger.exception("Metadata extraction failed")
            if status:
                with contextlib.suppress(Exception):
                    await status.edit(friendly_error(error), parse_mode=None)
            else:
                await event.reply(friendly_error(error), parse_mode=None)
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
            await event.answer("انتهت صلاحية الأزرار. أرسل الرابط مجددًا.", alert=True)
            return
        if event.sender_id != media.owner_id or event.chat_id != media.chat_id or not self.authorized(event.sender_id):
            await event.answer("هذه الأزرار تخص صاحب الطلب.", alert=True)
            return
        if (mode not in {"v", "a"} or (mode == "v" and quality not in VIDEO_CHOICES)
                or (mode == "a" and quality not in AUDIO_CHOICES)):
            await event.answer("اختيار غير صالح.", alert=True)
            return
        if media.owner_id in self.busy_users:
            await event.answer("لديك طلب قيد المعالجة. انتظر انتهاءه.", alert=True)
            return
        if self.queue.full():
            await event.answer("الطابور ممتلئ. حاول لاحقًا.", alert=True)
            return
        self.busy_users.add(media.owner_id)
        self.queue.put_nowait(Job(media, mode, quality, event.message_id))
        await event.answer("تمت إضافة الطلب إلى طابور التحميل.")

    async def download(self, job, folder, progress, cancel_event=None):
        args = ytdlp_args() + ["--newline", "--no-color", "--progress", "--progress-template",
            "download:PROGRESS %(progress._percent_str)s %(progress._speed_str)s ETA %(progress._eta_str)s",
            "--concurrent-fragments", "3", "--max-filesize", str(MAX_FILE_BYTES),
            "--output", str(folder / "%(id).80s.%(ext)s"), "--print", "after_move:FINAL:%(filepath)s"]
        if job.mode == "a":
            args += ["--format", "bestaudio/best", "--extract-audio", "--audio-format", "mp3",
                     "--audio-quality", f"{job.quality}K"]
        else:
            cap = "" if job.quality == "best" else f"[height<={job.quality}]"
            args += ["--format", f"bestvideo{cap}+bestaudio/best{cap}", "--merge-output-format", "mkv"]
        args += ["--", job.media.url]
        paths = []
        async def receive(line):
            if line.startswith("PROGRESS "):
                await progress.update("جاري التنزيل:\n" + line.removeprefix("PROGRESS "))
            elif line.startswith("FINAL:"):
                paths.append(Path(line.removeprefix("FINAL:")).resolve())
        def check_resources():
            total = 0
            for path in folder.rglob("*"):
                with contextlib.suppress(FileNotFoundError):
                    if path.is_file():
                        total += path.stat().st_size
            if total > MAX_FILE_BYTES * 3:
                raise ValueError("تجاوزت الملفات المؤقتة الحد المسموح.")
            if shutil.disk_usage(folder).free < 256 * 1024 * 1024:
                raise ValueError("مساحة السيرفر المتاحة غير كافية.")
        await run_process(args, DOWNLOAD_TIMEOUT, receive, check_resources, cancel_event)
        if not paths:
            raise ValueError("لم ينتج ملف نهائي. قد يكون الحجم أكبر من الحد.")
        path = paths[-1]
        if folder.resolve() not in path.parents or not path.is_file():
            raise ValueError("مسار الملف الناتج غير صالح.")
        if not path.stat().st_size:
            raise ValueError("الملف الناتج فارغ.")
        if path.stat().st_size > MAX_FILE_BYTES:
            raise ValueError(f"الملف أكبر من الحد {MAX_FILE_MB} MB. اختر جودة أقل.")
        return path

    async def probe(self, path, cancel_event=None):
        result = await run_process(["ffprobe", "-v", "error", "-show_entries",
            "format=duration:stream=codec_type,width,height,duration", "-of", "json", str(path)],
            30, cancel_event=cancel_event)
        try:
            return json.loads("\n".join(result))
        except json.JSONDecodeError:
            return {}

    async def upload(self, job, path, progress, cancel_event=None):
        metadata = await self.probe(path, cancel_event)
        seconds = int(float(metadata.get("format", {}).get("duration") or job.media.duration or 0))
        attributes = []
        if job.mode == "a":
            attributes.append(DocumentAttributeAudio(duration=seconds, title=job.media.title[:64],
                                                     performer=job.media.uploader[:64]))
        else:
            video = next((s for s in metadata.get("streams", []) if s.get("codec_type") == "video"), {})
            if video:
                attributes.append(DocumentAttributeVideo(duration=seconds, w=int(video.get("width") or 0),
                    h=int(video.get("height") or 0), supports_streaming=path.suffix.lower() == ".mp4"))
        caption = (f"{job.media.title}\nالناشر: {job.media.uploader}\nالمدة: {duration_text(seconds)}\n"
                   f"الحجم: {human_size(path.stat().st_size)}")
        async def upload_progress(current, total):
            if cancel_event and cancel_event.is_set():
                raise TaskCancelled()
            await progress.update(f"جاري الرفع إلى تيليجرام: {current * 100 / total if total else 0:.1f}%\n"
                                  f"{human_size(current)} / {human_size(total)}")
        if cancel_event and cancel_event.is_set():
            raise TaskCancelled()
        await self.client.send_file(job.media.chat_id, str(path), caption=caption, parse_mode=None,
            reply_to=job.message_id, attributes=attributes or None,
            supports_streaming=job.mode == "v" and path.suffix.lower() == ".mp4",
            force_document=job.mode == "v" and path.suffix.lower() != ".mp4",
            progress_callback=upload_progress)

    async def retry_wait(self, seconds, cancel_event=None):
        if cancel_event is None:
            await asyncio.sleep(seconds)
            return
        try:
            await asyncio.wait_for(cancel_event.wait(), timeout=seconds)
            raise TaskCancelled()
        except asyncio.TimeoutError:
            return

    async def transfer(self, job, folder, progress, cancel_event=None):
        path = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                await progress.update(f"محاولة التنزيل {attempt}/{MAX_ATTEMPTS}", force=True)
                path = await self.download(job, folder, progress, cancel_event)
                break
            except TaskCancelled:
                raise
            except Exception as error:
                logger.warning("Download attempt %s failed: %s", attempt, error)
                if permanent_error(error) or attempt == MAX_ATTEMPTS:
                    raise
                await self.retry_wait(RETRY_DELAY * attempt, cancel_event)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                await progress.update(f"جاري الرفع؛ محاولة {attempt}/{MAX_ATTEMPTS}", force=True)
                await self.upload(job, path, progress, cancel_event)
                return
            except TaskCancelled:
                raise
            except Exception as error:
                logger.warning("Upload attempt %s failed: %s", attempt, error)
                if attempt == MAX_ATTEMPTS:
                    raise
                delay = error.seconds + 1 if isinstance(error, FloodWaitError) else RETRY_DELAY * attempt
                await self.retry_wait(delay, cancel_event)

    async def process_job(self, job):
        folder = Path(tempfile.mkdtemp(prefix="job-", dir=DOWNLOAD_ROOT))
        status = None
        try:
            if shutil.disk_usage(folder).free < 512 * 1024 * 1024:
                raise ValueError("مساحة السيرفر منخفضة. حاول لاحقًا.")
            status = await self.client.send_message(job.media.chat_id, "جاري تجهيز التحميل...",
                                                    reply_to=job.message_id, parse_mode=None)
            progress = Progress(status)
            await self.transfer(job, folder, progress)
            self.cache.pop(job.media.token, None)
            with contextlib.suppress(Exception):
                await self.client.edit_message(job.media.chat_id, job.message_id, buttons=None)
            await progress.update("تم إرسال الملف بنجاح وحذف كاش الطلب.", force=True)
        except Exception as error:
            logger.exception("Job failed")
            with contextlib.suppress(Exception):
                if status:
                    await status.edit(friendly_error(error), parse_mode=None)
                else:
                    await self.client.send_message(job.media.chat_id, friendly_error(error), parse_mode=None)
        finally:
            shutil.rmtree(folder, ignore_errors=True)
            self.busy_users.discard(job.media.owner_id)

    async def worker(self, number):
        logger.info("Worker %s started", number)
        while True:
            job = await self.queue.get()
            try:
                await self.process_job(job)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Worker recovered from unexpected failure")
            finally:
                self.queue.task_done()


# =========================================================
# Added collection workflow: playlists and TikTok profiles
# =========================================================

class CollectionMediaBot(MediaBot):
    def __init__(self):
        super().__init__()
        self.collections = OrderedDict()
        self.collection_by_user = {}
        self.background_tasks = set()

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)
        def done(finished):
            self.background_tasks.discard(finished)
            if not finished.cancelled() and finished.exception():
                error = finished.exception()
                logger.error("Background task failed", exc_info=(type(error), error, error.__traceback__))
        task.add_done_callback(done)
        return task

    @staticmethod
    def collection_kind(url):
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        path = parsed.path.rstrip("/")
        if (host == "youtube.com" or host.endswith(".youtube.com") or host == "youtu.be"):
            if parse_qs(parsed.query).get("list") or path == "/playlist":
                return "youtube"
        if (host == "tiktok.com" or host.endswith(".tiktok.com")) and re.fullmatch(r"/@[^/]+", path):
            return "tiktok"
        return None

    def drop_collection(self, token):
        collection = self.collections.pop(token, None)
        if collection:
            if self.collection_by_user.get(collection.owner_id) == token:
                self.collection_by_user.pop(collection.owner_id, None)
            collection.entries.clear()
            collection.selected.clear()
            collection.failed.clear()

    def prune_collections(self):
        now = time.monotonic()
        active = {"scan", "running", "queued"}
        for token, collection in list(self.collections.items()):
            ttl = FAILED_CACHE_TTL if collection.stage == "failed" else CACHE_TTL
            if collection.stage not in active and now - collection.touched > ttl:
                self.drop_collection(token)
        if len(self.collections) > CACHE_LIMIT:
            for token, collection in list(self.collections.items()):
                if len(self.collections) <= CACHE_LIMIT:
                    break
                if collection.stage not in active:
                    self.drop_collection(token)

    @staticmethod
    def cb(collection, action, label):
        return Button.inline(label, f"col|{collection.token}|{action}".encode())

    async def panel(self, collection, message, text, rows):
        collection.touched = time.monotonic()
        await message.edit(text, buttons=rows, parse_mode=None, link_preview=False)

    async def scan_collection(self, url, cancel_event):
        result = {}
        async def receive(line):
            if line.startswith("{"):
                with contextlib.suppress(json.JSONDecodeError):
                    result.update(json.loads(line))
        args = [x for x in ytdlp_args() if x != "--no-playlist"]
        args += ["--yes-playlist", "--flat-playlist", "--skip-download", "--quiet",
                 "--playlist-end", str(MAX_COLLECTION_ITEMS + 1), "--dump-single-json", "--", url]
        await run_process(args, COLLECTION_INFO_TIMEOUT, receive, cancel_event=cancel_event)
        if not result:
            raise ValueError("لم أستطع قراءة المجموعة من هذا الرابط.")
        if result.get("_type") not in {"playlist", "multi_video"}:
            raise ValueError("الرابط لم يُرجع مجموعة فيديوهات. أرسل رابط القائمة أو الحساب كاملًا.")
        entries = []
        seen = set()
        raw_entries = result.get("entries") or []
        for item in raw_entries:
            if not item:
                continue
            target = item.get("webpage_url") or item.get("url")
            if not target and item.get("id") and "youtube" in str(item.get("ie_key", "")).lower():
                target = "https://www.youtube.com/watch?v=" + str(item["id"])
            try:
                target = extract_url(target)
            except (ValueError, TypeError):
                continue
            if target in seen:
                continue
            seen.add(target)
            stamp = item.get("timestamp") or item.get("release_timestamp")
            if not stamp and item.get("upload_date"):
                with contextlib.suppress(ValueError):
                    stamp = datetime.strptime(item["upload_date"], "%Y%m%d").replace(tzinfo=timezone.utc).timestamp()
            with contextlib.suppress(ValueError, TypeError):
                stamp = float(stamp) if stamp else None
            if not isinstance(stamp, (float, int)):
                stamp = None
            entries.append(Entry(target, plain(item.get("title") or "فيديو", 200), stamp))
        if not entries:
            raise ValueError("لم يتم العثور على روابط فيديو متاحة. الحساب قد يكون مقيدًا أو يتطلب تسجيل دخول.")
        total_hint = result.get("playlist_count") or result.get("n_entries") or 0
        with contextlib.suppress(ValueError, TypeError):
            total_hint = int(total_hint)
        truncated = len(raw_entries) > MAX_COLLECTION_ITEMS
        if isinstance(total_hint, int) and total_hint > MAX_COLLECTION_ITEMS:
            truncated = True
        return result, entries[:MAX_COLLECTION_ITEMS], truncated

    async def on_message(self, event):
        if not event.is_private or not event.sender_id:
            return
        if not self.authorized(event.sender_id):
            return await super().on_message(event)
        self.prune_collections()
        token = self.collection_by_user.get(event.sender_id)
        collection = self.collections.get(token)
        raw = (event.raw_text or "").strip()
        if collection and collection.stage == "custom" and raw.isdecimal():
            count = int(raw)
            if not 1 <= count <= len(collection.entries):
                await event.reply(f"اكتب عددًا من 1 إلى {len(collection.entries)}.")
                return
            collection.count = count
            message = await self.client.get_messages(collection.chat_id, ids=collection.message_id)
            if message:
                await self.order_menu(collection, message)
            else:
                self.drop_collection(collection.token)
                await event.reply("الرسالة غير موجودة. أرسل الرابط مجددًا.")
            return
        if not URL_RE.search(raw):
            return await super().on_message(event)
        if event.sender_id in self.busy_users or event.sender_id in self.inspecting_users:
            await event.reply("لديك طلب يعمل حاليًا. انتظر انتهاءه أو ألغِ المهمة من زرها.")
            return
        if collection and collection.stage == "scan":
            await event.reply("انتظر انتهاء فحص المجموعة أو اضغط إلغاء.")
            return
        try:
            url = extract_url(raw)
        except ValueError:
            return await super().on_message(event)
        kind = self.collection_kind(url)
        if not kind:
            return await super().on_message(event)
        if collection:
            self.drop_collection(collection.token)
        status = await event.reply("جاري قراءة القائمة أو الحساب...", parse_mode=None)
        collection = Collection(secrets.token_hex(8), event.sender_id, event.chat_id,
                                status.id, tiktok=kind == "tiktok")
        self.collections[collection.token] = collection
        self.collection_by_user[event.sender_id] = collection.token
        await status.edit("جاري اكتشاف الفيديوهات. الحساب الكبير قد يستغرق وقتًا...",
                          buttons=[[self.cb(collection, "cancel", "إلغاء")]], parse_mode=None)
        self.spawn(self.prepare_collection(collection, status, url))

    async def prepare_collection(self, collection, message, url):
        try:
            async with self.info_slots:
                info, entries, truncated = await self.scan_collection(url, collection.cancel)
            if collection.cancel.is_set():
                raise TaskCancelled()
            collection.entries = entries
            collection.truncated = truncated
            collection.title = plain(info.get("title") or "مجموعة فيديوهات", 200)
            collection.uploader = plain(info.get("uploader") or info.get("channel") or "غير متاح", 100)
            collection.chronological = all(x.timestamp is not None for x in entries)
            collection.stage = "count"
            rows = [[self.cb(collection, "n:all", "كل المكتشف")]]
            choices = [self.cb(collection, f"n:{n}", str(n)) for n in (10, 25, 50, 100) if n <= len(entries)]
            rows += [choices[i:i+3] for i in range(0, len(choices), 3)]
            rows += [[self.cb(collection, "custom", "تحديد عدد آخر")], [self.cb(collection, "cancel", "إلغاء")]]
            warning = ""
            if truncated:
                warning = f"\nوصل الفحص لحد {MAX_COLLECTION_ITEMS} عنصر؛ النتائج ليست المجموعة كاملة."
            await self.panel(collection, message,
                f"{collection.title}\nالناشر: {collection.uploader}\nالروابط المكتشفة: {len(entries)}"
                f"{warning}\n\nاختر العدد المطلوب:", rows)
        except Exception as error:
            if not isinstance(error, TaskCancelled):
                logger.exception("Collection scan failed")
            with contextlib.suppress(Exception):
                await message.edit(friendly_error(error), buttons=None, parse_mode=None)
            self.drop_collection(collection.token)

    async def enrich_dates(self, collection, message):
        collection.stage = "scan"
        progress = Progress(message, buttons=[[self.cb(collection, "cancel", "إلغاء")]])
        try:
            for index, entry in enumerate(collection.entries, 1):
                if collection.cancel.is_set():
                    raise TaskCancelled()
                if entry.timestamp is not None:
                    continue
                await progress.update(f"فحص تواريخ النشر: {index}/{len(collection.entries)}\nهذه الخطوة قد تستغرق وقتًا طويلًا.")
                result = {}
                async def receive(line):
                    if line.startswith("{"):
                        with contextlib.suppress(json.JSONDecodeError):
                            result.update(json.loads(line))
                try:
                    async with self.info_slots:
                        await run_process(ytdlp_args() + ["--quiet", "--skip-download", "--dump-single-json", "--", entry.url],
                                          INFO_TIMEOUT, receive, cancel_event=collection.cancel)
                    stamp = result.get("timestamp") or result.get("release_timestamp")
                    if not stamp and result.get("upload_date"):
                        stamp = datetime.strptime(result["upload_date"], "%Y%m%d").replace(tzinfo=timezone.utc).timestamp()
                    if stamp:
                        entry.timestamp = float(stamp)
                except TaskCancelled:
                    raise
                except Exception as error:
                    logger.warning("Could not obtain publication date: %s", error)
                    if "sign in to confirm" in str(error).lower() or "429" in str(error):
                        break
                await self.retry_wait(BETWEEN_VIDEOS, collection.cancel)
            collection.chronological = all(e.timestamp is not None for e in collection.entries)
            collection.dates_checked = True
            await self.order_menu(collection, message)
        except TaskCancelled:
            self.drop_collection(collection.token)
            with contextlib.suppress(Exception):
                await message.edit("تم إلغاء فحص التواريخ وحذف الكاش.", buttons=None, parse_mode=None)
        except Exception:
            logger.exception("Date inspection failed")
            self.drop_collection(collection.token)
            with contextlib.suppress(Exception):
                await message.edit("تعذر فحص التواريخ. أرسل الرابط مجددًا.", buttons=None, parse_mode=None)

    async def order_menu(self, collection, message):
        collection.stage = "order"
        rows = []
        if collection.tiktok and collection.chronological:
            rows.append([self.cb(collection, "o:new", "الأحدث أولًا"), self.cb(collection, "o:old", "الأقدم أولًا")])
            note = "الاختيار حسب تواريخ النشر المتاحة."
            if collection.truncated:
                note += "\nداخل الجزء المكتشف فقط؛ لا يمكن ضمان أقدم فيديوهات الحساب كله."
        else:
            rows.append([self.cb(collection, "o:normal", "أول القائمة أولًا"),
                         self.cb(collection, "o:reverse", "آخر القائمة أولًا")])
            note = ("لم تتوفر تواريخ موثوقة لكل العناصر؛ ترتيب المصدر ليس ضمانًا للأحدث أو الأقدم."
                    if collection.tiktok else "ترتيب بلاي ليست يوتيوب ليس بالضرورة ترتيب تاريخ النشر.")
        if collection.tiktok and not collection.chronological and not getattr(collection, "dates_checked", False):
            rows.append([self.cb(collection, "dates", "فحص التواريخ لإتاحة الأحدث/الأقدم (بطيء)")])
        rows.append([self.cb(collection, "cancel", "إلغاء")])
        await self.panel(collection, message,
            f"المطلوب: {collection.count}\n{note}\n\nاختر الترتيب قبل تحديد العدد من القائمة:", rows)

    @staticmethod
    def select_entries(collection, order):
        entries = list(collection.entries)
        labels = {"normal": "أول القائمة", "reverse": "آخر القائمة", "new": "الأحدث", "old": "الأقدم"}
        if order not in labels:
            raise ValueError("ترتيب غير صالح.")
        if order in ("new", "old"):
            if not collection.chronological:
                raise ValueError("تواريخ النشر غير مكتملة؛ لا يمكن ترتيب القائمة زمنيًا بدقة.")
            entries.sort(key=lambda entry: entry.timestamp, reverse=order == "new")
        elif order == "reverse":
            entries.reverse()
        return entries[:collection.count], labels[order]

    async def collection_quality_menu(self, collection, message):
        collection.stage = "quality"
        rows = []
        for group in (("360", "480", "720"), ("1080", "1440", "2160")):
            rows.append([self.cb(collection, f"q:v:{q}", f"حتى {q}p") for q in group])
        rows += [[self.cb(collection, "q:v:best", "أفضل فيديو متاح")],
                 [self.cb(collection, f"q:a:{q}", f"MP3 {q}") for q in ("128", "192", "320")],
                 [self.cb(collection, "cancel", "إلغاء")]]
        await self.panel(collection, message,
            "اختر الجودة المشتركة للمهمة.\nالدقة سقف وليست ضمانًا لتوفرها لكل فيديو.\n"
            "ترميز MP3 الأعلى لا يحسن جودة المصدر.", rows)

    async def on_collection_button(self, event):
        try:
            _, token, action = event.data.decode().split("|", 2)
        except (ValueError, UnicodeDecodeError):
            return
        self.prune_collections()
        collection = self.collections.get(token)
        if not collection:
            await event.answer("انتهت صلاحية الطلب. أرسل الرابط مجددًا.", alert=True)
            return
        if (event.sender_id != collection.owner_id or event.chat_id != collection.chat_id
                or not self.authorized(event.sender_id)):
            await event.answer("الطلب يخص صاحبه فقط.", alert=True)
            return
        collection.touched = time.monotonic()
        await event.answer()
        message = await event.get_message()
        stage = collection.stage
        if action == "cancel":
            collection.cancel.set()
            if stage not in {"scan", "running", "queued"}:
                self.drop_collection(token)
                await message.edit("تم حذف كاش الطلب.", buttons=None, parse_mode=None)
            else:
                await event.respond("تم طلب الإلغاء. سيتم إيقاف المعالجة وتنظيف الملفات.")
            return
        if stage in {"scan", "running", "queued"}:
            return
        if action == "custom" and stage == "count":
            collection.stage = "custom"
            await self.panel(collection, message, f"اكتب عددًا من 1 إلى {len(collection.entries)} في رسالة عادية.",
                             [[self.cb(collection, "cancel", "إلغاء")]])
        elif action.startswith("n:") and stage == "count":
            raw = action.split(":", 1)[1]
            if raw != "all" and not raw.isdecimal():
                return
            collection.count = len(collection.entries) if raw == "all" else min(int(raw), len(collection.entries))
            if collection.count < 1:
                return
            await self.order_menu(collection, message)
        elif action == "dates" and stage == "order" and collection.tiktok:
            collection.stage = "scan"
            self.spawn(self.enrich_dates(collection, message))
        elif action.startswith("o:") and stage == "order":
            collection.selected, collection.order = self.select_entries(collection, action.split(":")[1])
            await self.collection_quality_menu(collection, message)
        elif action.startswith("q:") and stage == "quality":
            parts = action.split(":")
            if len(parts) != 3:
                return
            _, mode, quality = parts
            if mode not in {"v", "a"} or (mode == "v" and quality not in VIDEO_CHOICES) or (mode == "a" and quality not in AUDIO_CHOICES):
                return
            collection.mode, collection.quality, collection.stage = mode, quality, "confirm"
            await self.panel(collection, message,
                f"{collection.title}\nالعدد: {len(collection.selected)}\nالترتيب: {collection.order}\n"
                f"النوع: {'فيديو' if mode == 'v' else 'MP3'}\nالجودة: {quality}\n\nهل تبدأ؟",
                [[self.cb(collection, "start", "بدء التحميل")], [self.cb(collection, "cancel", "إلغاء")]])
        elif (action == "start" and stage == "confirm") or (action == "retry" and stage == "failed"):
            if collection.owner_id in self.busy_users:
                await event.respond("لديك طلب آخر قيد المعالجة.")
                return
            if self.queue.full():
                await event.respond("الطابور ممتلئ. حاول بعد قليل.")
                return
            if action == "retry":
                collection.selected = list(collection.failed)
                collection.failed.clear()
                collection.cancel = asyncio.Event()
            collection.stage = "queued"
            self.busy_users.add(collection.owner_id)
            self.queue.put_nowait(CollectionJob(collection))
            await self.panel(collection, message, "تمت إضافة المهمة إلى الطابور.",
                             [[self.cb(collection, "cancel", "إلغاء بقية المهمة")]])

    async def process_job(self, job):
        if isinstance(job, CollectionJob):
            await self.process_collection(job.collection)
        else:
            await super().process_job(job)

    async def process_collection(self, collection):
        total = len(collection.selected)
        sent = 0
        failures = []
        cancelled = False
        interrupted = False
        status = None
        try:
            if collection.cancel.is_set():
                raise TaskCancelled()
            collection.stage = "running"
            status = await self.client.send_message(collection.chat_id, "جاري بدء المهمة...",
                reply_to=collection.message_id, parse_mode=None,
                buttons=[[self.cb(collection, "cancel", "إلغاء بقية المهمة")]])
            progress = Progress(status, buttons=[[self.cb(collection, "cancel", "إلغاء بقية المهمة")]])
            for index, entry in enumerate(collection.selected, 1):
                if collection.cancel.is_set():
                    raise TaskCancelled()
                folder = Path(tempfile.mkdtemp(prefix="collection-", dir=DOWNLOAD_ROOT))
                prefix = f"الفيديو {index}/{total}\nتم الإرسال: {sent} | فشل: {len(failures)}\n{entry.title}"
                child_progress = PrefixProgress(progress, prefix)
                try:
                    if shutil.disk_usage(folder).free < 512 * 1024 * 1024:
                        raise ValueError("مساحة السيرفر منخفضة.")
                    await child_progress.update("جاري فحص الفيديو وحد المدة...", force=True)
                    info = None
                    for attempt in range(1, MAX_ATTEMPTS + 1):
                        try:
                            async with self.info_slots:
                                info = await self.inspect(entry.url, collection.cancel)
                            break
                        except TaskCancelled:
                            raise
                        except Exception as error:
                            if permanent_error(error) or attempt == MAX_ATTEMPTS:
                                raise
                            await self.retry_wait(RETRY_DELAY * attempt, collection.cancel)
                    proxy = type("Owner", (), {"sender_id": collection.owner_id, "chat_id": collection.chat_id})()
                    media = self.make_media(info, entry.url, proxy)
                    child = Job(media, collection.mode, collection.quality, collection.message_id)
                    await self.transfer(child, folder, child_progress, collection.cancel)
                    sent += 1
                except TaskCancelled:
                    raise
                except Exception as error:
                    logger.exception("Collection item failed: %s", entry.url)
                    failures.append(entry)
                    await child_progress.update(friendly_error(error), force=True)
                finally:
                    shutil.rmtree(folder, ignore_errors=True)
                if index < total:
                    await self.retry_wait(BETWEEN_VIDEOS, collection.cancel)
        except TaskCancelled:
            cancelled = True
        except asyncio.CancelledError:
            interrupted = True
            raise
        except Exception:
            interrupted = True
            logger.exception("Collection interrupted unexpectedly")
        finally:
            self.busy_users.discard(collection.owner_id)
            summary = ("تم إلغاء المهمة." if cancelled else "توقفت المهمة." if interrupted else "انتهت المهمة.")
            summary += f"\nالمطلوب: {total}\nتم الإرسال: {sent}\nفشل: {len(failures)}"
            if failures and not cancelled and not interrupted:
                collection.failed = failures
                collection.entries.clear()
                collection.selected.clear()
                collection.stage = "failed"
                collection.touched = time.monotonic()
                summary += f"\nتم حذف الملفات. كاش الفاشل فقط محفوظ لمدة {FAILED_CACHE_TTL // 60} دقيقة."
                rows = [[self.cb(collection, "retry", "إعادة محاولة الفاشل")],
                        [self.cb(collection, "cancel", "حذف الكاش")]]
            else:
                self.drop_collection(collection.token)
                summary += "\nتم حذف ملفات وكاش المهمة."
                rows = None
            with contextlib.suppress(Exception):
                await self.client.edit_message(collection.chat_id, collection.message_id,
                                               summary, buttons=rows, parse_mode=None)
            if status:
                with contextlib.suppress(Exception):
                    await status.edit(summary, buttons=None, parse_mode=None)

    async def cleanup_loop(self):
        while True:
            await asyncio.sleep(30)
            self.trim_cache()
            self.prune_collections()

    async def run(self):
        await self.client.start(bot_token=BOT_TOKEN)
        self.client.add_event_handler(self.on_message, events.NewMessage(incoming=True))
        self.client.add_event_handler(self.on_button, events.CallbackQuery(pattern=rb"^get\|"))
        self.client.add_event_handler(self.on_collection_button, events.CallbackQuery(pattern=rb"^col\|"))
        workers = [asyncio.create_task(self.worker(i)) for i in range(1, WORKERS + 1)]
        cleanup = asyncio.create_task(self.cleanup_loop())
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.client.disconnect()))
        me = await self.client.get_me()
        logger.info("Bot started: @%s", me.username)
        try:
            await self.client.run_until_disconnected()
        finally:
            tasks = workers + [cleanup] + list(self.background_tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.cache.clear()
            for token in list(self.collections):
                self.drop_collection(token)
            while not self.queue.empty():
                with contextlib.suppress(asyncio.QueueEmpty):
                    self.queue.get_nowait()
                    self.queue.task_done()
            await self.client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(CollectionMediaBot().run())
    except KeyboardInterrupt:
        pass
