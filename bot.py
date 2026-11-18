import asyncio
import logging
import os
import re
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yt_dlp
from telethon import TelegramClient, events, Button
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeVideo,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("MediaDownloaderBot")

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "1900"))
MAX_DURATION_MINUTES = int(os.getenv("MAX_DURATION_MINUTES", "180"))
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "50"))
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2"))
DOWNLOAD_TIMEOUT_SECONDS = int(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "1800"))

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

URL_PATTERN = re.compile(r"https?://[^\s<>\"']+")
SAFE_NAME_PATTERN = re.compile(r"[^\w\u0600-\u06FF\-_. ]+", re.UNICODE)

VIDEO_QUALITIES = ["360", "480", "720", "1080", "1440", "best"]

QUALITY_FORMATS = {
    "360": "bestvideo[height<=360]+bestaudio/best[height<=360]/best",
    "480": "bestvideo[height<=480]+bestaudio/best[height<=480]/best",
    "720": "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
    "1080": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
    "1440": "bestvideo[height<=1440]+bestaudio/best[height<=1440]/best",
    "best": "bestvideo+bestaudio/best",
}


@dataclass
class DownloadTask:
    url: str
    mode: str
    quality: str
    chat_id: int
    message_id: int
    title: str
    uploader: str
    duration: int
    thumbnail: Optional[str] = None


class YdlLogger:
    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message):
        logger.error("yt-dlp: %s", message)


class MediaBot:
    def __init__(self):
        if not API_ID or not API_HASH or not BOT_TOKEN:
            raise RuntimeError(
                "يجب إضافة API_ID و API_HASH و BOT_TOKEN في متغيرات Railway."
            )

        self.client = TelegramClient(
            "bot_session",
            API_ID,
            API_HASH,
        )

        self.queue: asyncio.Queue[DownloadTask] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self.active_downloads = 0
        self.worker_started = False

    async def start(self):
        await self.client.start(bot_token=BOT_TOKEN)
        me = await self.client.get_me()

        logger.info("البوت يعمل الآن: @%s", me.username)

        self.client.add_event_handler(
            self.handle_start,
            events.NewMessage(pattern=r"^/start$"),
        )

        self.client.add_event_handler(
            self.handle_link,
            events.NewMessage(
                incoming=True,
                func=lambda event: event.raw_text and URL_PATTERN.search(event.raw_text),
            ),
        )

        self.client.add_event_handler(
            self.handle_button,
            events.CallbackQuery(pattern=r"^download\|"),
        )

        if not self.worker_started:
            self.worker_started = True
            asyncio.create_task(self.queue_worker())

        await self.client.run_until_disconnected()

    @staticmethod
    def extract_url(text: str) -> Optional[str]:
        match = URL_PATTERN.search(text)
        return match.group(0) if match else None

    @staticmethod
    def safe_name(title: str, media_id: str, extension: str) -> str:
        title = SAFE_NAME_PATTERN.sub(" ", title or "media")
        title = " ".join(title.split())[:60].strip() or "media"
        return f"{title}_{media_id[:10]}.{extension}"

    @staticmethod
    def human_size(size: float) -> str:
        units = ["B", "KB", "MB", "GB"]
        index = 0

        while size >= 1024 and index < len(units) - 1:
            size /= 1024
            index += 1

        return f"{size:.1f} {units[index]}"

    @staticmethod
    def format_duration(seconds: int) -> str:
        if not seconds:
            return "غير معروفة"

        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        if hours:
            return f"{hours}:{minutes:02d}:{seconds:02d}"

        return f"{minutes}:{seconds:02d}"

    async def extract_info(self, url: str) -> dict:
        options = {
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "skip_download": True,
            "logger": YdlLogger(),
        }

        def run():
            with yt_dlp.YoutubeDL(options) as ydl:
                return ydl.extract_info(url, download=False)

        return await asyncio.to_thread(run)

    def build_info_text(self, info: dict) -> str:
        title = info.get("title") or "وسائط"
        uploader = info.get("uploader") or info.get("channel") or "غير معروف"
        duration = int(info.get("duration") or 0)
        views = info.get("view_count") or 0
        extractor = info.get("extractor_key") or info.get("extractor") or "غير معروف"

        text = (
            f"🎬 **{title}**\n\n"
            f"👤 **القناة:** {uploader}\n"
            f"⏱️ **المدة:** {self.format_duration(duration)}\n"
            f"👁️ **المشاهدات:** {views:,}\n"
            f"🌐 **المصدر:** {extractor}\n\n"
            f"اختر نوع التحميل من الأزرار أدناه:"
        )

        return text

    def build_buttons(self, media_id: str) -> list:
        video_buttons = []

        for quality in VIDEO_QUALITIES:
            video_buttons.append(
                Button.inline(
                    f"🎬 {quality if quality != 'best' else 'أفضل جودة'}",
                    f"download|video|{quality}|{media_id}".encode(),
                )
            )

        rows = [
            video_buttons[:3],
            video_buttons[3:],
            [
                Button.inline(
                    "🎵 تحميل صوت MP3",
                    f"download|audio|best|{media_id}".encode(),
                )
            ],
        ]

        return [row for row in rows if row]

    async def handle_start(self, event):
        await event.reply(
            "👋 أرسل لي أي رابط فيديو مباشرةً، وسأعرض لك معلوماته وأزرار التحميل.\n\n"
            "يدعم البوت YouTube وTikTok وFacebook وInstagram وX وغيرها من المواقع المدعومة في yt-dlp."
        )

    async def handle_link(self, event):
        url = self.extract_url(event.raw_text)

        if not url:
            return

        status = await event.reply("🔎 جاري قراءة معلومات الرابط...")

        try:
            info = await self.extract_info(url)

            if not info:
                await status.edit("❌ لم أتمكن من قراءة هذا الرابط.")
                return

            if info.get("_type") == "playlist" and info.get("entries"):
                entries = list(info.get("entries") or [])

                if not entries:
                    await status.edit("❌ لا توجد وسائط قابلة للتنزيل في هذا الرابط.")
                    return

                info = entries[0]

            media_id = str(info.get("id") or int(time.time()))
            info_text = self.build_info_text(info)
            buttons = self.build_buttons(media_id)

            await status.edit(
                info_text,
                buttons=buttons,
                link_preview=False,
            )

            self.cache_media_info(media_id, url, info)

        except yt_dlp.utils.DownloadError as error:
            message = str(error)

            if "Private video" in message:
                text = "🔒 هذا المحتوى خاص أو يتطلب تسجيل دخول."
            elif "Sign in to confirm" in message:
                text = "🔐 الموقع يطلب تحققًا أو تسجيل دخول إضافي."
            elif "Unsupported URL" in message:
                text = "❌ هذا الرابط غير مدعوم حاليًا."
            else:
                text = f"❌ تعذر قراءة الرابط:\n`{message[:350]}`"

            await status.edit(text)
            logger.error("DownloadError: %s", message)

        except Exception:
            logger.error("Info error:\n%s", traceback.format_exc())
            await status.edit("❌ حدث خطأ أثناء قراءة معلومات الرابط.")

    def cache_media_info(self, media_id: str, url: str, info: dict):
        if not hasattr(self, "media_cache"):
            self.media_cache = {}

        self.media_cache[media_id] = {
            "url": url,
            "title": info.get("title") or "media",
            "uploader": info.get("uploader") or info.get("channel") or "Unknown",
            "duration": int(info.get("duration") or 0),
            "thumbnail": info.get("thumbnail"),
        }

        if len(self.media_cache) > 300:
            oldest_key = next(iter(self.media_cache))
            self.media_cache.pop(oldest_key, None)

    async def handle_button(self, event):
        try:
            data = event.data.decode("utf-8")
            _, mode, quality, media_id = data.split("|", 3)
        except Exception:
            await event.answer("❌ بيانات الزر غير صالحة.", alert=True)
            return

        media_data = getattr(self, "media_cache", {}).get(media_id)

        if not media_data:
            await event.answer(
                "انتهت صلاحية هذا الزر. أرسل الرابط مرة أخرى.",
                alert=True,
            )
            return

        if self.queue.full():
            await event.answer("الطابور ممتلئ حاليًا، حاول بعد قليل.", alert=True)
            return

        task = DownloadTask(
            url=media_data["url"],
            mode=mode,
            quality=quality,
            chat_id=event.chat_id,
            message_id=event.message_id,
            title=media_data["title"],
            uploader=media_data["uploader"],
            duration=media_data["duration"],
            thumbnail=media_data.get("thumbnail"),
        )

        await self.queue.put(task)

        mode_text = "🎵 صوت MP3" if mode == "audio" else f"🎬 فيديو {quality}"
        await event.answer(f"تمت إضافة {mode_text} إلى الطابور.")
        await event.edit(
            "✅ تمت إضافة الطلب إلى الطابور.\n"
            f"🎯 النوع: {mode_text}\n\n"
            "سيتم إرسال الملف هنا بعد الانتهاء."
        )

    def build_ydl_options(
        self,
        task: DownloadTask,
        output_path: Path,
        progress_callback,
    ) -> dict:
        options = {
            "outtmpl": str(output_path),
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "retries": 5,
            "fragment_retries": 5,
            "socket_timeout": 30,
            "concurrent_fragment_downloads": 4,
            "restrictfilenames": True,
            "windowsfilenames": True,
            "overwrites": True,
            "logger": YdlLogger(),
            "progress_hooks": [progress_callback],
        }

        if task.mode == "audio":
            options.update(
                {
                    "format": "bestaudio/best",
                    "postprocessors": [
                        {
                            "key": "FFmpegExtractAudio",
                            "preferredcodec": "mp3",
                            "preferredquality": "192",
                        }
                    ],
                }
            )
        else:
            options.update(
                {
                    "format": QUALITY_FORMATS.get(
                        task.quality,
                        QUALITY_FORMATS["best"],
                    ),
                    "merge_output_format": "mp4",
                }
            )

        return options

    async def download_task(
        self,
        task: DownloadTask,
        status_message,
    ) -> Path:
        extension = "mp3" if task.mode == "audio" else "mp4"
        media_id = str(int(time.time() * 1000))
        output_path = DOWNLOAD_DIR / self.safe_name(
            task.title,
            media_id,
            extension,
        )

        progress_state = {
            "last_update": 0.0,
            "downloaded": 0,
            "total": 0,
        }

        def progress_hook(data):
            if data.get("status") != "downloading":
                return

            progress_state["downloaded"] = data.get("downloaded_bytes") or 0
            progress_state["total"] = (
                data.get("total_bytes")
                or data.get("total_bytes_estimate")
                or 0
            )

            now = time.time()

            if now - progress_state["last_update"] < 4:
                return

            progress_state["last_update"] = now
            downloaded = progress_state["downloaded"]
            total = progress_state["total"]

            if total:
                percent = downloaded / total * 100
                text = (
                    f"⬇️ جاري التنزيل: {percent:.1f}%\n"
                    f"📦 {self.human_size(downloaded)} من {self.human_size(total)}"
                )
            else:
                text = f"⬇️ جاري التنزيل...\n📦 {self.human_size(downloaded)}"

            asyncio.run_coroutine_threadsafe(
                self.safe_edit(status_message, text),
                asyncio.get_running_loop(),
            )

        options = self.build_ydl_options(
            task=task,
            output_path=output_path,
            progress_callback=progress_hook,
        )

        def run():
            with yt_dlp.YoutubeDL(options) as ydl:
                ydl.download([task.url])

        await asyncio.wait_for(
            asyncio.to_thread(run),
            timeout=DOWNLOAD_TIMEOUT_SECONDS,
        )

        if not output_path.exists():
            candidates = list(DOWNLOAD_DIR.glob("*.mp4")) + list(
                DOWNLOAD_DIR.glob("*.mp3")
            )

            if not candidates:
                raise FileNotFoundError("لم يتم إنشاء ملف التنزيل.")

            output_path = max(candidates, key=lambda path: path.stat().st_mtime)

        file_size = output_path.stat().st_size
        max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024

        if file_size > max_bytes:
            output_path.unlink(missing_ok=True)
            raise ValueError(
                f"حجم الملف {self.human_size(file_size)} أكبر من الحد المسموح "
                f"{MAX_FILE_SIZE_MB} MB."
            )

        return output_path

    async def safe_edit(self, message, text: str):
        try:
            await message.edit(text)
        except Exception:
            pass

    async def send_file(self, task: DownloadTask, file_path: Path):
        caption = (
            f"🎬 **{task.title}**\n"
            f"👤 {task.uploader}\n"
            f"⏱️ {self.format_duration(task.duration)}\n"
            f"📦 {self.human_size(file_path.stat().st_size)}"
        )

        if task.mode == "audio":
            await self.client.send_file(
                task.chat_id,
                file_path,
                caption=caption,
                reply_to=task.message_id,
                attributes=[
                    DocumentAttributeAudio(
                        duration=task.duration,
                        title=task.title[:64],
                        performer=task.uploader[:64],
                    )
                ],
            )
        else:
            await self.client.send_file(
                task.chat_id,
                file_path,
                caption=caption,
                reply_to=task.message_id,
                supports_streaming=True,
                attributes=[
                    DocumentAttributeVideo(
                        duration=task.duration,
                        w=0,
                        h=0,
                        supports_streaming=True,
                    )
                ],
            )

    async def process_task(self, task: DownloadTask):
        status_message = await self.client.send_message(
            task.chat_id,
            "⬇️ جاري تجهيز التنزيل...",
            reply_to=task.message_id,
        )

        file_path = None

        try:
            file_path = await self.download_task(task, status_message)

            await self.safe_edit(status_message, "📤 جاري الرفع إلى تيليجرام...")
            await self.send_file(task, file_path)
            await status_message.delete()

        except asyncio.TimeoutError:
            await self.safe_edit(
                status_message,
                "⏰ انتهت مهلة التنزيل. جرّب جودة أقل أو ملفًا أقصر.",
            )

        except yt_dlp.utils.DownloadError as error:
            message = str(error)

            if "Private video" in message:
                text = "🔒 المحتوى خاص أو يتطلب تسجيل دخول."
            elif "Sign in to confirm" in message:
                text = "🔐 الموقع يطلب تحققًا أو تسجيل دخول إضافي."
            elif "Unsupported URL" in message:
                text = "❌ الرابط غير مدعوم حاليًا."
            else:
                text = f"❌ فشل التنزيل:\n`{message[:350]}`"

            await self.safe_edit(status_message, text)
            logger.error("Download error: %s", message)

        except Exception:
            logger.error("Unexpected error:\n%s", traceback.format_exc())
            await self.safe_edit(
                status_message,
                "❌ حدث خطأ غير متوقع أثناء المعالجة.",
            )

        finally:
            if file_path and file_path.exists():
                try:
                    file_path.unlink()
                except Exception:
                    pass

    async def queue_worker(self):
        while True:
            task = await self.queue.get()

            while self.active_downloads >= MAX_CONCURRENT_DOWNLOADS:
                await asyncio.sleep(1)

            self.active_downloads += 1

            try:
                await self.process_task(task)
            finally:
                self.active_downloads -= 1
                self.queue.task_done()


if __name__ == "__main__":
    bot = MediaBot()

    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        logger.info("تم إيقاف البوت.")
