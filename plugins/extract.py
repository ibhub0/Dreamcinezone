import asyncio
import os
import logging
import aiofiles
import tempfile
import uuid

from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from telegraph import Telegraph
from pymediainfo import MediaInfo

from database.ia_filterdb import get_file_details
from info import BIN_CHANNEL
from dreamxbotz.util.file_properties import get_name

logger = logging.getLogger(__name__)

# TELEGRAPH: prefer env var; if missing, create an account (network call).
TELEGRAPH_ACCESS_TOKEN = os.environ.get("TELEGRAPH_ACCESS_TOKEN")
if TELEGRAPH_ACCESS_TOKEN:
    telegraph = Telegraph(access_token=TELEGRAPH_ACCESS_TOKEN)
else:
    telegraph = Telegraph()
    try:
        telegraph.create_account(short_name="DreamxBotz")
        TELEGRAPH_ACCESS_TOKEN = telegraph.get_access_token()
    except Exception:
        # If account creation fails, keep telegraph object but page creation will fail later and be handled.
        logger.exception("Failed to create Telegraph account at start-up.")


@Client.on_callback_query(filters.regex(r"^extract_data"), group=2)
async def extract_data_handler(client: Client, query: CallbackQuery):
    _, file_id = query.data.split(":")

    # Replace the clicked button to a waiting button (non-spammy)
    current_markup = query.message.reply_markup
    wait_keyboard = []
    if current_markup and getattr(current_markup, "inline_keyboard", None):
        for row in current_markup.inline_keyboard:
            new_row = []
            for btn in row:
                if btn.callback_data == query.data:
                    new_row.append(InlineKeyboardButton("ᴘʟᴇᴀꜱᴇ ᴡᴀɪᴛ... ⏳", callback_data="wait_data"))
                else:
                    new_row.append(btn)
            wait_keyboard.append(new_row)

    try:
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(wait_keyboard))
    except Exception:
        # editing markup is non-critical; continue
        logger.debug("Failed to set waiting markup, continuing.")

    # unique temp file in system temp dir
    temp_path = os.path.join(tempfile.gettempdir(), f"acc_{query.from_user.id}_{query.message.id}_{uuid.uuid4().hex}.tmp")

    try:
        files_ = await get_file_details(file_id)
        if not files_:
            await query.answer("File not found in DB.", show_alert=True)
            # revert markup
            await _safe_revert_markup(query, current_markup)
            return

        # send cached media so we have a message object to stream from
        log_msg = await client.send_cached_media(chat_id=BIN_CHANNEL, file_id=file_id)
        file_name = get_name(log_msg)
        # make filename nicer for page title
        safe_title = file_name.replace(".", " ").replace("_", " ").replace("-", " ").replace("[", "").replace("]", "").replace("(", "").replace(")", "").replace("mkv", "").replace("mp4", "")

        # determine media object for file_size hint
        media = getattr(log_msg, log_msg.media.value) if log_msg.media else None
        file_size = getattr(media, "file_size", None) or getattr(log_msg, "file_size", None) or 0

        # choose chunk count adaptively (small files -> fewer chunks)
        if file_size and file_size > 200 * 1024 * 1024:
            chunk_limit = 4
        else:
            chunk_limit = 2

        # stream first N chunks with an overall timeout to avoid hangs
        try:
            stream_timeout = 12  # seconds total for the streaming loop
            async def _stream_to_file():
                async with aiofiles.open(temp_path, "wb") as f:
                    async for chunk in client.stream_media(log_msg, limit=chunk_limit):
                        await f.write(chunk)

            await asyncio.wait_for(_stream_to_file(), timeout=stream_timeout)
        except asyncio.TimeoutError:
            logger.warning("Streaming partial file timed out.")
            # continue with whatever we have (partial)
        except Exception as e:
            logger.exception("Error while streaming partial file.")
            await query.answer(f"Stream error: {e}", show_alert=True)
            await _safe_revert_markup(query, current_markup)
            return

        # Prepare path to MediaInfo DLL if present (Windows dev), else None for Linux
        lib_path = os.path.abspath("MediaInfo.dll") if os.path.exists("MediaInfo.dll") else None

        # Parse with timeout to avoid hangs on corrupted partial files
        try:
            media_info = await asyncio.wait_for(
                asyncio.to_thread(MediaInfo.parse, temp_path, library_file=lib_path),
                timeout=6
            )
        except asyncio.TimeoutError:
            logger.warning("MediaInfo.parse timed out.")
            await query.answer("Metadata scan timed out.", show_alert=True)
            await _safe_revert_markup(query, current_markup)
            return
        except OSError as e:
            # likely missing DLL on Windows; give helpful message
            logger.exception("MediaInfo library error.")
            await query.answer("MediaInfo native library not found on server.", show_alert=True)
            await _safe_revert_markup(query, current_markup)
            return
        except Exception as e:
            logger.exception("MediaInfo.parse failed.")
            await query.answer(f"Metadata parse error: {e}", show_alert=True)
            await _safe_revert_markup(query, current_markup)
            return

        # Safety: ensure we have tracks
        if not getattr(media_info, "tracks", None):
            await query.answer("No readable metadata found.", show_alert=True)
            await _safe_revert_markup(query, current_markup)
            return

        # Collect tracks (dedupe while preserving order)
        audio_tracks = []
        subtitle_tracks = []
        video_info = []
        seen_audio = set()
        seen_subs = set()

        for track in media_info.tracks:
            ttype = getattr(track, "track_type", "").lower()
            if ttype == "video":
                width = getattr(track, "width", "?") or "?"
                height = getattr(track, "height", "?") or "?"
                codec = getattr(track, "format", None) or getattr(track, "codec_id", None) or "Unknown"
                video_info.append(f"Video: {codec} {width}x{height}")

            elif ttype == "audio":
                lang = None
                # prefer other_language full names if available
                if getattr(track, "other_language", None):
                    try:
                        lang = track.other_language[0]
                    except Exception:
                        lang = track.language or "und"
                else:
                    lang = getattr(track, "language", None) or "und"

                if lang not in seen_audio:
                    seen_audio.add(lang)
                    audio_tracks.append(lang)

            elif ttype in ("text", "subtitle"):
                lang = None
                if getattr(track, "other_language", None):
                    try:
                        lang = track.other_language[0]
                    except Exception:
                        lang = track.language or "und"
                else:
                    lang = getattr(track, "language", None) or "und"

                if lang not in seen_subs:
                    seen_subs.add(lang)
                    subtitle_tracks.append(lang)

        # Heuristic: detect likely incomplete scan (no audio & no video but maybe subtitles)
        incomplete_scan = (not audio_tracks and not video_info) and bool(subtitle_tracks)

        # Build Telegraph HTML content
        page_parts = []

        if incomplete_scan:
            page_parts.append('<i>⚠️ Partial scan — metadata may be incomplete.</i><br><br>')

        page_parts.append("<h3><b>Available Tracks</b></h3><br>")

        # Video
        if video_info:
            page_parts.append("<b>Video Track:</b><br>")
            for v in video_info:
                page_parts.append(f"<blockquote>• {v}</blockquote>")
            page_parts.append("<br>")


        # Audio
        if audio_tracks:
            page_parts.append(f"<b>Audio Tracks ({len(audio_tracks)}):</b><br>")
            for a in audio_tracks:
                page_parts.append(f"<blockquote>• {a}</blockquote>")
            page_parts.append("<br>")
        else:
            page_parts.append("<b>Audio Tracks:</b> None<br><br>")

        # Subtitles
        if subtitle_tracks:
            page_parts.append(f"<b>Subtitle Tracks ({len(subtitle_tracks)}):</b><br>")
            for s in subtitle_tracks:
                page_parts.append(f"<blockquote>• {s}</blockquote>")
            page_parts.append("<br>")
        else:
            page_parts.append("<b>Subtitle Tracks:</b> None<br>")

        # Footer (FIXED)
        page_parts.append(
            '<code>Join <a href="https://t.me/DreamxBotz">DreamxBotz</a></code>'
        )

        page_content = "".join(page_parts)

        # Create Telegraph page off the event loop
        try:
            response = await asyncio.to_thread(
                telegraph.create_page,
                title=safe_title[:200],  # limit title length
                html_content=page_content,
                author_name="DreamxBotz"
            )
            telegraph_url = response.get("url")
        except Exception as e:
            logger.exception("Telegraph create_page failed.")
            await query.answer("Failed to create Telegraph page.", show_alert=True)
            await _safe_revert_markup(query, current_markup)
            return

        # Replace the button with a link
        success_keyboard = []
        if current_markup and getattr(current_markup, "inline_keyboard", None):
            for row in current_markup.inline_keyboard:
                new_row = []
                for btn in row:
                    if getattr(btn, "callback_data", None) == query.data:
                        new_row.append(InlineKeyboardButton("📝 ᴠɪᴇᴡ ᴛʀᴀᴄᴋꜱ 📝", url=telegraph_url))
                    else:
                        new_row.append(btn)
                success_keyboard.append(new_row)

        try:
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(success_keyboard))
        except Exception:
            logger.exception("Failed to edit message markup with success keyboard.")
            # best-effort: send a small message with the link
            try:
                await query.message.reply_text(f"Metadata: {telegraph_url}")
            except Exception:
                pass

    except Exception as e:
        logger.exception("Unhandled error in extract_data_handler.")
        await query.answer(f"Error: {e}", show_alert=True)
        # revert markup to original to avoid locking the button
        await _safe_revert_markup(query, current_markup)
    finally:
        # Cleanup temp file
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            logger.exception("Failed to remove temp file.")


# small helper to revert markup safely
async def _safe_revert_markup(query: CallbackQuery, markup):
    try:
        if markup:
            await query.edit_message_reply_markup(reply_markup=markup)
    except Exception:
        logger.debug("Could not revert markup; ignoring.")
