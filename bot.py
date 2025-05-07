import os
import logging
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.error import BadRequest
import re
import subprocess
import time
import aiofiles
import aiofiles.os
from yt_dlp.utils import DownloadError, ExtractorError
from urllib.parse import urlparse, urlunparse
import hashlib

# Импортируем наши модули
import config
from downloader import VideoDownloader, data_lock, active_downloads, canonical_url_map
from database import Database
from localization import get_message # Импортируем функцию локализации

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Инициализация объектов
downloader = VideoDownloader()
db = Database()

# Регулярное выражение для проверки URL
URL_PATTERN = re.compile(r'https?://\S+')

# Регулярное выражение для определения плейлистов YouTube
PLAYLIST_PATTERN = re.compile(r'[?&]list=([a-zA-Z0-9_-]+)')

# Константа для ключа контекста чата
CHAT_CONTEXT_KEY = 'video_requests'

# --- Добавлено: Ключ для плейлистов --- 
PLAYLIST_CONTEXT_KEY = 'playlist_requests'
# --- Конец добавления ---

# --- Добавлено: Функция нормализации URL ---
def normalize_url(url):
    """Removes query parameters and fragments from a URL."""
    if not url: return None
    try:
        parsed = urlparse(url)
        # Reconstruct URL without query and fragment
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
    except Exception as e:
        logger.warning(f"Не удалось нормализовать URL '{url}': {e}")
        return url # Возвращаем исходный URL в случае ошибки
# --- Конец добавления ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(get_message('start'))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    await update.message.reply_text(get_message('help'))

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /settings"""
    keyboard = [
        [
            InlineKeyboardButton("Низкое качество", callback_data="quality_low"),
            InlineKeyboardButton("Среднее качество", callback_data="quality_medium")
        ],
        [
            InlineKeyboardButton("Высокое качество", callback_data="quality_high"),
            InlineKeyboardButton("Только аудио", callback_data="quality_audio")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        get_message('settings'),
        reply_markup=reply_markup
    )

async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на кнопки в настройках"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user_id = update.effective_user.id
    
    if data.startswith("quality_"):
        quality = data.split("_")[1]
        db.update_user_settings(user_id, quality)
        
        quality_names = {
            "low": "низкое",
            "medium": "среднее",
            "high": "высокое",
            "audio": "только аудио"
        }
        
        await query.edit_message_text(
            text=get_message('settings_saved', quality=quality_names.get(quality, quality))
        )

async def update_progress_message(context, chat_id, message_id, url):
    """Обновляет сообщение с прогрессом загрузки"""
    is_downloading = True
    last_percent = -1
    
    while is_downloading:
        try:
            logger.debug(f"Запрос прогресса для URL: {url}") # Лог перед вызовом
            progress_info = downloader.get_download_progress(url)
            logger.debug(f"Получена информация о прогрессе для URL {url}: {progress_info}") # Лог после вызова

            if not progress_info or progress_info.get('percent') >= 100:
                logger.info(f"Завершение цикла обновления прогресса для URL {url}. progress_info: {progress_info}")
                is_downloading = False
                break
            
            percent = progress_info.get('percent_rounded', 0)
            logger.debug(f"Прогресс для обновления сообщения {message_id}: {percent}% (из {progress_info.get('percent')})") # Лог перед обновлением сообщения
            
            if percent != last_percent:
                try:
                    # --- Добавлено: Создание статус-бара --- 
                    bar_length = 10 # Длина статус-бара
                    filled_length = int(bar_length * percent // 100)
                    bar = '█' * filled_length + '░' * (bar_length - filled_length)
                    # --- Конец добавления ---
                    
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        # Используем новый ключ локализации со статус-баром
                        text=get_message('progress_bar', bar=bar, progress=percent)
                    )
                    last_percent = percent
                except Exception as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка при обновлении прогресса: {e}")
            
            await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"Ошибка в цикле обновления прогресса: {e}")
            await asyncio.sleep(5)

async def handle_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик для URL адресов (теперь диспетчер)"""
    message = update.message
    message_text = message.text
    
    # --- Изменено: Проверка на URL и плейлист --- 
    url_match = URL_PATTERN.search(message_text)
    if not url_match:
        return
    url = url_match.group(0)
    playlist_match = PLAYLIST_PATTERN.search(url)
    # Считаем плейлистом, если есть list= и это не ссылка на видео в плейлисте
    is_playlist = bool(playlist_match) and "/watch?" not in url 
    # --- Конец изменений --- 
    
    user_id = update.effective_user.id
    username = update.effective_user.username
    
    # --- Проверка лимита перенесена сюда, до начала обработки --- 
    if not db.check_download_limit(user_id):
        await message.reply_text(
            get_message('limit_reached', limit=config.MAX_DOWNLOADS_PER_USER)
        )
        return
    # --- Конец переноса --- 

    # Обновление статистики происходит только при начале реальной загрузки
    # db.update_user_stats(user_id, username) # Перенесено
    
    # --- Изменено: Разделение логики для видео и плейлиста --- 
    if is_playlist:
        await handle_playlist_url(update, context, url)
    else:
        # Вызываем обработчик одиночного видео
        await handle_single_video_url(update, context, url)
    # --- Конец изменений ---

# --- Функция для обработки URL ОДИНОЧНОГО ВИДЕО (логика из старой handle_url) --- 
async def handle_single_video_url(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str):
    """Обработчик для URL одиночного видео."""
    message = update.message
    user_id = update.effective_user.id # Получаем user_id

    # Сообщение о начале обработки
    progress_message = await message.reply_text(get_message('processing_link'))
    message_id = progress_message.message_id
    
    try:
        # Получаем информацию о видео и предлагаем форматы
        video_info = await downloader.get_video_info(url)
        formats = video_info.get('formats', [])
        
        if formats:
            video_formats = [f for f in formats if f.get('vcodec') != 'none' and f.get('height', 0) > 0]
            if not video_formats:
                 logger.info(f"Видеоформаты не найдены для {url}, предлагаем только аудио/авто.")
                 keyboard = [
                     [InlineKeyboardButton(get_message('quality_audio'), callback_data="format_audio")],
                     [InlineKeyboardButton(get_message('quality_auto_button'), callback_data="format_auto")]
                 ]
            else:
                video_formats.sort(key=lambda x: int(x.get('height', 0) or 0), reverse=True)
                grouped_formats = {}
                for f in video_formats:
                    height = f.get('height')
                    if height and height not in grouped_formats:
                         grouped_formats[height] = f
                
                keyboard = []
                format_list_texts = []
                for height, f in sorted(grouped_formats.items(), reverse=True):
                    format_id = f.get('format_id')
                    if not format_id:
                        continue
                    
                    quality_category = 'high'
                    if height <= 480:
                        quality_category = 'medium'
                        
                    size_mb = "?"
                    if f.get('filesize'):
                        size_mb = f"{round(f['filesize'] / (1024 * 1024), 1)} MB"
                    elif f.get('tbr'):
                        duration = video_info.get('duration', 0)
                        if duration and duration > 0:
                            try:
                                 estimated_size = f['tbr'] * 1000 * duration / 8 / (1024 * 1024)
                                 size_mb = f"~{round(estimated_size, 1)} MB"
                            except TypeError:
                                logger.warning(f"Ошибка при расчете размера для формата {format_id}")
                    
                    format_button_text = get_message('quality_format',
                        resolution=f"{height}p", size=size_mb, fps=f.get('fps', '?'))
                    format_list_texts.append(f"- {format_button_text}")
                    
                    button_callback_data = f"format_{quality_category}"
                    keyboard.append([InlineKeyboardButton(format_button_text, callback_data=button_callback_data)])
            
                keyboard.append([InlineKeyboardButton(get_message('quality_audio'), callback_data="format_audio")])
                keyboard.append([InlineKeyboardButton(get_message('quality_auto_button'), callback_data="format_auto")])

            reply_markup = InlineKeyboardMarkup(keyboard)

            if CHAT_CONTEXT_KEY not in context.chat_data:
                context.chat_data[CHAT_CONTEXT_KEY] = {}
            context.chat_data[CHAT_CONTEXT_KEY][message_id] = {'url': url}
            logger.debug(f"Сохранен URL видео '{url}' для message_id {message_id}")

            await progress_message.edit_text(
                get_message('quality_selection', formats="\n".join(format_list_texts)),
                reply_markup=reply_markup
            )
        else:
            logger.info(f"Форматы не найдены для URL {url}, запускаем скачивание в auto.")
            db.update_user_stats(user_id, update.effective_user.username)
            await download_with_quality(update, context, url, "auto", progress_message)

    except (DownloadError, ExtractorError) as ytdlp_err:
        error_message = str(ytdlp_err)
        user_message_key = 'download_error'
        if "channel does not have a" in error_message or "/channel/" in url or "/user/" in url or "/c/" in url or "/@" in url.split('/')[-1]:
             user_message_key = 'error_channel_link'
        elif "This playlist does not exist" in error_message or ("list=" in url and "/playlist?list=" not in url):
             user_message_key = 'error_playlist_link' 
        elif "Video unavailable" in error_message:
             user_message_key = 'error_video_unavailable'
        elif "Private video" in error_message:
             user_message_key = 'error_video_private'
        
        logger.error(f"Ошибка yt-dlp при обработке URL видео '{url}': {error_message}")
        if CHAT_CONTEXT_KEY in context.chat_data and message_id in context.chat_data[CHAT_CONTEXT_KEY]:
            del context.chat_data[CHAT_CONTEXT_KEY][message_id]
        try:
            await progress_message.edit_text(get_message(user_message_key))
        except Exception as edit_err:
             logger.error(f"Не удалось отправить сообщение об ошибке yt-dlp для видео: {edit_err}")
    
    except Exception as e:
        logger.exception(f"Неожиданная ошибка при обработке URL видео '{url}': {e}")
        if CHAT_CONTEXT_KEY in context.chat_data and message_id in context.chat_data[CHAT_CONTEXT_KEY]:
            del context.chat_data[CHAT_CONTEXT_KEY][message_id]
        try:
            if progress_message:
                await progress_message.edit_text(get_message('download_error'))
            else:
                 await message.reply_text(get_message('download_error'))
        except Exception as edit_err:
             logger.error(f"Не удалось отправить сообщение об общей ошибке для видео: {edit_err}")

# --- Новая функция для обработки URL ПЛЕЙЛИСТА --- 
async def handle_playlist_url(update: Update, context: ContextTypes.DEFAULT_TYPE, playlist_url: str):
    """Обработчик для URL плейлиста."""
    message = update.message
    user_id = update.effective_user.id
    
    status_message = await message.reply_text(get_message('playlist_fetching_info'))
    message_id = status_message.message_id

    try:
        playlist_info = await downloader.get_playlist_info(playlist_url)
        playlist_title = playlist_info.get('title', 'Плейлист')
        video_entries = playlist_info.get('entries', [])
        video_count = len(video_entries)

        if video_count == 0:
            await status_message.edit_text(get_message('playlist_empty'))
            return
        
        MAX_PLAYLIST_ITEMS = 50
        if video_count > MAX_PLAYLIST_ITEMS:
             await status_message.edit_text(get_message('playlist_too_long', count=video_count, limit=MAX_PLAYLIST_ITEMS))
             return

        video_urls = [entry['url'] for entry in video_entries]

        if PLAYLIST_CONTEXT_KEY not in context.chat_data:
            context.chat_data[PLAYLIST_CONTEXT_KEY] = {}
        context.chat_data[PLAYLIST_CONTEXT_KEY][message_id] = {
            'playlist_url': playlist_url, 
            'video_urls': video_urls
        }
        logger.debug(f"Сохранены данные плейлиста '{playlist_title}' ({video_count} видео) для message_id {message_id}")
        
        keyboard = [
            [InlineKeyboardButton(get_message('playlist_confirm_button', count=video_count), callback_data=f"pl_confirm_{message_id}")],
            [InlineKeyboardButton(get_message('playlist_cancel_button'), callback_data=f"pl_cancel_{message_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await status_message.edit_text(
            get_message('playlist_confirm_prompt', title=playlist_title, count=video_count),
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )

    except ValueError as ve:
         logger.warning(f"Ошибка получения информации о плейлисте {playlist_url}: {ve}")
         await status_message.edit_text(get_message('playlist_not_found'))
    except (DownloadError, ExtractorError) as ytdlp_err:
         logger.error(f"Ошибка yt-dlp при получении инфо плейлиста '{playlist_url}': {ytdlp_err}")
         await status_message.edit_text(get_message('playlist_fetch_error'))
    except Exception as e:
        logger.exception(f"Неожиданная ошибка при обработке плейлиста '{playlist_url}': {e}")
        await status_message.edit_text(get_message('download_error'))
        if PLAYLIST_CONTEXT_KEY in context.chat_data and message_id in context.chat_data[PLAYLIST_CONTEXT_KEY]:
            del context.chat_data[PLAYLIST_CONTEXT_KEY][message_id]

# --- Конец новой функции --- 

async def _initialize_download(context: ContextTypes.DEFAULT_TYPE, url: str, format_id: str, user_id: int, chat_id: int, message_id: int | None):
    """Инициализирует загрузку: получает инфо, определяет качество, обновляет сообщение (если message_id есть), инициализирует словари."""
    start_time = time.time()
    actual_format_id = format_id
    quality_name = format_id
    canonical_url = url 
    progress_message = None

    try:
        video_info = await downloader.get_video_info(url)
        canonical_url = video_info.get('webpage_url', url)
    except Exception as info_err:
        logger.warning(f"Не удалось получить video_info для '{url}' в _initialize_download: {info_err}. Используем исходный URL как канонический.")

    if format_id == "auto":
        quality_label = await downloader.get_optimal_quality(url, user_id)
        quality_name = {
            "low": get_message('quality_low'),
            "medium": get_message('quality_medium'),
            "high": get_message('quality_high'),
        }.get(quality_label, quality_label)
        actual_format_id = quality_label
    elif format_id in ["low", "medium", "high", "audio"]:
        quality_name = {
            "low": get_message('quality_low'),
            "medium": get_message('quality_medium'),
            "high": get_message('quality_high'),
            "audio": get_message('quality_audio')
        }.get(format_id, format_id)
    elif format_id.isdigit():
        quality_name = get_message('quality_numeric_format', format_id=format_id)

    if message_id:
        text_to_set = get_message('quality_selected', quality=quality_name)
        try:
            progress_message = await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text_to_set,
                reply_markup=None
            )
            logger.debug(f"Обновлено сообщение {message_id} с качеством ('{quality_name}').")
        except Exception as edit_err:
            logger.error(f"Не удалось обновить сообщение {message_id} с выбранным качеством в _initialize_download: {edit_err}")
            progress_message = None

    with data_lock:
        # --- Изменено: Используем нормализованный канонический URL как ключ карты ---
        normalized_canonical = normalize_url(canonical_url)
        if normalized_canonical: # Только если нормализация успешна
            active_downloads[url] = {
                'percent': 0, 'percent_rounded': 0, 'downloaded_bytes': 0,
                'speed': 0, 'eta': 0, 'filename': None,
                'chat_id': chat_id if message_id else None,
                'message_id': message_id,
                'canonical_url': canonical_url, # Сохраняем оригинальный канонический для возможной очистки
                'process': None
            }
            canonical_url_map[normalized_canonical] = url # Карта: Нормализованный канонический -> Оригинальный
            logger.debug(f"Initialized active_downloads for {url}. Map: {normalized_canonical} -> {url}")
        else:
             logger.error(f"Не удалось нормализовать канонический URL '{canonical_url}', инициализация для '{url}' пропущена.")
        # --- Конец изменений ---
        
    return actual_format_id, canonical_url, start_time, progress_message

async def _run_actual_download(context: ContextTypes.DEFAULT_TYPE, url: str, actual_format_id: str, user_id: int, chat_id: int | None, message_id: int | None):
    """Запускает фоновую задачу обновления прогресса (если нужно) и саму загрузку."""
    progress_task = None
    if chat_id and message_id:
        progress_task = context.application.create_task(
            update_progress_message(context, chat_id, message_id, url)
        )
    
    try:
        result = await downloader.download_video(url, actual_format_id, user_id, chat_id, message_id)
        return result, progress_task
    except Exception:
         if progress_task and not progress_task.done():
              progress_task.cancel()
         raise

async def _send_video_result(context: ContextTypes.DEFAULT_TYPE, result: dict, chat_id: int, message_id: int | None, progress_message: Update | None):
    """Обрабатывает результат скачивания, отправляет видео (возможно, по частям)."""
    video_parts = []
    file_path = result['file_path']
    file_size = result['size']
    title = result['title']

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл не найден после скачивания: {file_path}")
    if file_size == 0:
        raise ValueError(f"Файл после скачивания пустой: {file_path}")

    try:
        if file_size > config.MAX_TELEGRAM_SIZE:
            # --- Изменено: Предлагаем выбор между разделением и прямой ссылкой, если включено ---
            if config.DIRECT_LINK_ENABLED:
                # Создаем уникальный идентификатор для файла
                file_id = hashlib.md5(file_path.encode()).hexdigest()[:12]
                
                # Сохраняем путь к файлу в контексте
                if 'large_files' not in context.bot_data:
                    context.bot_data['large_files'] = {}
                    
                context.bot_data['large_files'][file_id] = {
                    'file_path': file_path,
                    'title': title,
                    'size': file_size
                }
                
                # Создаем клавиатуру для выбора с коротким идентификатором
                keyboard = [
                    [
                        InlineKeyboardButton(get_message('split_video_button'), callback_data=f"split_{file_id}"),
                        InlineKeyboardButton(get_message('direct_link_button'), callback_data=f"link_{file_id}")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                # Отправляем сообщение с выбором
                size_mb = round(file_size / (1024 * 1024), 1)
                if progress_message and message_id:
                    try:
                        await progress_message.edit_text(
                            get_message('large_file_options', title=title, size=size_mb),
                            reply_markup=reply_markup,
                            parse_mode='HTML'
                        )
                    except Exception as edit_err:
                        logger.warning(f"Не удалось обновить сообщение {message_id} для выбора метода получения: {edit_err}")
                else:
                    await context.bot.send_message(
                        chat_id=chat_id, 
                        text=get_message('large_file_options', title=title, size=size_mb),
                        reply_markup=reply_markup,
                        parse_mode='HTML'
                    )
                    
                # Файл будет обработан позже после выбора пользователя
                return
            # --- Конец изменения ---
            
            # Если прямые ссылки не включены, используем старый метод разделения
            if progress_message and message_id:
                try:
                    await progress_message.edit_text(get_message('split_video_started_no_progress', title=title))
                except Exception as edit_err:
                     logger.warning(f"Не удалось обновить сообщение {message_id} о начале разделения: {edit_err}")
            else: 
                 await context.bot.send_message(chat_id=chat_id, text=get_message('split_video_started_no_progress', title=title))
                 
            video_parts = await downloader.split_large_video(file_path)
            if not video_parts:
                raise ValueError(f"Не удалось разделить видео: {file_path}")

            total_parts = len(video_parts)
            for i, part_path in enumerate(video_parts, 1):
                logger.info(f"Отправка части {i}/{total_parts}: {part_path}")
                with open(part_path, 'rb') as part_file:
                    await context.bot.send_video(
                        chat_id=chat_id,
                        video=part_file,
                        caption=get_message('split_video_part', part=i, total=total_parts, title=title),
                        supports_streaming=True,
                        read_timeout=120, write_timeout=120, connect_timeout=60, pool_timeout=120
                    )
            if progress_message and message_id:
                 try:
                     await progress_message.edit_text(get_message('split_video_completed'))
                 except Exception as edit_err:
                     logger.warning(f"Не удалось обновить сообщение {message_id} о завершении разделения: {edit_err}")
        else:
            logger.info(f"Отправка целого файла: {file_path}")
            with open(file_path, 'rb') as video_file:
                await context.bot.send_video(
                    chat_id=chat_id,
                    video=video_file,
                    caption=f"🎥 {title}",
                    supports_streaming=True,
                    read_timeout=120, write_timeout=120, connect_timeout=60, pool_timeout=120
                )
            if progress_message and message_id:
                try:
                    await progress_message.delete()
                except Exception as del_err:
                    logger.warning(f"Не удалось удалить сообщение о прогрессе {message_id}: {del_err}")

    finally:
        for part_f in video_parts:
            try:
                if os.path.exists(part_f):
                    os.remove(part_f)
                    logger.info(f"Удалена часть видео: {part_f}")
            except OSError as rm_err:
                 logger.warning(f"Не удалось удалить часть видео {part_f}: {rm_err}")
        
        if not video_parts and not config.CACHE_ENABLED:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Удален исходный файл (кэш отключен): {file_path}")
            except OSError as rm_err:
                logger.warning(f"Не удалось удалить исходный файл {file_path}: {rm_err}")

def _cleanup_download_state(url: str, canonical_url: str | None, progress_task):
    """Отменяет задачу прогресса и очищает словари (используя нормализованный ключ карты)."""
    if progress_task and not progress_task.done():
        progress_task.cancel()
        logger.debug(f"Задача обновления прогресса для URL '{url}' отменена в _cleanup_download_state.")
    
    with data_lock:
        download_info = active_downloads.pop(url, None) # Удаляем из active_downloads по оригинальному URL
        if download_info:
            logger.debug(f"Очистка active_downloads для {url} в _cleanup_download_state.")
            # --- Изменено: Удаляем из карты по нормализованному каноническому URL ---
            stored_canonical_url = download_info.get('canonical_url') # Получаем сохраненный канонический URL
            if stored_canonical_url:
                normalized_canonical_to_remove = normalize_url(stored_canonical_url)
                if normalized_canonical_to_remove and normalized_canonical_to_remove in canonical_url_map:
                    # Доп. проверка: убедимся, что значение в карте соответствует удаляемому original_url
                    if canonical_url_map[normalized_canonical_to_remove] == url:
                        del canonical_url_map[normalized_canonical_to_remove]
                        logger.debug(f"Removed mapping for normalized {normalized_canonical_to_remove}. Current map keys: {list(canonical_url_map.keys())}")
                    else:
                        # Этого не должно происходить, но логируем на всякий случай
                        logger.warning(f"Map value mismatch during cleanup for normalized key {normalized_canonical_to_remove}. Expected value '{url}', found '{canonical_url_map[normalized_canonical_to_remove]}'. Map not modified.")
            # --- Конец изменений ---
        elif url in canonical_url_map:
             # Попытка очистить карту, даже если active_downloads уже удален
             logger.warning(f"active_downloads для '{url}' не найден, но пытаемся очистить карту.")
             normalized_original = normalize_url(url)
             found_key_to_remove = None
             for k, v in canonical_url_map.items():
                 if v == url: # Ищем ключ, значение которого равно нашему original_url
                     found_key_to_remove = k
                     break
             if found_key_to_remove:
                 del canonical_url_map[found_key_to_remove]
                 logger.debug(f"Removed mapping with value '{url}' (key: {found_key_to_remove}) during fallback cleanup.")

# --- Основная функция-оркестратор для одиночного скачивания --- 
async def download_with_quality(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str, format_id: str, progress_message: Update):
    """Скачивание ОДИНОЧНОГО видео с выбранным качеством (оркестратор)"""
    user_id = update.effective_user.id
    chat_id = progress_message.chat_id
    message_id = progress_message.message_id 
    
    result = None
    progress_task = None
    canonical_url = None
    start_time = None

    try:
        db.update_user_stats(user_id, update.effective_user.username)
        
        actual_format_id, canonical_url, start_time, current_progress_message = await _initialize_download(
            context, url, format_id, user_id, chat_id, message_id
        )
        if current_progress_message: 
             progress_message = current_progress_message

        result, progress_task = await _run_actual_download(
            context, url, actual_format_id, user_id, chat_id, message_id
        )

        if result:
            download_duration = round(time.time() - start_time, 1)
            await send_notification(
                context, user_id, "download_complete",
                title=result['title'],
                quality=get_message(f'quality_{actual_format_id}') if actual_format_id in ['low', 'medium', 'high', 'audio'] else actual_format_id,
                size=round(result['size'] / (1024 * 1024), 1),
                time=download_duration
            )
            await _send_video_result(context, result, chat_id, message_id, progress_message)

    except Exception as e:
        logger.exception(f"Ошибка при обработке запроса на скачивание для URL '{url}': {e}")
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, 
                message_id=message_id, 
                text=get_message('download_error')
            )
        except Exception as edit_err:
            logger.error(f"Не удалось отредактировать сообщение {message_id} об ошибке скачивания: {edit_err}")
        await send_notification(
            context, user_id, "download_error",
            title=url, error=str(e)
        )

    finally:
        # Передаем оригинальный url и оригинальный canonical_url для очистки
        _cleanup_download_state(url, canonical_url, progress_task)

# --- Новая функция-воркер для скачивания видео из плейлиста --- 
async def _download_playlist_video(context: ContextTypes.DEFAULT_TYPE, video_url: str, user_id: int, chat_id: int, quality: str, semaphore: asyncio.Semaphore):
    """Скачивает и отправляет одно видео из плейлиста, управляя семафором."""
    async with semaphore:
        logger.info(f"(Плейлист) Начинаем обработку видео {video_url} в чате {chat_id}")
        result = None
        canonical_url = None
        start_time = None
        progress_task_placeholder = None 
        
        try:
            actual_format_id, canonical_url, start_time, _ = await _initialize_download(
                context, video_url, quality, user_id, chat_id, None 
            )

            cached_video = db.get_cached_video(video_url, actual_format_id)
            if cached_video:
                logger.info(f"(Плейлист) Видео {video_url} найдено в кэше: {cached_video['file_path']}")
                if os.path.exists(cached_video['file_path']):
                    if user_id:
                        db.log_download(user_id, video_url, "success_cache_playlist")
                    await _send_video_result(context, cached_video, chat_id, None, None) 
                    return 
                else:
                    db.remove_from_cache(video_url)

            result, _ = await _run_actual_download(
                context, video_url, actual_format_id, user_id, None, None 
            )

            if result:
                if start_time:
                    download_duration = round(time.time() - start_time, 1)
                    logger.info(f"(Плейлист) Видео '{result['title']}' скачано за {download_duration} сек.")
                await _send_video_result(context, result, chat_id, None, None) 

        except Exception as e:
            logger.error(f"(Плейлист) Ошибка при обработке видео {video_url}: {e}")
            await send_notification(
                context, user_id, "download_error",
                title=video_url, error=str(e)
            )
        finally:
            # Передаем оригинальный video_url и полученный canonical_url для очистки
            _cleanup_download_state(video_url, canonical_url, progress_task_placeholder)

# --- Конец функции-воркера ---

# --- Новая функция-обработчик подтверждения плейлиста --- 
async def playlist_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает подтверждение скачивания плейлиста."""
    query = update.callback_query
    message = query.message
    try:
         original_message_id = int(query.data.split('_')[-1])
    except (IndexError, ValueError):
         logger.error(f"Не удалось извлечь message_id из callback_data: {query.data}")
         await query.answer("Произошла ошибка.")
         return
         
    chat_id = message.chat_id
    user_id = query.from_user.id
    
    try:
         await query.answer()
    except BadRequest as e:
         if "Query is too old" in str(e) or "query id is invalid" in str(e):
             logger.warning(f"Callback query для подтверждения плейлиста устарел: {e}")
             try:
                 await context.bot.edit_message_text(chat_id=chat_id, message_id=original_message_id, text=get_message('error_callback_too_old'))
             except Exception:
                 pass 
             return
         else:
             logger.error(f"Ошибка BadRequest при ответе на callback подтверждения плейлиста: {e}")
             return
    except Exception as e:
         logger.error(f"Неожиданная ошибка при ответе на callback подтверждения плейлиста: {e}")
         return
    
    playlist_data = None
    if PLAYLIST_CONTEXT_KEY in context.chat_data and original_message_id in context.chat_data[PLAYLIST_CONTEXT_KEY]:
        playlist_data = context.chat_data[PLAYLIST_CONTEXT_KEY].pop(original_message_id) 
        if not context.chat_data[PLAYLIST_CONTEXT_KEY]:
             del context.chat_data[PLAYLIST_CONTEXT_KEY]
             
    if not playlist_data or 'video_urls' not in playlist_data:
        logger.warning(f"Не найдены данные плейлиста для original_message_id {original_message_id} в playlist_confirm_callback.")
        try:
             await query.edit_message_text(get_message('error_context_lost'))
        except Exception as e:
             logger.error(f"Не удалось обновить сообщение об утерянном контексте плейлиста: {e}")
        return
        
    video_urls = playlist_data['video_urls']
    video_count = len(video_urls)
    
    try:
        await query.edit_message_text(get_message('playlist_download_starting', count=video_count))
    except Exception as e:
         logger.warning(f"Не удалось обновить сообщение о начале загрузки плейлиста: {e}")

    user_quality = db.get_user_settings(user_id)
    if user_quality == 'auto':
         logger.info(f"(Плейлист) Качество пользователя 'auto', используем 'high'.")
         user_quality = 'high' 
    
    semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_DOWNLOADS)
    
    tasks = []
    started_count = 0
    for video_url in video_urls:
        if not db.check_download_limit(user_id):
            logger.warning(f"(Плейлист) Достигнут лимит для user {user_id}. Пропуск оставшихся {len(video_urls) - started_count} видео.")
            await context.bot.send_message(
                 chat_id=chat_id, 
                 text=get_message('limit_reached_playlist', limit=config.MAX_DOWNLOADS_PER_USER, started=started_count)
            )
            break 
        
        db.update_user_stats(user_id, update.effective_user.username)
        started_count += 1
        
        tasks.append(context.application.create_task(
            _download_playlist_video(context, video_url, user_id, chat_id, user_quality, semaphore)
        ))
        await asyncio.sleep(0.1)

    results = []
    if tasks:
         results = await asyncio.gather(*tasks, return_exceptions=True)
    
    errors_count = sum(1 for res in results if isinstance(res, Exception))

    await context.bot.send_message(
        chat_id=chat_id,
        text=get_message('playlist_download_finished', 
                         total=started_count, 
                         success=(started_count - errors_count),
                         errors=errors_count)
    )

# --- Новая функция-обработчик отмены плейлиста --- 
async def playlist_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает отмену скачивания плейлиста."""
    query = update.callback_query
    message = query.message
    try:
         original_message_id = int(query.data.split('_')[-1])
    except (IndexError, ValueError):
         logger.error(f"Не удалось извлечь message_id из callback_data отмены: {query.data}")
         await query.answer("Произошла ошибка.")
         return

    try:
        await query.answer()
    except BadRequest as e:
         if "Query is too old" in str(e) or "query id is invalid" in str(e):
             logger.warning(f"Callback query для отмены плейлиста устарел: {e}")
             try:
                 await context.bot.edit_message_text(chat_id=message.chat_id, message_id=original_message_id, text=get_message('error_callback_too_old'))
             except Exception:
                 pass
             return
         else:
             logger.error(f"Ошибка BadRequest при ответе на callback отмены плейлиста: {e}")
             return
    except Exception as e:
         logger.warning(f"Не удалось ответить на callback query отмены плейлиста: {e}")

    if PLAYLIST_CONTEXT_KEY in context.chat_data and original_message_id in context.chat_data[PLAYLIST_CONTEXT_KEY]:
        context.chat_data[PLAYLIST_CONTEXT_KEY].pop(original_message_id, None)
        if not context.chat_data[PLAYLIST_CONTEXT_KEY]:
             del context.chat_data[PLAYLIST_CONTEXT_KEY]
        logger.debug(f"Очищен контекст для отмененного плейлиста original_message_id {original_message_id}")

    try:
        await query.edit_message_text(get_message('playlist_cancelled'))
    except Exception as e:
        logger.error(f"Не удалось обновить сообщение об отмене плейлиста: {e}")

# --- Конец новой функции --- 

# --- Добавлено: Функция для отправки уведомлений ---
async def send_notification(context: ContextTypes.DEFAULT_TYPE, user_id: int, notification_type: str, **kwargs):
    """Отправляет уведомление пользователю, если оно включено в настройках."""
    try:
        settings = db.get_notification_settings(user_id)
        if settings.get(notification_type, False): # Проверяем, включен ли этот тип уведомлений
            message_key = f"{notification_type}_notification" # Формируем ключ для локализации
            message_text = get_message(message_key, **kwargs)
            if message_text != message_key: # Убедимся, что ключ найден
                await context.bot.send_message(chat_id=user_id, text=message_text)
                logger.debug(f"Отправлено уведомление '{notification_type}' пользователю {user_id}")
            else:
                logger.warning(f"Ключ локализации '{message_key}' не найден для уведомления.")
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления '{notification_type}' пользователю {user_id}: {e}")
# --- Конец добавления ---

# --- Восстановленные функции для уведомлений --- 
async def notifications_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /notifications"""
    user_id = update.effective_user.id
    settings = db.get_notification_settings(user_id)
    
    keyboard = []
    # Словарь с названиями уведомлений (можно вынести в локализацию)
    notification_names = {
        "download_complete": "Завершение загрузки",
        "download_error": "Ошибка загрузки",
        "download_progress": "Прогресс загрузки", # Уведомления о прогрессе пока не реализованы
        "system_alert": "Системные оповещения" # Системные уведомления пока не используются
    }
    for setting, enabled in settings.items():
        # Пропускаем неиспользуемые типы уведомлений в интерфейсе
        if setting not in notification_names: continue 
            
        status = "✅ Включено" if enabled else "❌ Выключено"
        button_text = f"{notification_names.get(setting, setting)}: {status}" 
        # Используем ключ из локализации для самой кнопки, если он есть, иначе - сформированный текст
        # button_text = get_message('notification_toggle', name=notification_names.get(setting, setting), status=status)
        keyboard.append([
            InlineKeyboardButton(button_text, callback_data=f"notify_{setting}")
        ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        get_message('notification_settings'),
        reply_markup=reply_markup
    )

async def notification_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на кнопки настроек уведомлений"""
    query = update.callback_query
    
    try:
        await query.answer()
    except BadRequest as e:
         if "Query is too old" in str(e) or "query id is invalid" in str(e):
             logger.warning(f"Callback query для уведомлений устарел: {e}")
             # Можно показать сообщение пользователю или просто проигнорировать
             # await query.message.reply_text(get_message('error_callback_too_old'))
             return
         else:
            logger.error(f"Ошибка BadRequest при ответе на callback уведомлений: {e}")
            return
    except Exception as e:
        logger.error(f"Неожиданная ошибка при ответе на callback уведомлений: {e}")
        return

    data = query.data
    if not data.startswith("notify_"):
        return
    
    notification_type = data.split("_", 1)[1]
    user_id = update.effective_user.id
    
    # Переключаем настройку в БД
    if db.toggle_notification(user_id, notification_type):
        # Обновляем клавиатуру
        settings = db.get_notification_settings(user_id)
        keyboard = []
        notification_names = {
             "download_complete": "Завершение загрузки",
             "download_error": "Ошибка загрузки",
             "download_progress": "Прогресс загрузки",
             "system_alert": "Системные оповещения"
        }
        for setting, enabled in settings.items():
            if setting not in notification_names: continue
            status = "✅ Включено" if enabled else "❌ Выключено"
            button_text = f"{notification_names.get(setting, setting)}: {status}"
            keyboard.append([
                InlineKeyboardButton(button_text, callback_data=f"notify_{setting}")
            ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            # Редактируем сообщение с настройками
            await query.edit_message_text(
                get_message('notification_settings'),
                reply_markup=reply_markup
            )
            # Отправляем подтверждение в чат (опционально)
            # await context.bot.send_message(chat_id=user_id, text=get_message('notification_updated'))
        except Exception as e:
            if "Message is not modified" not in str(e):
                 logger.error(f"Ошибка при обновлении настроек уведомлений: {e}")
    else:
         logger.error(f"Не удалось обновить настройку уведомления '{notification_type}' для пользователя {user_id}")
         # Можно отправить сообщение об ошибке пользователю
         # await query.message.reply_text("Не удалось сохранить настройку.")

# --- Конец восстановленных функций --- 

def check_ffmpeg():
    """Проверяет наличие ffmpeg в системе на разных платформах"""
    possible_paths = [
        "ffmpeg",
        "ffmpeg.exe",
        "/usr/bin/ffmpeg",
        "/usr/local/bin/ffmpeg",
        "C:\\Program Files\\ffmpeg\\bin\\ffmpeg.exe",
        "C:\\Users\\admin\\AppData\\Local\\Microsoft\\WinGet\\Links\\ffmpeg.exe",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffmpeg.exe")
    ]
    
    import platform
    system = platform.system().lower()
    
    if system == "windows":
        program_files = os.environ.get("ProgramFiles", "C:\\Program Files")
        program_files_x86 = os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")
        possible_paths.extend([
            os.path.join(program_files, "ffmpeg", "bin", "ffmpeg.exe"),
            os.path.join(program_files_x86, "ffmpeg", "bin", "ffmpeg.exe")
        ])
    
    for path in possible_paths:
        try:
            if path != "ffmpeg" and path != "ffmpeg.exe" and not os.path.isfile(path):
                continue
                
            subprocess_args = {
                "args": [path, "-version"],
                "stdout": subprocess.PIPE,
                "stderr": subprocess.PIPE,
                "timeout": 5
            }
            
            if system == "windows":
                subprocess_args["creationflags"] = subprocess.CREATE_NO_WINDOW
                
            result = subprocess.run(**subprocess_args)
            
            if result.returncode == 0:
                logger.info(f"ffmpeg найден: {path}")
                return True
        except (subprocess.SubprocessError, FileNotFoundError, PermissionError, OSError):
            continue
        except Exception as e:
            logger.debug(f"Ошибка при проверке ffmpeg по пути {path}: {e}")
            continue
    
    return False

def main():
    """Запуск бота"""
    if not check_ffmpeg():
        logger.warning(
            "ВНИМАНИЕ: ffmpeg не найден. Функциональность скачивания видео может быть ограничена. "
            "Рекомендуется установить ffmpeg: https://ffmpeg.org/download.html"
        )
    
    # Асинхронное создание директории больше не нужно здесь, т.к. оно в download_video
    # if not os.path.exists(config.DOWNLOAD_DIR):
    #     os.makedirs(config.DOWNLOAD_DIR)
    
    application = Application.builder().token(config.TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CommandHandler("notifications", notifications_command))
    # --- Добавляем новый обработчик команды для статистики прямых ссылок ---
    if config.DIRECT_LINK_ENABLED:
        application.add_handler(CommandHandler("directlinks", directlinks_command))
    # --- Конец добавления ---
    
    application.add_handler(CallbackQueryHandler(settings_callback, pattern=r'^quality_'))
    application.add_handler(CallbackQueryHandler(format_callback, pattern=r'^format_'))
    application.add_handler(CallbackQueryHandler(notification_callback, pattern=r'^notify_'))
    application.add_handler(CallbackQueryHandler(playlist_confirm_callback, pattern=r'^pl_confirm_'))
    application.add_handler(CallbackQueryHandler(playlist_cancel_callback, pattern=r'^pl_cancel_'))
    # --- Регистрируем обработчик колбэков для больших файлов ---
    if config.DIRECT_LINK_ENABLED:
        application.add_handler(CallbackQueryHandler(large_file_callback, pattern=r'^(split|link)_'))
    # --- Конец добавления ---
    
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_url))
    
    # --- Добавляем планировщик задач для очистки устаревших ссылок ---
    if config.DIRECT_LINK_ENABLED:
        # Создаем планировщик для периодической очистки устаревших ссылок
        job_queue = application.job_queue
        job_queue.run_repeating(
            cleanup_expired_links, 
            interval=config.DIRECT_LINK_CLEANUP_INTERVAL,  # Интервал из конфига
            first=10  # Первый запуск через 10 секунд после старта бота
        )
        logger.info(f"Настроена периодическая очистка устаревших ссылок (интервал: {config.DIRECT_LINK_CLEANUP_INTERVAL} сек)")
    # --- Конец добавления ---
    
    application.run_polling()

# --- Восстановленная функция format_callback --- 
async def format_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора формата/категории для одиночного видео."""
    query = update.callback_query
    message = query.message
    message_id = message.message_id
    chat_id = message.chat_id
    data = query.data

    try:
        await query.answer()
    except BadRequest as e:
         if "Query is too old" in str(e) or "query id is invalid" in str(e):
             logger.warning(f"Callback query для выбора формата устарел: {e}")
             try:
                 await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=get_message('error_callback_too_old'))
             except Exception:
                 pass
             return
         else:
            logger.error(f"Ошибка BadRequest при ответе на callback выбора формата: {e}")
            return
    except Exception as e:
        logger.error(f"Неожиданная ошибка при ответе на callback выбора формата: {e}")
        return

    if not data.startswith("format_"):
        logger.warning(f"Некорректные данные в format_callback: {data}")
        return

    # Извлекаем ID/категорию формата
    format_id = data.split("_", 1)[1] # format_low, format_medium, format_auto, format_audio и т.д.

    # Получаем URL из контекста чата для ОДИНОЧНЫХ видео
    url = None
    if CHAT_CONTEXT_KEY in context.chat_data and message_id in context.chat_data[CHAT_CONTEXT_KEY]:
        url_data = context.chat_data[CHAT_CONTEXT_KEY].pop(message_id, None) # Извлекаем и удаляем
        if url_data:
            url = url_data.get('url')
        # Очищаем ключ, если он пуст
        if not context.chat_data[CHAT_CONTEXT_KEY]:
            del context.chat_data[CHAT_CONTEXT_KEY]

    if not url:
        logger.error(f"Не найден URL в chat_data для message_id {message_id} в format_callback. Невозможно продолжить.")
        try:
            await query.edit_message_text(get_message('error_context_lost'))
        except Exception as e:
             logger.error(f"Не удалось отредактировать сообщение об утерянном контексте видео: {e}")
        return

    # Обновляем сообщение перед началом скачивания (опционально, т.к. _initialize_download тоже это делает)
    # try:
    #     await message.edit_text(get_message('download_started'))
    # except Exception as e:
    #     logger.warning(f"Не удалось отредактировать сообщение на 'download_started' в format_callback: {e}")

    # Обновляем статистику перед началом загрузки
    db.update_user_stats(update.effective_user.id, update.effective_user.username)
    
    # Запускаем скачивание одиночного видео
    await download_with_quality(update, context, url, format_id, message)
# --- Конец восстановленной функции --- 

# --- Новая функция обработки колбэков для выбора способа получения больших файлов ---
async def large_file_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор пользователя при работе с большими файлами (разделение или прямая ссылка)"""
    query = update.callback_query
    data = query.data
    action, file_id = data.split("_", 1)  # Формат: "split_file_id" или "link_file_id"
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e) or "query id is invalid" in str(e):
            logger.warning(f"Callback query для большого файла устарел: {e}")
            try:
                await context.bot.edit_message_text(chat_id=chat_id, message_id=query.message.message_id, text=get_message('error_callback_too_old'))
            except Exception:
                pass 
            return
        else:
            logger.error(f"Ошибка BadRequest при ответе на callback: {e}")
            return
    except Exception as e:
        logger.error(f"Неожиданная ошибка при ответе на callback: {e}")
        return
    
    # Проверяем, что информация о файле сохранена в контексте
    if 'large_files' not in context.bot_data or file_id not in context.bot_data['large_files']:
        logger.warning(f"Не найдены данные большого файла для {file_id}")
        try:
            await query.edit_message_text(get_message('error_context_lost'))
        except Exception as e:
            logger.error(f"Не удалось обновить сообщение об утерянном контексте: {e}")
        return
    
    file_info = context.bot_data['large_files'][file_id]
    file_path = file_info['file_path']
    title = file_info['title']
    file_size = file_info['size']
    
    if action == "split":
        # Разделяем видео на части
        await query.edit_message_text(get_message('split_video_started'))
        
        try:
            video_parts = await downloader.split_large_video(file_path)
            if not video_parts:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=get_message('download_error')
                )
                return
            
            total_parts = len(video_parts)
            for i, part_path in enumerate(video_parts, 1):
                logger.info(f"Отправка части {i}/{total_parts}: {part_path}")
                with open(part_path, 'rb') as part_file:
                    await context.bot.send_video(
                        chat_id=chat_id,
                        video=part_file,
                        caption=get_message('split_video_part', part=i, total=total_parts, title=title),
                        supports_streaming=True,
                        read_timeout=120, write_timeout=120, connect_timeout=60, pool_timeout=120
                    )
                    
                # Удаляем отправленную часть
                try:
                    if os.path.exists(part_path):
                        os.remove(part_path)
                        logger.debug(f"Удалена часть видео после отправки: {part_path}")
                except OSError as rm_err:
                    logger.warning(f"Не удалось удалить часть видео {part_path}: {rm_err}")
            
            await context.bot.send_message(
                chat_id=chat_id,
                text=get_message('split_video_completed')
            )
            
        except Exception as e:
            logger.error(f"Ошибка при разделении файла {file_path}: {e}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=get_message('download_error')
            )
            
    elif action == "link":
        # Генерируем прямую ссылку для скачивания
        await query.edit_message_text(get_message('direct_link_generating'))
        
        try:
            # Импортируем здесь для избежания циклических зависимостей
            from link_generator import LinkGenerator
            link_gen = LinkGenerator()
            
            # Генерируем ссылку
            link_info = await link_gen.generate_link(file_path, title)
            
            if not link_info:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=get_message('direct_link_error')
                )
                return
            
            # Форматируем дату истечения для локализации
            expires_str = link_info['expires'].strftime("%d.%m.%Y %H:%M")
            
            # Отправляем сообщение с прямой ссылкой
            await context.bot.send_message(
                chat_id=chat_id,
                text=get_message('direct_link_ready', 
                    title=title,
                    url=link_info['url'],
                    size=link_info['size_mb'],
                    expires=expires_str
                ),
                parse_mode='HTML',
                disable_web_page_preview=False
            )
            
            logger.info(f"Создана прямая ссылка для {title}: {link_info['url']}")
            
        except Exception as e:
            logger.error(f"Ошибка при создании прямой ссылки для {file_path}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            await context.bot.send_message(
                chat_id=chat_id,
                text=get_message('direct_link_error')
            )
    
    # Удаляем файл из контекста
    del context.bot_data['large_files'][file_id]
    if not context.bot_data['large_files']:
        del context.bot_data['large_files']

# --- Новая команда для статистики прямых ссылок ---
async def directlinks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для просмотра статистики прямых ссылок (только для админов)"""
    # Проверяем, является ли пользователь админом (примерно)
    user_id = update.effective_user.id
    is_admin = user_id in [123456789]  # Здесь нужно заменить на реальные ID админов
    
    if not is_admin:
        await update.message.reply_text("У вас нет прав для выполнения этой команды.")
        return
    
    from link_generator import LinkGenerator
    link_gen = LinkGenerator()
    stats = await link_gen.get_links_stats()
    
    await update.message.reply_text(
        get_message('direct_link_stats',
            active=stats['active_links'],
            size=stats['total_size_mb']
        )
    )

# --- Функция для периодической очистки устаревших ссылок ---
async def cleanup_expired_links(context: ContextTypes.DEFAULT_TYPE):
    """Периодически очищает устаревшие прямые ссылки"""
    try:
        from link_generator import LinkGenerator
        link_gen = LinkGenerator()
        count = await link_gen.cleanup_expired_links()
        
        if count > 0:
            logger.info(f"Удалено {count} устаревших файлов прямых ссылок")
    except Exception as e:
        logger.error(f"Ошибка при очистке устаревших ссылок: {e}")
# --- Конец новых функций ---

if __name__ == '__main__':
    main() 