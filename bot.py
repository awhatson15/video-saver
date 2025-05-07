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
import yt_dlp
import uuid

# Импортируем наши модули
import config
from downloader import VideoDownloader, data_lock, active_downloads, canonical_url_map
from database import Database
from localization import get_message  # Импортируем функцию локализации

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
        return urlunparse(
    (parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
    except Exception as e:
        logger.warning(f"Не удалось нормализовать URL '{url}': {e}")
        return url  # Возвращаем исходный URL в случае ошибки
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
            InlineKeyboardButton(
    "Низкое качество",
     callback_data="quality_low"),
            InlineKeyboardButton(
    "Среднее качество",
     callback_data="quality_medium")
        ],
        [
            InlineKeyboardButton(
    "Высокое качество",
     callback_data="quality_high"),
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
            text=get_message(
    'settings_saved',
    quality=quality_names.get(
        quality,
         quality))
        )


async def update_progress_message(chat_id, message_id, url):
    """Обновляет сообщение о прогрессе загрузки."""
    last_update = 0
    update_interval = 3  # Минимальный интервал между обновлениями в секундах

    while True:
        try:
            current_time = time.time()

            # Получаем информацию о прогрессе
            with data_lock:
                if url not in active_downloads:
                    logger.debug(f"URL {url} больше не активен")
                    return

                download_info = active_downloads[url]
                if download_info['cancelled']:
                    logger.info(f"Загрузка {url} была отменена")
                    return

                # Проверяем, нужно ли обновлять сообщение
                if current_time - \
                    download_info['last_update'] < update_interval:
                    await asyncio.sleep(0.5)
                    continue

                # Формируем текст сообщения
                status = download_info['status']
                if status == 'initializing':
                    text = get_message('download_initializing')
                else:
                    # Форматируем информацию о прогрессе
                    downloaded = format_size(download_info['downloaded_bytes'])
                    total = format_size(
    download_info['total_bytes'] or download_info['total_bytes_estimate'])
                    speed = format_size(download_info['speed']) + '/s'
                    eta = format_time(
    download_info['eta']) if download_info['eta'] else 'N/A'
                    percent = download_info['percent_rounded']

                    text = get_message(
                        'download_progress',
                        filename=download_info['filename'] or 'Видео',
                        downloaded=downloaded,
                        total=total,
                        speed=speed,
                        eta=eta,
                        percent=percent
                    )

                # Создаем клавиатуру с кнопкой отмены
                keyboard = [
                    [InlineKeyboardButton(
                        get_message('cancel_download_button'),
                        callback_data=f'cancel_download_{url}'
                    )]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=text,
                        reply_markup=reply_markup
                    )
                    download_info['last_update'] = current_time
                except telegram.error.BadRequest as e:
                    if "Message is not modified" in str(e):
                        pass
                    else:
                        logger.warning(f"Ошибка при обновлении сообщения: {e}")
                except Exception as e:
                    logger.error(
    f"Неожиданная ошибка при обновлении сообщения: {e}")

            await asyncio.sleep(1)

        except asyncio.CancelledError:
            logger.info(f"Задача обновления прогресса для {url} отменена")
            raise
        except Exception as e:
            logger.error(f"Ошибка в update_progress_message для {url}: {e}")
            await asyncio.sleep(5)  # Пауза перед следующей попыткой


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
    if not await user_can_download(user_id, update.effective_chat.id):
        logger.warning(f"Превышен лимит загрузок для пользователя {user_id}")
        await message.reply_text(get_message('download_limit_reached'))
        return

    # Обновляем статистику пользователя
    db.update_user_stats(user_id, username)

    # --- Изменено: Диспетчеризация на основе типа URL ---
    if is_playlist:
        # Обрабатываем плейлист
        await handle_playlist_url(update, context, url)
    else:
        # Обрабатываем одиночное видео
        await handle_single_video_url(update, context, url)
    # --- Конец изменений ---

# --- Функция для обработки URL ОДИНОЧНОГО ВИДЕО (логика из старой handle_url) ---


async def handle_single_video_url(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str):
    """Обрабатывает URL одиночного видео."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # Проверяем, может ли пользователь скачивать
    if not await user_can_download(user_id, chat_id):
        await update.message.reply_text(get_message('download_limit_exceeded'))
        return
    
    # Отправляем сообщение о начале обработки
    message = await update.message.reply_text(get_message('processing_video'))
    
    # Сохраняем URL в контексте чата
    if chat_id not in context.chat_data:
        context.chat_data[chat_id] = {}
    context.chat_data[chat_id][CHAT_CONTEXT_KEY] = url
    
    # Добавляем URL в активные загрузки
    with data_lock:
        active_downloads[url] = {
            'status': 'initializing',
            'percent': 0,
            'percent_rounded': 0,
            'downloaded_bytes': 0,
            'total_bytes': 0,
            'total_bytes_estimate': 0,
            'speed': 0,
            'eta': 0,
            'filename': '',
            'last_update': time.time(),
            'cancelled': False,
            'user_id': user_id,
            'chat_id': chat_id,
            'message_id': message.message_id
        }
    
    try:
        # Получаем информацию о видео
        info = await downloader.get_video_info(url)
        if not info:
            await message.edit_text(get_message('video_info_error'))
            return
        
        # Получаем доступные форматы
        formats = info.get('formats', [])
        if not formats:
            await message.edit_text(get_message('no_formats_available'))
            return
        
        # Логируем количество полученных форматов
        logger.info(f"Получено {len(formats)} форматов для видео {url}")
        
        # Логируем все форматы с их разрешениями для диагностики
        logger.info("Полный список форматов:")
        for idx, fmt in enumerate(formats):
            vcodec = fmt.get('vcodec', 'none')
            acodec = fmt.get('acodec', 'none')
            height = fmt.get('height', 0)
            width = fmt.get('width', 0)
            format_id = fmt.get('format_id', 'unknown')
            format_note = fmt.get('format_note', '')
            filesize = fmt.get('filesize', 0)
            
            logger.info(f"Формат #{idx}: id={format_id}, note={format_note}, vcodec={vcodec}, acodec={acodec}, resolution={width}x{height}, size={filesize}")
        
        # Разделяем форматы на видео и аудио
        video_formats = []
        audio_formats = []
        
        # Используем все доступные видео-форматы
        for fmt in formats:
            vcodec = fmt.get('vcodec', 'none')
            acodec = fmt.get('acodec', 'none')
            height = fmt.get('height', 0)
            format_id = fmt.get('format_id', '')
            
            # Отбираем видео форматы
            if vcodec != 'none' and height > 0:
                # Для YouTube: пропускаем форматы без аудио только если есть форматы с аудио
                if acodec == 'none' and 'youtube' in url.lower():
                    # Добавляем в видео-форматы только если в формате указано "видео"
                    if 'video only' in fmt.get('format', '').lower():
                        video_formats.append(fmt)
                else:
                    video_formats.append(fmt)
            
            # Отбираем аудио форматы
            elif vcodec == 'none' and acodec != 'none':
                audio_formats.append(fmt)
        
        # Если нет видео форматов, попробуем найти их другим способом
        if not video_formats:
            logger.warning(f"Не найдены видео форматы стандартным методом, пробуем другую фильтрацию")
            video_formats = [fmt for fmt in formats if fmt.get('height', 0) > 0]
        
        logger.info(f"После фильтрации: {len(video_formats)} видео форматов, {len(audio_formats)} аудио форматов")
        
        # Группируем форматы по разрешению
        format_groups = {}
        for fmt in video_formats:
            height = fmt.get('height', 0)
            if height > 0:
                if height not in format_groups:
                    format_groups[height] = []
                format_groups[height].append(fmt)
        
        # Сортируем разрешения по убыванию
        sorted_heights = sorted(format_groups.keys(), reverse=True)
        
        # Логируем найденные разрешения
        logger.info(f"Найдены разрешения: {sorted_heights}")
        
        # Создаем клавиатуру с кнопками выбора качества
        keyboard = []
        
        # Добавляем кнопки для каждого разрешения
        for height in sorted_heights:
            formats_for_height = format_groups[height]
            # Берем форматы с и без аудио
            formats_with_audio = [f for f in formats_for_height if f.get('acodec') != 'none']
            if formats_with_audio:
                # Предпочитаем форматы с аудио
                best_format = max(
                    formats_with_audio,
                    key=lambda x: x.get('filesize', 0) if x.get('filesize') else 0
                )
            else:
                # Если нет форматов с аудио, используем любой
                best_format = max(
                    formats_for_height,
                    key=lambda x: x.get('filesize', 0) if x.get('filesize') else 0
                )
            
            format_id = best_format.get('format_id')
            ext = best_format.get('ext', 'mp4')
            filesize = best_format.get('filesize', 0)
            filesize_str = format_size(filesize) if filesize else "N/A"
            
            # Создаем текст для кнопки
            button_text = f"{height}p ({filesize_str})"
            
            # Создаем callback_data с хешем URL
            url_hash = hashlib.md5(url.encode()).hexdigest()
            callback_data = f"download_{url_hash}_{format_id}_{user_id}"
            
            # Добавляем кнопку
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        # Добавляем кнопку для аудио
        if audio_formats:
            best_audio = max(audio_formats, key=lambda x: x.get('abr', 0) if x.get('abr') else 0)
            audio_format_id = best_audio.get('format_id')
            audio_ext = best_audio.get('ext', 'mp3')
            audio_bitrate = best_audio.get('abr', 0)
            audio_bitrate_str = f"{audio_bitrate}kbps" if audio_bitrate else "N/A"
            
            url_hash = hashlib.md5(url.encode()).hexdigest()
            keyboard.append([InlineKeyboardButton(f"🎵 Аудио ({audio_bitrate_str})", callback_data=f"download_{url_hash}_{audio_format_id}_{user_id}")])
        
        # Добавляем кнопку отмены
        url_hash = hashlib.md5(url.encode()).hexdigest()
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data=f"cancel_{url_hash}_{user_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Обновляем сообщение с кнопками выбора качества
        await message.edit_text(
            text=get_message('select_quality', title=info.get('title', 'Видео')),
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка при обработке URL: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await message.edit_text(get_message('video_info_error'))
        
        # Очищаем состояние загрузки
        _cleanup_download_state(url, None, None)

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
            [InlineKeyboardButton(
                get_message('playlist_confirm_button', count=video_count),
                callback_data=f"pl_confirm_{message_id}")],
            [InlineKeyboardButton(
                get_message('playlist_cancel_button'), 
                callback_data=f"pl_cancel_{message_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await status_message.edit_text(
            get_message(
    'playlist_confirm_prompt',
    title=playlist_title,
     count=video_count),
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


async def _initialize_download(context, url, chat_id, message_id, user_id, ydl_opts=None):
    """Инициализирует процесс загрузки видео."""
    bot = context.bot
    start_time = time.time()

    try:
        # Создаем или обновляем сообщение о загрузке
        progress_text = get_message('download_started')

        if message_id:
            try:
                progress_message = await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=progress_text
                )
            except Exception as e:
                logger.warning(
    f"Не удалось обновить сообщение {message_id}: {e}")
                progress_message = await bot.send_message(chat_id=chat_id, text=progress_text)
                message_id = progress_message.message_id
        else:
            progress_message = await bot.send_message(chat_id=chat_id, text=progress_text)
            message_id = progress_message.message_id

        # Запускаем процесс скачивания
        logger.info(f"Начало загрузки для URL: {url} с options: {ydl_opts}")

        # Добавляем URL в активные загрузки
        with data_lock:
            active_downloads[url] = {
                'status': 'initializing',
                'percent': 0,
                'percent_rounded': 0,
                'downloaded_bytes': 0,
                'total_bytes': 0,
                'total_bytes_estimate': 0,
                'speed': 0,
                'eta': 0,
                'filename': '',
                'last_update': time.time(),
                'cancelled': False,
                'user_id': user_id,
                'chat_id': chat_id,
                'message_id': message_id
            }

        # Запускаем задачу обновления сообщения о прогрессе
        progress_task = asyncio.create_task(
            update_progress_message(chat_id, message_id, url)
        )

        # Выполняем скачивание
        result = await asyncio.to_thread(
            download_video, url, ydl_opts, context, user_id
        )

        # Отправляем результат
        if result:
            # Останавливаем задачу обновления прогресса
            if progress_task:
                progress_task.cancel()
                try:
                    await progress_task
                except asyncio.CancelledError:
                    pass

            # Обновляем сообщение о завершении загрузки
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=get_message('download_complete_preparing')
                )
            except Exception as e:
                logger.warning(
    f"Не удалось обновить сообщение о завершении загрузки: {e}")

            # Отправляем видео или аудио
            file_path = result.get('filepath')
            thumbnail_path = result.get('thumbnail')
            duration = result.get('duration', 0)
            title = result.get('title', 'Видео')

            if file_path and os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                file_extension = os.path.splitext(file_path)[1].lower()

                try:
                    caption = get_message('download_complete',
                                        title=title,
                                        size=round(
                                            file_size / (1024 * 1024), 1),
                                        time=round(time.time() - start_time, 1))

                    if file_extension in ['.mp4', '.mkv', '.webm', '.avi']:
                        # Отправляем видео
                        await bot.send_video(
                            chat_id=chat_id,
                            video=open(file_path, 'rb'),
                            caption=caption,
                            duration=int(duration) if duration else None,
                            thumb=open(thumbnail_path, 'rb') if thumbnail_path and os.path.exists(
                                thumbnail_path) else None,
                            supports_streaming=True
                        )
                    elif file_extension in ['.mp3', '.m4a', '.ogg', '.opus']:
                        # Отправляем аудио
                        await bot.send_audio(
                            chat_id=chat_id,
                            audio=open(file_path, 'rb'),
                            caption=caption,
                            duration=int(duration) if duration else None,
                            thumb=open(thumbnail_path, 'rb') if thumbnail_path and os.path.exists(
                                thumbnail_path) else None,
                            title=title
                        )
                    else:
                        # Отправляем как документ
                        await bot.send_document(
                            chat_id=chat_id,
                            document=open(file_path, 'rb'),
                            caption=caption,
                            thumb=open(thumbnail_path, 'rb') if thumbnail_path and os.path.exists(
                                thumbnail_path) else None
                        )

                    # Удаляем файлы после отправки
                    try:
                        os.remove(file_path)
                        if thumbnail_path and os.path.exists(thumbnail_path):
                            os.remove(thumbnail_path)
                    except Exception as e:
                        logger.warning(f"Не удалось удалить файлы после отправки: {e}")
                        
                    # Удаляем сообщение о прогрессе
                    try:
                        await bot.delete_message(chat_id=chat_id, message_id=message_id)
                    except Exception as e:
                        logger.warning(f"Не удалось удалить сообщение о прогрессе: {e}")
                    
                except Exception as e:
                    logger.error(f"Ошибка при отправке результата: {e}")
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=get_message('download_complete_but_error', error=str(e))
                    )
            else:
                logger.error(f"Файл не найден: {file_path}")
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=get_message('download_complete_but_error', error="Файл не найден")
                )
        
        return result
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации загрузки: {e}")
        if message_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=get_message('download_error', url=url, error=str(e))
                )
            except Exception:
                pass
        raise

def download_video(url, ydl_opts, context, user_id):
    """Синхронная функция для скачивания видео (запускается в отдельном потоке)."""
    temp_dir = os.path.join(config.DOWNLOAD_DIR, f"user_{user_id}")
    os.makedirs(temp_dir, exist_ok=True)
    
    # Базовые опции
    base_opts = {
        'outtmpl': os.path.join(temp_dir, '%(title)s-%(id)s.%(ext)s'),
        'quiet': True,
        'no_warnings': True,
        'progress_hooks': [lambda d: progress_hook(d, context, url)],
        'restrictfilenames': True,
        'no_color': True,
        'user_id': user_id  # Добавляем ID пользователя в опции
    }
    
    # Объединяем базовые опции с переданными
    final_opts = {**base_opts, **(ydl_opts or {})}
    
    try:
        with yt_dlp.YoutubeDL(final_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if info:
                result = {
                    'filepath': ydl.prepare_filename(info),
                    'title': info.get('title', 'Видео'),
                    'thumbnail': info.get('thumbnail'),
                    'duration': info.get('duration'),
                    'size': info.get('filesize', 0)
                }
                
                # Если расширение не соответствует формату, исправляем путь
                if 'ext' in info and not result['filepath'].endswith(f".{info['ext']}"):
                    result['filepath'] = f"{os.path.splitext(result['filepath'])[0]}.{info['ext']}"
                
                return result
            else:
                raise ValueError("Не удалось получить информацию о видео")
    except Exception as e:
        logger.error(f"Ошибка при скачивании видео {url}: {e}")
        raise
    
    return None

def progress_hook(d, context, url):
    """Хук для отслеживания прогресса скачивания."""
    if not hasattr(context.bot_data, 'progress_data') or url not in context.bot_data['progress_data']:
        return
    
    progress_data = context.bot_data['progress_data'][url]
    
    # Обновляем данные о прогрессе
    if d['status'] == 'downloading':
        if 'downloaded_bytes' in d:
            progress_data['downloaded_bytes'] = d['downloaded_bytes']
        if 'total_bytes' in d:
            progress_data['total_bytes'] = d['total_bytes']
        elif 'total_bytes_estimate' in d:
            progress_data['total_bytes'] = d['total_bytes_estimate']
        
        if 'speed' in d and d['speed']:
            progress_data['speed'] = d['speed']
        if 'eta' in d:
            progress_data['eta'] = d['eta']
        
        if progress_data['total_bytes'] > 0:
            progress_data['percent'] = (progress_data['downloaded_bytes'] / progress_data['total_bytes']) * 100
        
        progress_data['status'] = 'downloading'
        progress_data['filename'] = d.get('filename')
        
        # Сохраняем ID пользователя, если он есть в данных
        if 'user_id' in d:
            progress_data['user_id'] = d['user_id']
    
    elif d['status'] == 'finished':
        progress_data['status'] = 'finished'
        progress_data['percent'] = 100
        
    context.bot_data['progress_data'][url] = progress_data

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
    title = result.get('title', 'Видео')

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
async def download_with_quality(update: Update, context: ContextTypes.DEFAULT_TYPE, url, format_id):
    """Загружает видео с выбранным качеством."""
    try:
        # Отправляем сообщение о начале загрузки
        message = await update.callback_query.edit_message_text(
            text="⏳ Начинаю загрузку видео...",
            reply_markup=None
        )
        
        # Получаем ID пользователя и чата
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        message_id = message.message_id
        
        # Добавляем URL в активные загрузки
        with data_lock:
            active_downloads[url] = {
                'status': 'initializing',
                'percent': 0,
                'percent_rounded': 0,
                'downloaded_bytes': 0,
                'total_bytes': 0,
                'total_bytes_estimate': 0,
                'speed': 0,
                'eta': 0,
                'filename': '',
                'last_update': time.time(),
                'cancelled': False,
                'user_id': user_id,
                'chat_id': chat_id,
                'message_id': message_id
            }
        
        # Запускаем загрузку с увеличенным таймаутом
        try:
            # Устанавливаем таймаут в 10 минут (600 секунд)
            result = await asyncio.wait_for(
                downloader.download_video(
                    url=url,
                    format_id=format_id,
                    user_id=user_id,
                    chat_id=chat_id,
                    message_id=message_id
                ),
                timeout=600
            )
        except asyncio.TimeoutError:
            logger.error(f"Таймаут при загрузке видео: {url}")
            await update.callback_query.edit_message_text(
                text="❌ Ошибка: превышено время ожидания при загрузке видео. Попробуйте еще раз или выберите другое качество.",
                reply_markup=None
            )
            return
        except Exception as e:
            logger.error(f"Ошибка при загрузке видео: {e}")
            await update.callback_query.edit_message_text(
                text=f"❌ Ошибка при загрузке видео: {str(e)}",
                reply_markup=None
            )
            return
        
        # Запускаем обновление прогресса
        progress_task = asyncio.create_task(
            update_progress_message(chat_id, message_id, url)
        )
        
        # Отменяем задачу обновления прогресса
        progress_task.cancel()
        
        # Проверяем результат загрузки
        if not result or not result.get('success', False):
            error_message = result.get('error', 'Неизвестная ошибка') if result else 'Неизвестная ошибка'
            logger.error(f"Ошибка при загрузке видео: {error_message}")
            await update.callback_query.edit_message_text(
                text=f"❌ Ошибка при загрузке видео: {error_message}",
                reply_markup=None
            )
            return
        
        # Получаем информацию о загруженном файле
        file_path = result.get('filename')
        title = result.get('title', 'Видео')
        
        if not os.path.exists(file_path):
            logger.error(f"Файл не найден: {file_path}")
            await update.callback_query.edit_message_text(
                text="❌ Ошибка: файл не найден после загрузки",
                reply_markup=None
            )
            return
        
        # Получаем размер файла
        file_size = os.path.getsize(file_path)
        
        # Максимальный размер файла для Telegram (50 МБ)
        MAX_TELEGRAM_SIZE = 50 * 1024 * 1024
        
        # Проверяем размер файла
        if file_size > MAX_TELEGRAM_SIZE:
            # Если файл слишком большой, предлагаем пользователю выбрать способ получения
            file_size_mb = file_size / (1024 * 1024)
            
            # Создаем уникальный ID для файла
            file_id = str(uuid.uuid4())
            
            # Сохраняем информацию о файле в контексте бота
            if 'large_files' not in context.bot_data:
                context.bot_data['large_files'] = {}
                
            context.bot_data['large_files'][file_id] = {
                'file_path': file_path,
                'title': title,
                'size': file_size_mb
            }
            
            # Создаем клавиатуру с опциями
            keyboard = [
                [
                    InlineKeyboardButton("Разделить на части", callback_data=f"split_{file_id}"),
                    InlineKeyboardButton("Создать прямую ссылку", callback_data=f"link_{file_id}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Отправляем сообщение с опциями
            await update.callback_query.edit_message_text(
                text=f"📁 Файл слишком большой ({file_size_mb:.2f} МБ). Выберите способ получения:",
                reply_markup=reply_markup
            )
            return
        
        # Если файл не слишком большой, отправляем его напрямую
        logger.info(f"Отправка файла: {file_path}")
        with open(file_path, 'rb') as video_file:
            await context.bot.send_video(
                chat_id=chat_id,
                video=video_file,
                caption=f"🎥 {title}",
                supports_streaming=True,
                read_timeout=120, write_timeout=120, connect_timeout=60, pool_timeout=120
            )
        
        # Отправляем сообщение об успешной загрузке
        await update.callback_query.edit_message_text(
            text=f"✅ Видео успешно загружено и отправлено!"
        )
        
    except Exception as e:
        logger.error(f"Ошибка в download_with_quality: {e}")
        import traceback
        logger.error(traceback.format_exc())
        try:
            await update.callback_query.edit_message_text(
                text=f"❌ Произошла ошибка: {str(e)}"
            )
        except Exception as edit_err:
            logger.error(f"Не удалось отправить сообщение об ошибке: {edit_err}")
    finally:
        # Очищаем состояние загрузки
        _cleanup_download_state(url, None, None)

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
        # Добавляем кнопку отмены в сообщение о начале загрузки плейлиста
        keyboard = [[InlineKeyboardButton(get_message('cancel_button'), callback_data=f"pl_stop_{original_message_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            get_message('playlist_download_starting', count=video_count),
            reply_markup=reply_markup
        )
    except Exception as e:
         logger.warning(f"Не удалось обновить сообщение о начале загрузки плейлиста: {e}")

    user_quality = db.get_user_settings(user_id)
    if user_quality == 'auto':
         logger.info(f"(Плейлист) Качество пользователя 'auto', используем 'high'.")
         user_quality = 'high' 
    
    semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_DOWNLOADS)
    
    # Добавляем плейлист в контекст для возможности отмены
    if 'active_playlists' not in context.bot_data:
        context.bot_data['active_playlists'] = {}
    
    context.bot_data['active_playlists'][original_message_id] = {
        'is_cancelled': False,
        'chat_id': chat_id,
        'tasks': []
    }
    
    tasks = []
    started_count = 0
    for video_url in video_urls:
        # Проверяем, не была ли отменена загрузка
        if context.bot_data['active_playlists'].get(original_message_id, {}).get('is_cancelled', False):
            logger.info(f"Загрузка плейлиста {original_message_id} была отменена. Пропуск оставшихся видео.")
            break
            
        if not db.check_download_limit(user_id):
            logger.warning(f"(Плейлист) Достигнут лимит для user {user_id}. Пропуск оставшихся {len(video_urls) - started_count} видео.")
            await context.bot.send_message(
                 chat_id=chat_id, 
                 text=get_message('limit_reached_playlist', limit=config.MAX_DOWNLOADS_PER_USER, started=started_count)
            )
            break 
        
        db.update_user_stats(user_id, update.effective_user.username)
        started_count += 1
        
        download_task = context.application.create_task(
            _download_playlist_video(context, video_url, user_id, chat_id, user_quality, semaphore)
        )
        tasks.append(download_task)
        
        # Сохраняем задачу для возможности отмены
        if original_message_id in context.bot_data['active_playlists']:
            context.bot_data['active_playlists'][original_message_id]['tasks'].append(download_task)
            
        await asyncio.sleep(0.1)

    results = []
    if tasks:
         results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Удаляем плейлист из контекста
    if 'active_playlists' in context.bot_data and original_message_id in context.bot_data['active_playlists']:
        del context.bot_data['active_playlists'][original_message_id]
        if not context.bot_data['active_playlists']:
            del context.bot_data['active_playlists']
    
    errors_count = sum(1 for res in results if isinstance(res, Exception))

    # Отправляем сообщение о завершении, только если загрузка не была отменена
    if not context.bot_data.get('active_playlists', {}).get(original_message_id, {}).get('is_cancelled', False):
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message.message_id,
                text=get_message('playlist_download_finished', 
                               total=started_count, 
                               success=(started_count - errors_count),
                               errors=errors_count)
            )
        except Exception as e:
            # Если не можем отредактировать, отправляем новое сообщение
            logger.warning(f"Не удалось обновить сообщение о завершении плейлиста: {e}")
    await context.bot.send_message(
        chat_id=chat_id,
        text=get_message('playlist_download_finished', 
                         total=started_count, 
                         success=(started_count - errors_count),
                         errors=errors_count)
    )

# --- Новая функция для отмены загрузки плейлиста ---
async def playlist_stop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик для отмены загрузки активного плейлиста."""
    query = update.callback_query
    message = query.message
    
    try:
        # Извлекаем ID сообщения с плейлистом
        playlist_id = int(query.data.split('_')[-1])
    except (IndexError, ValueError):
        logger.error(f"Не удалось извлечь message_id из callback_data: {query.data}")
        await query.answer("Произошла ошибка.")
        return
    
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e) or "query id is invalid" in str(e):
            logger.warning(f"Callback query для остановки плейлиста устарел: {e}")
            return
        else:
            logger.error(f"Ошибка BadRequest при ответе на callback остановки плейлиста: {e}")
            return
    except Exception as e:
        logger.error(f"Неожиданная ошибка при ответе на callback остановки плейлиста: {e}")
        return
    
    if 'active_playlists' not in context.bot_data or playlist_id not in context.bot_data['active_playlists']:
        logger.warning(f"Не найден активный плейлист {playlist_id} для отмены.")
        try:
            await query.edit_message_text(get_message('error_cancel_failed'))
        except Exception as e:
            logger.error(f"Не удалось обновить сообщение об ошибке отмены плейлиста: {e}")
        return
    
    # Помечаем плейлист как отмененный
    context.bot_data['active_playlists'][playlist_id]['is_cancelled'] = True
    
    # Отменяем все активные задачи
    for task in context.bot_data['active_playlists'][playlist_id].get('tasks', []):
        if not task.done():
            task.cancel()
    
    logger.info(f"Плейлист {playlist_id} отменен пользователем.")
    
    try:
        await query.edit_message_text(get_message('playlist_cancelled'))
    except Exception as e:
        logger.error(f"Не удалось обновить сообщение об отмене плейлиста: {e}")

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
    application.add_handler(CallbackQueryHandler(playlist_stop_callback, pattern=r'^pl_stop_'))
    # --- Добавляем обработчик для кнопки отмены загрузки ---
    application.add_handler(CallbackQueryHandler(cancel_download_callback, pattern=r'^cancel_download_'))
    # --- Добавляем обработчик для кнопки отмены ---
    application.add_handler(CallbackQueryHandler(cancel_callback, pattern=r'^cancel_'))
    # --- Добавляем обработчик для кнопки выбора качества ---
    application.add_handler(CallbackQueryHandler(quality_callback, pattern=r'^download_'))
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

    # Обработка кнопки отмены
    if format_id == "cancel":
        # Удаляем информацию о запросе из контекста
        if CHAT_CONTEXT_KEY in context.chat_data and message_id in context.chat_data[CHAT_CONTEXT_KEY]:
            context.chat_data[CHAT_CONTEXT_KEY].pop(message_id, None)
            if not context.chat_data[CHAT_CONTEXT_KEY]:
                del context.chat_data[CHAT_CONTEXT_KEY]
                
        # Уведомляем пользователя об отмене
        await query.edit_message_text(get_message('download_cancelled'))
        return

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

    # Логируем информацию о выбранном формате
    logger.info(f"Выбран формат: {format_id} для URL: {url}")

    # Обновляем статистику перед началом загрузки
    db.update_user_stats(update.effective_user.id, update.effective_user.username)
    
    # Запускаем скачивание одиночного видео
    await download_with_quality(update, context, url, format_id)
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
                try:
                    with open(part_path, 'rb') as part_file:
                        await context.bot.send_video(
                            chat_id=chat_id,
                            video=part_file,
                            caption=get_message('split_video_part', part=i, total=total_parts, title=title),
                            supports_streaming=True,
                            read_timeout=120, write_timeout=120, connect_timeout=60, pool_timeout=120
                        )
                except Exception as send_err:
                    logger.error(f"Ошибка при отправке части {i}: {send_err}")
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"❌ Ошибка при отправке части {i}: {str(send_err)}"
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
            import traceback
            logger.error(traceback.format_exc())
            await context.bot.send_message(
                chat_id=chat_id,
                text=get_message('download_error')
            )
            
    elif action == "link":
        # Генерируем прямую ссылку с правильным именем файла
        await query.edit_message_text(get_message('direct_link_generating'))
        
        try:
            # Импортируем здесь для избежания циклических зависимостей
            from link_generator import LinkGenerator
            link_gen = LinkGenerator()
            
            # Получаем оригинальное имя файла с расширением
            original_filename = os.path.basename(file_path)
            # Добавляем название видео к имени файла, если оно не является просто номером
            if title and not title.isdigit() and title != "Видео" and title != original_filename:
                name_parts = os.path.splitext(original_filename)
                original_filename = f"{title}{name_parts[1]}" if len(name_parts) > 1 else f"{title}.mp4"
                
            # Генерируем ссылку с названием видео в имени файла
            link_info = await link_gen.generate_link(file_path, original_filename)
            
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

# --- Новый обработчик для кнопки отмены загрузки --- 
async def cancel_download_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатия на кнопку отмены загрузки."""
    try:
        # Получаем данные из callback
        query = update.callback_query
        await query.answer()
        
        # Извлекаем URL и user_id из callback_data
        callback_data = query.data
        parts = callback_data.replace("cancel_download_", "").split("_")
        
        if len(parts) < 2:
            logger.error(f"Неверный формат callback_data: {callback_data}")
            await query.edit_message_text("❌ Ошибка: неверный формат данных")
            return
        
        url = parts[0]
        user_id = int(parts[1])
        
        # Проверяем, что отменяет загрузку тот же пользователь, который её начал
        if query.from_user.id != user_id:
            await query.answer("Вы не можете отменить чужую загрузку", show_alert=True)
            return
        
        # Отменяем загрузку
        success = downloader.cancel_download(url)
        
        if success:
            # Обновляем сообщение
            await query.edit_message_text(
                text="❌ Загрузка отменена пользователем",
                reply_markup=None
            )
            logger.info(f"Загрузка {url} отменена пользователем")
        else:
            # Обновляем сообщение
            await query.edit_message_text(
                text="⚠️ Не удалось отменить загрузку. Возможно, она уже завершена.",
                reply_markup=None
            )
            logger.warning(f"Не удалось отменить загрузку {url}")
    
    except Exception as e:
        logger.error(f"Ошибка при отмене загрузки: {e}")
        try:
            await query.edit_message_text(
                text="❌ Произошла ошибка при отмене загрузки",
                reply_markup=None
            )
        except:
            pass

async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает отмену загрузки."""
    query = update.callback_query
    data = query.data
    
    try:
        # Извлекаем URL хеш и user_id из callback_data
        # Формат: "cancel_[url_hash]_[user_id]"
        parts = data.split('_')
        if len(parts) < 3:
            logger.error(f"Неверный формат callback_data: {data}")
            await query.edit_message_text("❌ Ошибка: неверный формат данных")
            return
            
        url_hash = parts[1]
        user_id = int(parts[2])
        
        # Проверяем, что пользователь, отменяющий загрузку, тот же, что и запрашивал видео
        if update.effective_user.id != user_id:
            logger.warning(f"Пользователь {update.effective_user.id} пытается отменить загрузку пользователя {user_id}")
            await query.edit_message_text("❌ Вы не можете отменить чужую загрузку")
            return
            
        # Получаем оригинальный URL из контекста чата
        chat_id = update.effective_chat.id
        if chat_id not in context.chat_data or CHAT_CONTEXT_KEY not in context.chat_data[chat_id]:
            logger.error(f"URL не найден в контексте чата {chat_id}")
            await query.edit_message_text("❌ Ошибка: URL не найден в контексте")
            return
            
        url = context.chat_data[chat_id][CHAT_CONTEXT_KEY]
        
        # Отменяем загрузку
        with data_lock:
            if url in active_downloads:
                active_downloads[url]['cancelled'] = True
                logger.info(f"Загрузка {url} отменена пользователем {user_id}")
                
                # Отменяем задачу обновления прогресса, если она существует
                if 'progress_task' in active_downloads[url]:
                    progress_task = active_downloads[url]['progress_task']
                    if progress_task and not progress_task.done():
                        progress_task.cancel()
                        logger.debug(f"Задача обновления прогресса для URL '{url}' отменена.")
        
        # Отправляем сообщение об отмене
        await query.edit_message_text("❌ Загрузка отменена")
        
    except Exception as e:
        logger.error(f"Ошибка в cancel_callback: {e}")
        import traceback
        logger.error(traceback.format_exc())
        try:
            await query.edit_message_text("❌ Произошла ошибка при отмене загрузки")
        except Exception as edit_err:
            logger.error(f"Не удалось отправить сообщение об ошибке: {edit_err}")

def format_size(size_bytes):
    """Форматирует размер в байтах в человекочитаемый формат."""
    if not size_bytes:
        return "N/A"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            if unit == 'B':
                return f"{size_bytes:.0f} {unit}"
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"

def format_time(seconds):
    """Форматирует время в секундах в человекочитаемый формат."""
    if not seconds:
        return "N/A"
    
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    
    if hours > 0:
        return f"{hours}ч {minutes}м {seconds}с"
    elif minutes > 0:
        return f"{minutes}м {seconds}с"
    else:
        return f"{seconds}с"

async def quality_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор качества видео."""
    query = update.callback_query
    data = query.data
    
    try:
        # Извлекаем URL хеш, format_id и user_id из callback_data
        # Формат: "download_[url_hash]_[format_id]_[user_id]"
        parts = data.split('_')
        if len(parts) < 4:
            logger.error(f"Неверный формат callback_data: {data}")
            await query.edit_message_text("❌ Ошибка: неверный формат данных")
            return
            
        url_hash = parts[1]
        format_id = parts[2]
        user_id = int(parts[3])
        
        # Проверяем, что пользователь, выбирающий качество, тот же, что и запрашивал видео
        if update.effective_user.id != user_id:
            logger.warning(f"Пользователь {update.effective_user.id} пытается выбрать качество для запроса пользователя {user_id}")
            await query.edit_message_text("❌ Вы не можете выбрать качество для чужого запроса")
            return
            
        # Получаем оригинальный URL из контекста чата
        chat_id = update.effective_chat.id
        if chat_id not in context.chat_data or CHAT_CONTEXT_KEY not in context.chat_data[chat_id]:
            logger.error(f"URL не найден в контексте чата {chat_id}")
            await query.edit_message_text("❌ Ошибка: URL не найден в контексте")
            return
            
        url = context.chat_data[chat_id][CHAT_CONTEXT_KEY]
        
        # Запускаем загрузку с выбранным качеством
        await download_with_quality(update, context, url, format_id)
        
    except Exception as e:
        logger.error(f"Ошибка в quality_callback: {e}")
        import traceback
        logger.error(traceback.format_exc())
        try:
            await query.edit_message_text("❌ Произошла ошибка при выборе качества")
        except Exception as edit_err:
            logger.error(f"Не удалось отправить сообщение об ошибке: {edit_err}")

async def user_can_download(user_id, chat_id):
    """Проверяет, может ли пользователь начать новую загрузку."""
    try:
        # Проверяем лимит загрузок
        if not db.check_download_limit(user_id):
            logger.warning(f"Пользователь {user_id} превысил лимит загрузок")
            return False
        
        # Проверяем количество активных загрузок
        with data_lock:
            active_count = sum(1 for url, info in active_downloads.items() if info.get('user_id') == user_id)
        
        if active_count >= config.MAX_CONCURRENT_DOWNLOADS:
            logger.warning(f"Пользователь {user_id} превысил лимит одновременных загрузок")
            return False
        
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при проверке возможности загрузки для пользователя {user_id}: {e}")
        return False

if __name__ == '__main__':
    main() 