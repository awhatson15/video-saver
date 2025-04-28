import os
import logging
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.error import BadRequest
import re
import subprocess
import time

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

# Константа для ключа контекста чата
CHAT_CONTEXT_KEY = 'video_requests'

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
    """Обработчик для URL адресов"""
    message = update.message
    message_text = message.text

    if not URL_PATTERN.search(message_text):
        return

    url = URL_PATTERN.search(message_text).group(0)
    user_id = update.effective_user.id
    username = update.effective_user.username

    if not db.check_download_limit(user_id):
        await message.reply_text(
            get_message('limit_reached', limit=config.MAX_DOWNLOADS_PER_USER)
        )
        return

    db.update_user_stats(user_id, username)

    progress_message = await message.reply_text(get_message('quality_auto'))
    message_id = progress_message.message_id

    try:
        video_info = await downloader.get_video_info(url)
        formats = video_info.get('formats', [])

        if formats:
            # --- Изменено: Возвращаем детальные кнопки, но callback_data указывает категорию --- 
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
                # Группируем по высоте, чтобы предложить по одной кнопке на разрешение
                for f in video_formats:
                    height = f.get('height')
                    if height and height not in grouped_formats:
                         grouped_formats[height] = f # Берем первый попавшийся формат для этой высоты
                
                keyboard = []
                format_list_texts = [] # Тексты для отображения в сообщении
                
                # Создаем кнопки для каждого уникального разрешения
                for height, f in sorted(grouped_formats.items(), reverse=True):
                    format_id = f.get('format_id') # Получаем ID для информации, но не для callback
                    if not format_id:
                        logger.warning(f"Пропуск формата без ID для URL {url}: {f}")
                        continue

                    # Определяем категорию качества для callback_data
                    quality_category = 'high' # По умолчанию
                    if height <= 480:
                        quality_category = 'medium'
                    # Условие для low может быть сложнее, т.к. зависит от худшего. 
                    # Пока оставим так: все <= 480 это medium, все > 480 это high.
                    # Можно добавить более точное определение, если нужно.
                    
                    # Рассчитываем размер для отображения
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
                    
                    # Текст кнопки и описание формата
                    format_button_text = get_message('quality_format',
                        resolution=f"{height}p",
                        size=size_mb,
                        fps=f.get('fps', '?')
                    )
                    format_list_texts.append(f"- {format_button_text}") # Добавляем в список для сообщения

                    # Создаем кнопку: текст детальный, callback - категория
                    button_callback_data = f"format_{quality_category}"
                    keyboard.append([InlineKeyboardButton(
                        format_button_text,
                        callback_data=button_callback_data
                    )])
            
            # Добавляем кнопки Аудио и Авто
            keyboard.append([InlineKeyboardButton(
                get_message('quality_audio'), 
                callback_data="format_audio"
            )])
            keyboard.append([InlineKeyboardButton(
                get_message('quality_auto_button'), 
                callback_data="format_auto"
            )])

            reply_markup = InlineKeyboardMarkup(keyboard)
            # --- Конец изменений ---

            # Сохраняем URL в контексте чата, связанный с ID сообщения
            if CHAT_CONTEXT_KEY not in context.chat_data:
                context.chat_data[CHAT_CONTEXT_KEY] = {}
            context.chat_data[CHAT_CONTEXT_KEY][message_id] = {'url': url}
            logger.debug(f"Сохранен URL '{url}' для message_id {message_id} в chat_data.")

            await progress_message.edit_text(
                get_message('quality_selection', formats="\n".join(format_list_texts)), # Возвращаем старый ключ
                reply_markup=reply_markup
            )
        else:
            logger.info(f"Форматы не найдены для URL {url}, запускаем скачивание в auto.")
            # --- Изменено: передаем progress_message напрямую --- 
            await download_with_quality(update, context, url, "auto", progress_message)

    except Exception as e:
        logger.exception(f"Ошибка при обработке URL '{url}': {e}") # Используем logger.exception для стектрейса
        # Очищаем контекст, если он был создан
        if CHAT_CONTEXT_KEY in context.chat_data and message_id in context.chat_data[CHAT_CONTEXT_KEY]:
            del context.chat_data[CHAT_CONTEXT_KEY][message_id]
        try:
            if progress_message:
                await progress_message.edit_text(get_message('download_error'))
            else:
                await message.reply_text(get_message('download_error'))
        except Exception as edit_err:
             logger.error(f"Не удалось отправить сообщение об ошибке обработки URL: {edit_err}")

async def notifications_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /notifications"""
    user_id = update.effective_user.id
    settings = db.get_notification_settings(user_id)
    
    keyboard = []
    notification_names = {
        "download_complete": "Завершение загрузки",
        "download_error": "Ошибка загрузки",
        "download_progress": "Прогресс загрузки",
        "system_alert": "Системные оповещения"
    }
    for setting, enabled in settings.items():
        status = "✅ Включено" if enabled else "❌ Выключено"
        button_text = get_message('notification_toggle', 
                                  name=notification_names.get(setting, setting), 
                                  status=status)
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
    await query.answer()
    
    data = query.data
    if not data.startswith("notify_"):
        return
    
    notification_type = data.split("_", 1)[1]
    user_id = update.effective_user.id
    
    if db.toggle_notification(user_id, notification_type):
        settings = db.get_notification_settings(user_id)
        keyboard = []
        notification_names = {
             "download_complete": "Завершение загрузки",
             "download_error": "Ошибка загрузки",
             "download_progress": "Прогресс загрузки",
             "system_alert": "Системные оповещения"
        }
        for setting, enabled in settings.items():
            status = "✅ Включено" if enabled else "❌ Выключено"
            button_text = get_message('notification_toggle', 
                                      name=notification_names.get(setting, setting), 
                                      status=status)
            keyboard.append([
                InlineKeyboardButton(button_text, callback_data=f"notify_{setting}")
            ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await query.edit_message_text(
                get_message('notification_settings'),
                reply_markup=reply_markup
            )
        except Exception as e:
            if "Message is not modified" not in str(e):
                 logger.error(f"Ошибка при обновлении настроек уведомлений: {e}")
        
        await context.bot.send_message(
            chat_id=user_id,
            text=get_message('notification_updated')
        )

async def send_notification(context: ContextTypes.DEFAULT_TYPE, user_id: int, notification_type: str, **kwargs):
    """Отправка уведомления пользователю"""
    settings = db.get_notification_settings(user_id)
    
    if not settings.get(notification_type, True):
        return
    
    message_key = f"{notification_type}_notification"
    message = get_message(message_key, **kwargs)
    
    if message == message_key or not message: 
        logger.warning(f"Шаблон уведомления для ключа '{message_key}' не найден или пуст.")
        return
        
    try:
        await context.bot.send_message(chat_id=user_id, text=message)
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления '{notification_type}' пользователю {user_id}: {e}")

# --- Рефакторинг: Вспомогательные функции для download_with_quality --- 

async def _initialize_download(context: ContextTypes.DEFAULT_TYPE, url: str, format_id: str, user_id: int, chat_id: int, message_id: int):
    """Инициализирует загрузку: получает инфо, определяет качество, обновляет сообщение, инициализирует словари."""
    start_time = time.time()
    actual_format_id = format_id
    quality_name = format_id
    canonical_url = url # Значение по умолчанию

    try:
        video_info = await downloader.get_video_info(url)
        canonical_url = video_info.get('webpage_url', url)
    except Exception as info_err:
        logger.warning(f"Не удалось получить video_info для '{url}' в _initialize_download: {info_err}. Используем исходный URL как канонический.")

    # Определение quality_name (логика из старой функции)
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
    else:
        logger.warning(f"Неизвестный format_id '{format_id}' получен в _initialize_download")

    # Обновление сообщения
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
        raise # Перебрасываем ошибку, т.к. без сообщения неясно, что происходит

    # Инициализация active_downloads и map
    with data_lock:
        active_downloads[url] = {
            'percent': 0, 'percent_rounded': 0, 'downloaded_bytes': 0,
            'speed': 0, 'eta': 0, 'filename': None,
            'chat_id': chat_id, 'message_id': message_id,
            'canonical_url': canonical_url,
            'process': None
        }
        canonical_url_map[canonical_url] = url
        logger.debug(f"Initialized active_downloads and map for {url} (canonical: {canonical_url})")
        
    return actual_format_id, canonical_url, start_time, progress_message

async def _run_actual_download(context: ContextTypes.DEFAULT_TYPE, url: str, actual_format_id: str, user_id: int, chat_id: int, message_id: int):
    """Запускает фоновую задачу обновления прогресса и саму загрузку."""
    progress_task = context.application.create_task(
        update_progress_message(context, chat_id, message_id, url)
    )
    
    try:
        result = await downloader.download_video(url, actual_format_id, user_id, chat_id, message_id)
        return result, progress_task
    except Exception:
         # Если скачивание не удалось, отменяем задачу прогресса здесь
         if progress_task and not progress_task.done():
              progress_task.cancel()
         raise # Перебрасываем ошибку дальше

async def _send_video_result(context: ContextTypes.DEFAULT_TYPE, result: dict, chat_id: int, message_id: int, progress_message):
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
            await progress_message.edit_text(get_message('split_video_started'))
            # Разделение теперь синхронное, но вызывается из async контекста
            # Для полной асинхронности нужно переделывать split_large_video
            video_parts = downloader.split_large_video(file_path)
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
            await progress_message.edit_text(get_message('split_video_completed'))
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
            # Удаляем сообщение о прогрессе после успешной отправки целого файла
            try:
                 await progress_message.delete()
            except Exception as del_err:
                 logger.warning(f"Не удалось удалить сообщение о прогрессе {message_id}: {del_err}")

    finally:
        # Гарантированное удаление частей видео, если они были созданы
        for part_f in video_parts:
            try:
                if os.path.exists(part_f):
                    os.remove(part_f)
                    logger.info(f"Удалена часть видео: {part_f}")
            except OSError as rm_err:
                 logger.warning(f"Не удалось удалить часть видео {part_f}: {rm_err}")
        
        # Удаляем исходный файл, если кэш отключен и видео НЕ разделялось
        # (Если разделялось, части удалены выше, а исходный может быть нужен кэшу)
        if not video_parts and not config.CACHE_ENABLED:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Удален исходный файл (кэш отключен): {file_path}")
            except OSError as rm_err:
                logger.warning(f"Не удалось удалить исходный файл {file_path}: {rm_err}")

def _cleanup_download_state(url: str, canonical_url: str | None, progress_task):
    """Отменяет задачу прогресса и очищает словари."""
    if progress_task and not progress_task.done():
        progress_task.cancel()
        logger.debug(f"Задача обновления прогресса для URL '{url}' отменена в _cleanup_download_state.")
    
    with data_lock:
        if url in active_downloads:
            logger.debug(f"Очистка active_downloads для {url} в _cleanup_download_state.")
            del active_downloads[url]
        if canonical_url and canonical_url in canonical_url_map:
            logger.debug(f"Очистка canonical_url_map для {canonical_url} в _cleanup_download_state.")
            del canonical_url_map[canonical_url]

# --- Конец вспомогательных функций --- 

async def download_with_quality(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str, format_id: str, progress_message):
    """Скачивание видео с выбранным качеством (оркестратор)"""
    user_id = update.effective_user.id
    chat_id = progress_message.chat_id
    message_id = progress_message.message_id # Получаем ID из переданного сообщения
    
    result = None
    progress_task = None
    canonical_url = None
    start_time = None

    try:
        # 1. Инициализация
        actual_format_id, canonical_url, start_time, current_progress_message = await _initialize_download(
            context, url, format_id, user_id, chat_id, message_id
        )
        # Обновляем progress_message на случай, если edit_message_text вернул новый объект
        progress_message = current_progress_message 

        # 2. Запуск скачивания и прогресса
        result, progress_task = await _run_actual_download(
            context, url, actual_format_id, user_id, chat_id, message_id
        )

        # 3. Обработка и отправка результата (если скачивание успешно)
        if result:
            # Отправляем уведомление о завершении
            download_duration = round(time.time() - start_time, 1)
            await send_notification(
                context, user_id, "download_complete",
                title=result['title'],
                # Используем actual_format_id для получения имени качества
                quality=get_message(f'quality_{actual_format_id}') if actual_format_id in ['low', 'medium', 'high', 'audio'] else actual_format_id,
                size=round(result['size'] / (1024 * 1024), 1),
                time=download_duration
            )
            # Отправляем видео
            await _send_video_result(context, result, chat_id, message_id, progress_message)

    except Exception as e:
        logger.exception(f"Ошибка при обработке запроса на скачивание для URL '{url}': {e}")
        try:
            # Пытаемся обновить исходное сообщение об ошибке
            await context.bot.edit_message_text(
                chat_id=chat_id, 
                message_id=message_id, 
                text=get_message('download_error')
            )
        except Exception as edit_err:
            logger.error(f"Не удалось отредактировать сообщение {message_id} об ошибке скачивания: {edit_err}")
        # Отправляем уведомление об ошибке
        await send_notification(
            context, user_id, "download_error",
            title=url, # Используем URL, т.к. title может быть недоступен
            error=str(e)
        )

    finally:
        # 4. Очистка состояния (прогресс-задача, словари)
        _cleanup_download_state(url, canonical_url, progress_task)
        
        # Очистка контекста чата (для callback-кнопок, если они были)
        if CHAT_CONTEXT_KEY in context.chat_data and message_id in context.chat_data[CHAT_CONTEXT_KEY]:
            del context.chat_data[CHAT_CONTEXT_KEY][message_id]
            logger.debug(f"Очищен контекст чата для message_id {message_id}")

async def format_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора формата видео"""
    query = update.callback_query
    message = query.message
    message_id = message.message_id
    chat_id = message.chat_id
    data = query.data

    await query.answer() # Отвечаем на коллбек

    if not data.startswith("format_"):
        logger.warning(f"Некорректные данные в format_callback: {data}")
        return

    # Извлекаем ID формата
    format_id = data.split("_", 1)[1] # format_auto или format_123

    # Получаем URL из контекста чата
    url = None
    if CHAT_CONTEXT_KEY in context.chat_data and message_id in context.chat_data[CHAT_CONTEXT_KEY]:
        url = context.chat_data[CHAT_CONTEXT_KEY][message_id].get('url')

    if not url:
        logger.error(f"Не найден URL в chat_data для message_id {message_id}. Невозможно продолжить.")
        try:
            await query.edit_message_text(get_message('error_context_lost'))
        except Exception as e:
             logger.error(f"Не удалось отредактировать сообщение об утерянном контексте: {e}")
        return

    # Опционально: удаляем контекст сразу после получения URL?
    # del context.chat_data[CHAT_CONTEXT_KEY][message_id] # Делаем это в finally в download_with_quality
    # logger.debug(f"Контекст для message_id {message_id} извлечен и будет очищен.")

    # Обновляем сообщение перед началом скачивания
    try:
        await message.edit_text(get_message('download_started'))
    except Exception as e:
        logger.warning(f"Не удалось отредактировать сообщение на 'download_started': {e}")

    # Запускаем скачивание
    await download_with_quality(update, context, url, format_id, message)

# --- Новый обработчик для кнопки Отмены --- 
async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатие кнопки отмены загрузки."""
    query = update.callback_query
    message = query.message
    message_id = message.message_id
    chat_id = message.chat_id # Необязательно, но может пригодиться
    user_id = query.from_user.id

    # --- Изменено: Обработка ошибки BadRequest при ответе --- 
    try:
        await query.answer() # Отвечаем на коллбек
    except BadRequest as e:
        # Если запрос слишком старый, просто логируем и выходим
        if "Query is too old" in str(e) or "query id is invalid" in str(e):
            logger.warning(f"Не удалось ответить на callback query (устарел или невалиден): {e}")
            return # Не можем продолжить обработку
        else:
            logger.error(f"Неожиданная ошибка BadRequest при ответе на callback query: {e}")
            # Можно попробовать продолжить, но лучше выйти
            return
    except Exception as e:
        logger.error(f"Неожиданная ошибка при ответе на callback query: {e}")
        return
    # --- Конец изменений ---

    url_to_cancel = None
    # Ищем URL в контексте чата по ID сообщения
    if CHAT_CONTEXT_KEY in context.chat_data and message_id in context.chat_data[CHAT_CONTEXT_KEY]:
        url_to_cancel = context.chat_data[CHAT_CONTEXT_KEY][message_id].get('url')

    if not url_to_cancel:
        logger.warning(f"Не найден URL для отмены в chat_data по message_id {message_id}. Возможно, загрузка уже завершена/отменена.")
        try:
            # Просто удаляем кнопку, если не можем найти URL
            await query.edit_message_text(text=message.text, reply_markup=None)
        except BadRequest as e:
            # --- Изменено: Игнорируем ошибку "Message to edit not found" ---
            if "Message to edit not found" in str(e):
                 logger.debug(f"Сообщение {message_id} для удаления кнопки отмены не найдено (возможно, уже удалено/изменено).")
            else:
                 logger.error(f"Не удалось убрать кнопку отмены для сообщения {message_id} без URL: {e}")
            # --- Конец изменений ---
        except Exception as e:
            # Логируем другие возможные ошибки
            logger.error(f"Не удалось убрать кнопку отмены для сообщения {message_id} без URL (не BadRequest): {e}")
        return

    logger.info(f"Пользователь {user_id} запросил отмену загрузки URL: {url_to_cancel}")
    
    # Вызываем метод отмены в даунлоадере
    cancelled_successfully = downloader.cancel_download(url_to_cancel)

    # Очищаем контекст чата
    if CHAT_CONTEXT_KEY in context.chat_data and message_id in context.chat_data[CHAT_CONTEXT_KEY]:
        del context.chat_data[CHAT_CONTEXT_KEY][message_id]
        logger.debug(f"Очищен контекст для отмененной загрузки message_id {message_id}")

    # Обновляем сообщение для пользователя
    try:
        if cancelled_successfully:
            await query.edit_message_text(get_message('download_cancelled'), reply_markup=None) # Убираем кнопку
        else:
            # Если cancel_download вернул False (загрузки не было в активных)
            await query.edit_message_text(get_message('error_cancel_failed'), reply_markup=None)
    except BadRequest as e:
         # --- Добавлено: Игнорируем ошибку "Message to edit not found" --- 
        if "Message to edit not found" in str(e):
             logger.debug(f"Сообщение {message_id} для обновления статуса отмены не найдено.")
        else:
            logger.error(f"Ошибка BadRequest при обновлении сообщения после отмены загрузки {message_id}: {e}")
         # --- Конец добавления ---
    except Exception as e:
        # Логируем другие ошибки
        logger.error(f"Ошибка при обновлении сообщения после отмены загрузки {message_id} (не BadRequest): {e}")
# --- Конец нового обработчика --- 

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
    
    if not os.path.exists(config.DOWNLOAD_DIR):
        os.makedirs(config.DOWNLOAD_DIR)
    
    application = Application.builder().token(config.TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CommandHandler("notifications", notifications_command))
    application.add_handler(CallbackQueryHandler(settings_callback, pattern=r'^quality_'))
    application.add_handler(CallbackQueryHandler(format_callback, pattern=r'^format_'))
    application.add_handler(CallbackQueryHandler(notification_callback, pattern=r'^notify_'))
    # --- Добавляем обработчик для кнопки отмены --- 
    application.add_handler(CallbackQueryHandler(cancel_callback, pattern=r'^cancel_download$'))
    # --- Конец добавления обработчика --- 
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_url))
    
    application.run_polling()

if __name__ == '__main__':
    main() 