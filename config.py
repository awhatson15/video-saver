import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Конфигурация бота
TOKEN = os.getenv('TELEGRAM_TOKEN')

# Директории
DOWNLOAD_DIR = 'downloads'
DATABASE_PATH = 'video_cache.db'

# Настройки загрузки видео
DEFAULT_VIDEO_FORMAT = 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo[ext=mp4]/best[ext=mp4]'
MAX_TELEGRAM_SIZE = 50 * 1024 * 1024  # 50MB в байтах
VIDEO_FORMATS = {
    # Худшее mp4 видео <=480p + лучшее m4a аудио, с фоллбэками
    'low': 'worstvideo[ext=mp4][height<=?480]+bestaudio[ext=m4a]/worstvideo[ext=mp4]/worst[ext=mp4]/worst',
    # Лучшее mp4 видео <=480p + лучшее m4a аудио, с фоллбэками
    'medium': 'bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/bestvideo[height<=480][ext=mp4]/best[height<=480][ext=mp4]',
    # Лучшее mp4 видео <=1080p + лучшее m4a аудио, с фоллбэками
    'high': 'bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/bestvideo[height<=1080][ext=mp4]/best[height<=1080][ext=mp4]',
    # Лучшее m4a аудио, с фоллбэком
    'audio': 'bestaudio[ext=m4a]/bestaudio'
}

# Качество видео по умолчанию для разных типов
DEFAULT_QUALITY = {
    'low': '360p',
    'medium': '480p',
    'high': '1080p'
}

# Настройки кэширования
CACHE_ENABLED = True
CACHE_DURATION = 30  # дней

# Ограничение загрузок
MAX_DOWNLOADS_PER_USER = 200  # в день
MAX_CONCURRENT_DOWNLOADS = 5

# Настройки уведомлений
NOTIFICATION_SETTINGS = {
    'download_complete': True,  # Уведомление о завершении загрузки
    'download_error': True,     # Уведомление об ошибке
    'download_progress': True,  # Уведомление о прогрессе
    'system_alert': True,       # Системные уведомления
}

# --- Добавленные настройки для прямых ссылок ---
DIRECT_LINK_ENABLED = True  # Включить функцию прямых ссылок
DIRECT_LINK_DOMAIN = 'dl.rox.su'  # Домен для прямых ссылок
DIRECT_LINK_EXPIRE_HOURS = 24  # Срок действия ссылки в часах
DIRECT_LINK_MAX_SIZE_GB = 10  # Максимальный суммарный размер файлов для прямых ссылок в ГБ
DIRECT_LINK_CLEANUP_INTERVAL = 60 * 60  # Интервал очистки просроченных файлов в секундах (каждый час)
DIRECT_LINK_STORAGE = os.getenv('DIRECT_LINK_STORAGE', '/var/www/downloads/shared')  # Путь к директории для хранения файлов
# --- Конец добавленных настроек ---

# Удаляем словарь MESSAGES
# MESSAGES = { ... } 