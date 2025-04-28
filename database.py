import sqlite3
import os
# import time # Больше не нужен напрямую
from datetime import datetime, timedelta
import config
import logging # Добавляем импорт логгера

logger = logging.getLogger(__name__) # Инициализируем логгер

class Database:
    def __init__(self, db_path=config.DATABASE_PATH):
        self.db_path = db_path
        # Включаем автоматическое определение типов при подключении
        self._conn_kwargs = {'detect_types': sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES}
        self.init_db()

    def _get_connection(self):
        """Возвращает соединение с базой данных."""
        return sqlite3.connect(self.db_path, **self._conn_kwargs)

    def init_db(self):
        """Инициализация базы данных и миграция схемы при необходимости."""
        # Проверяем и обновляем схему, если колонки имеют старый тип INTEGER
        self._migrate_schema()

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Таблица для кэширования видео (используем TIMESTAMP)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS video_cache (
                url TEXT PRIMARY KEY,
                title TEXT,
                file_path TEXT,
                size INTEGER,
                format TEXT,
                created_at TIMESTAMP
            )
            ''')

            # Таблица для статистики пользователей (используем TIMESTAMP)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                downloads_today INTEGER DEFAULT 0,
                total_downloads INTEGER DEFAULT 0,
                last_download TIMESTAMP,
                video_format TEXT DEFAULT "high",
                notification_settings TEXT DEFAULT '{"download_complete": true, "download_error": true, "download_progress": true, "system_alert": true}'
            )
            ''')

            # Таблица для логов скачивания (используем TIMESTAMP)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS download_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                url TEXT,
                status TEXT,
                error TEXT,
                created_at TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES user_stats (user_id)
            )
            ''')

            # Проверка и добавление колонки notification_settings (остается как есть)
            try:
                cursor.execute("PRAGMA table_info(user_stats)")
                columns = [info[1] for info in cursor.fetchall()]
                if 'notification_settings' not in columns:
                    cursor.execute('''
                    ALTER TABLE user_stats
                    ADD COLUMN notification_settings TEXT
                    DEFAULT '{"download_complete": true, "download_error": true, "download_progress": true, "system_alert": true}'
                    ''')
                    logger.info("Добавлена колонка 'notification_settings' в таблицу user_stats.")
            except Exception as e:
                 logger.error(f"Ошибка при проверке/добавлении колонки notification_settings: {e}")

            conn.commit()

    def _migrate_schema(self):
        """Проверяет типы колонок времени и изменяет их с INTEGER на TIMESTAMP при необходимости."""
        tables_columns = {
            'video_cache': 'created_at',
            'download_logs': 'created_at',
            'user_stats': 'last_download'
        }
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                for table, column in tables_columns.items():
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns_info = {info[1]: info[2] for info in cursor.fetchall()}
                    if column in columns_info and columns_info[column].upper() == 'INTEGER':
                        logger.warning(f"Обнаружена старая схема: колонка '{column}' в таблице '{table}' имеет тип INTEGER. Попытка миграции...")
                        # Попытка изменить тип колонки (может не работать в старых SQLite)
                        # Более надежный способ - создать новую таблицу, скопировать данные, удалить старую, переименовать новую.
                        # Но для простоты попробуем ALTER COLUMN (нестандартно для SQLite)
                        try:
                             # SQLite не поддерживает ALTER COLUMN TYPE напрямую.
                             # Пропустим автоматическую миграцию типа данных.
                             # Пользователю придется удалить старый .db файл или смигрировать вручную,
                             # если он столкнется с ошибками типов данных.
                             logger.warning(f"Автоматическая миграция типа колонки {table}.{column} с INTEGER на TIMESTAMP не поддерживается SQLite. "
                                           f"Если возникнут ошибки, удалите файл {self.db_path} или смигрируйте вручную.")
                             # cursor.execute(f"ALTER TABLE {table} ALTER COLUMN {column} TYPE TIMESTAMP") # Это не сработает
                             # logger.info(f"Тип колонки {table}.{column} изменен на TIMESTAMP.")
                        except Exception as alter_err:
                             logger.error(f"Не удалось изменить тип колонки {table}.{column}: {alter_err}. Оставляем как есть.")
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка во время проверки/миграции схемы БД: {e}")


    def add_video_to_cache(self, url, title, file_path, size, video_format="high"):
        """Добавление видео в кэш с использованием datetime.now()"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now()
            cursor.execute(
                "INSERT OR REPLACE INTO video_cache (url, title, file_path, size, format, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (url, title, file_path, size, video_format, current_time)
            )
            conn.commit()

    def get_cached_video(self, url, video_format="high"):
        """Получение видео из кэша с проверкой срока годности через datetime"""
        if not config.CACHE_ENABLED:
            return None

        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Получаем объект datetime напрямую благодаря detect_types
            cursor.execute(
                "SELECT title, file_path, size, created_at FROM video_cache WHERE url = ? AND format = ?",
                (url, video_format)
            )
            result = cursor.fetchone()

        if not result:
            return None

        title, file_path, size, created_at = result

        # Убедимся, что created_at это datetime объект
        if not isinstance(created_at, datetime):
             # Попытка ручного парсинга, если detect_types не сработал
             try:
                 created_at = datetime.fromisoformat(str(created_at))
             except (TypeError, ValueError):
                 logger.error(f"Не удалось преобразовать created_at ('{created_at}') в datetime для кэша URL {url}")
                 self.remove_from_cache(url) # Удаляем запись с некорректной датой
                 return None

        # Проверяем срок хранения и существование файла
        cache_expiry_date = datetime.now() - timedelta(days=config.CACHE_DURATION)
        if created_at < cache_expiry_date or not os.path.exists(file_path):
            logger.info(f"Кэш для {url} устарел или файл не найден. Удаление из кэша.")
            self.remove_from_cache(url)
            return None

        return {
            "title": title,
            "file_path": file_path,
            "size": size
        }

    def remove_from_cache(self, url):
        """Удаление видео из кэша"""
        # Сначала получаем путь к файлу
        file_to_delete = None
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT file_path FROM video_cache WHERE url = ?", (url,))
                result = cursor.fetchone()
                if result:
                    file_to_delete = result[0]
                # Удаляем запись из БД
                cursor.execute("DELETE FROM video_cache WHERE url = ?", (url,))
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при удалении записи из кэша для URL {url}: {e}")

        # Удаляем файл, если путь был найден
        if file_to_delete and os.path.exists(file_to_delete):
            try:
                os.remove(file_to_delete)
                logger.info(f"Удален файл из кэша: {file_to_delete}")
            except Exception as e:
                logger.error(f"Не удалось удалить файл кэша {file_to_delete}: {e}")


    def clean_expired_cache(self):
        """Очистка просроченного кэша с использованием datetime"""
        expired_files = []
        expiry_date = datetime.now() - timedelta(days=config.CACHE_DURATION)
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                # Находим устаревшие записи
                cursor.execute("SELECT file_path FROM video_cache WHERE created_at < ?", (expiry_date,))
                expired_files = [row[0] for row in cursor.fetchall()]
                # Удаляем устаревшие записи из БД
                cursor.execute("DELETE FROM video_cache WHERE created_at < ?", (expiry_date,))
                deleted_count = cursor.rowcount
                conn.commit()
                if deleted_count > 0:
                     logger.info(f"Удалено {deleted_count} устаревших записей из кэша видео.")
        except Exception as e:
            logger.error(f"Ошибка при удалении устаревших записей из БД кэша: {e}")
            return # Не удаляем файлы, если была ошибка в БД

        # Удаляем файлы
        deleted_file_count = 0
        for file_path in expired_files:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    deleted_file_count += 1
                except Exception as e:
                    logger.error(f"Не удалось удалить просроченный файл кэша {file_path}: {e}")
        if deleted_file_count > 0:
             logger.info(f"Удалено {deleted_file_count} просроченных файлов кэша.")


    def update_user_stats(self, user_id, username):
        """Обновление статистики пользователя с использованием datetime и проверкой даты"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now()
            today_date = now.date()

            # Получаем текущую статистику пользователя (включая last_download как datetime)
            cursor.execute("SELECT user_id, downloads_today, total_downloads, last_download, video_format, notification_settings FROM user_stats WHERE user_id = ?", (user_id,))
            user_data = cursor.fetchone()

            if not user_data:
                # Создаем запись для нового пользователя
                logger.info(f"Создание записи для нового пользователя: ID={user_id}, Username={username}")
                default_settings = '{"download_complete": true, "download_error": true, "download_progress": true, "system_alert": true}'
                cursor.execute(
                    "INSERT INTO user_stats (user_id, username, downloads_today, total_downloads, last_download, video_format, notification_settings) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (user_id, username, 1, 1, now, "high", default_settings)
                )
            else:
                # Обновляем счетчики для существующего пользователя
                db_user_id, db_downloads_today, db_total_downloads, db_last_download, db_video_format, db_notification_settings = user_data

                # Проверяем, является ли db_last_download объектом datetime
                if db_last_download and isinstance(db_last_download, datetime):
                    last_download_date = db_last_download.date()
                else:
                     # Если дата некорректна или отсутствует, считаем, что это не сегодня
                    last_download_date = None

                # Сбрасываем счетчик downloads_today, если последняя загрузка была не сегодня
                if last_download_date != today_date:
                    downloads_today = 1
                else:
                    downloads_today = db_downloads_today + 1

                total_downloads = db_total_downloads + 1

                # Обновляем запись
                cursor.execute(
                    "UPDATE user_stats SET username = ?, downloads_today = ?, total_downloads = ?, last_download = ? WHERE user_id = ?",
                    (username, downloads_today, total_downloads, now, user_id)
                )
                logger.debug(f"Обновлена статистика для пользователя ID={user_id}: downloads_today={downloads_today}, total={total_downloads}")

            conn.commit()

    def log_download(self, user_id, url, status, error=None):
        """Логирование загрузки с использованием datetime.now()"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            current_time = datetime.now()
            cursor.execute(
                "INSERT INTO download_logs (user_id, url, status, error, created_at) VALUES (?, ?, ?, ?, ?)",
                (user_id, url, status, error, current_time)
            )
            conn.commit()
            logger.debug(f"Запись в лог: User={user_id}, URL={url}, Status={status}")

    def get_user_settings(self, user_id):
        """Получение настроек пользователя (формат видео)"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT video_format FROM user_stats WHERE user_id = ?", (user_id,))
            format_result = cursor.fetchone()
        # Возвращаем формат видео или формат по умолчанию
        return format_result[0] if format_result else "high"

    def update_user_settings(self, user_id, video_format):
        """Обновление настроек пользователя (формат видео)"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Используем INSERT OR IGNORE + UPDATE для атомарности
            cursor.execute("INSERT OR IGNORE INTO user_stats (user_id, video_format) VALUES (?, ?)", (user_id, video_format))
            cursor.execute("UPDATE user_stats SET video_format = ? WHERE user_id = ?", (video_format, user_id))
            conn.commit()
            logger.info(f"Обновлен формат видео для пользователя {user_id} на '{video_format}'")
        return video_format

    def check_download_limit(self, user_id):
        """Проверка лимита скачиваний для пользователя (с учетом сброса счетчика)"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Получаем счетчик и дату последней загрузки
            cursor.execute("SELECT downloads_today, last_download FROM user_stats WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()

        if not result:
             # Новый пользователь, лимит точно не превышен
            return True

        downloads_today, last_download = result
        now_date = datetime.now().date()

        # Проверяем, является ли last_download объектом datetime
        if last_download and isinstance(last_download, datetime):
            last_download_date = last_download.date()
        else:
            last_download_date = None # Считаем, что загрузок сегодня не было

        # Если последняя загрузка была не сегодня, лимит не превышен (счетчик будет сброшен при след. загрузке)
        if last_download_date != now_date:
            return True
        else:
            # Если загрузка была сегодня, проверяем счетчик
            return downloads_today < config.MAX_DOWNLOADS_PER_USER


    def get_notification_settings(self, user_id):
        """Получение настроек уведомлений пользователя"""
        # Используем стандартную реализацию context manager для соединения
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT notification_settings FROM user_stats WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()

        if result and result[0]:
            try:
                import json
                return json.loads(result[0])
            except (json.JSONDecodeError, TypeError):
                 logger.warning(f"Не удалось разобрать JSON настроек уведомлений для пользователя {user_id}. Возвращаем значения по умолчанию.")
                 return config.NOTIFICATION_SETTINGS.copy()
        # Если пользователя нет или настройки пустые, возвращаем дефолтные
        return config.NOTIFICATION_SETTINGS.copy()

    def update_notification_settings(self, user_id, settings):
        """Обновление настроек уведомлений пользователя"""
        try:
            import json
            settings_json = json.dumps(settings)
        except (TypeError, ValueError) as e:
            logger.error(f"Ошибка сериализации настроек уведомлений в JSON: {e}")
            return False

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                # Используем INSERT OR IGNORE + UPDATE
                cursor.execute("INSERT OR IGNORE INTO user_stats (user_id, notification_settings) VALUES (?, ?)", (user_id, settings_json))
                cursor.execute("UPDATE user_stats SET notification_settings = ? WHERE user_id = ?", (settings_json, user_id))
                conn.commit()
                logger.info(f"Обновлены настройки уведомлений для пользователя {user_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при обновлении настроек уведомлений в БД для пользователя {user_id}: {e}")
            return False

    def toggle_notification(self, user_id, notification_type):
        """Включение/выключение конкретного типа уведомлений"""
        settings = self.get_notification_settings(user_id)
        # Переключаем значение, учитывая, что ключа может не быть (по умолчанию True)
        settings[notification_type] = not settings.get(notification_type, True)
        return self.update_notification_settings(user_id, settings) 