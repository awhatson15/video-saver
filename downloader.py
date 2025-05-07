import os
import logging
import yt_dlp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import config
from database import Database
import uuid
import subprocess
import math
import json
import threading
import time
import aiofiles
import aiofiles.os
from urllib.parse import urlparse, urlunparse

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Пул для асинхронных задач
executor = ThreadPoolExecutor(max_workers=config.MAX_CONCURRENT_DOWNLOADS)

# Инициализация базы данных
db = Database()

# Активные загрузки (для отслеживания прогресса)
active_downloads = {}
# Словарь для сопоставления НОРМАЛИЗОВАННЫХ канонических URL с исходными URL
canonical_url_map = {}
# Блокировка для потокобезопасного доступа к словарям выше
data_lock = threading.Lock()

# --- Добавлено: Функция нормализации URL (дублируем из bot.py для автономности модуля) ---
def normalize_url(url):
    """Removes query parameters and fragments from a URL."""
    if not url: return None
    try:
        parsed = urlparse(url)
        # Reconstruct URL without query and fragment
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
    except Exception as e:
        logger.warning(f"(Downloader) Не удалось нормализовать URL '{url}': {e}")
        return url # Возвращаем исходный URL в случае ошибки
# --- Конец добавления ---

class VideoDownloader:
    def __init__(self):
        # Создаем директорию для загрузок, если её нет
        if not os.path.exists(config.DOWNLOAD_DIR):
            os.makedirs(config.DOWNLOAD_DIR)
    
    async def get_video_info(self, url):
        """Получение информации о видео без скачивания"""
        try:
            loop = asyncio.get_event_loop()
            ydl_opts = {
                'quiet': True,
                'format': 'best',  # Получаем все доступные форматы
                'listformats': True,  # Включаем список форматов
                'no_warnings': True,
                'extract_flat': False,  # Получаем полную информацию
                'nocheckcertificate': True,
                'ignoreerrors': True,
                'no_color': True,
                'geo_bypass': True,
                'geo_bypass_country': 'US',
                'source_address': '0.0.0.0',  # Используем любой доступный IP
                'socket_timeout': 15,  # Увеличиваем таймаут
                'extractor_retries': 5,  # Повторные попытки извлечения
                'extractor_args': {
                    'youtube': {
                        'player_client': ['android'],  # Эмуляция Android-клиента
                        'skip': ['hls', 'dash']  # Пропускаем некоторые проблемные форматы
                    }
                }
            }
            
            def _extract_info():
                # Пробуем разные методы последовательно
                errors = []
                
                # Метод 1: Стандартный способ
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=False)
                        if info is not None:
                            return info
                        errors.append("Стандартный метод вернул None")
                except Exception as e:
                    errors.append(f"Стандартный метод не сработал: {str(e)}")
                
                # Метод 2: Пробуем с другим user-agent
                try:
                    opts_with_agent = ydl_opts.copy()
                    opts_with_agent['user_agent'] = 'Mozilla/5.0 (Android 12; Mobile; rv:109.0) Gecko/113.0 Firefox/113.0'
                    with yt_dlp.YoutubeDL(opts_with_agent) as ydl:
                        info = ydl.extract_info(url, download=False)
                        if info is not None:
                            return info
                        errors.append("Метод с user-agent вернул None")
                except Exception as e:
                    errors.append(f"Метод с user-agent не сработал: {str(e)}")
                
                # Метод 3: Если это YouTube, пробуем через инвидиус
                if 'youtube.com' in url or 'youtu.be' in url:
                    video_id = None
                    if 'youtube.com/watch' in url and 'v=' in url:
                        video_id = url.split('v=')[1].split('&')[0]
                    elif 'youtu.be/' in url:
                        video_id = url.split('youtu.be/')[1].split('?')[0]
                    elif 'youtube.com/shorts/' in url:
                        video_id = url.split('shorts/')[1].split('?')[0]
                        
                    if video_id:
                        try:
                            # Пробуем через инвидиус
                            invidious_url = f"https://invidious.snopyta.org/watch?v={video_id}"
                            logger.info(f"Пробуем через инвидиус: {invidious_url}")
                            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                info = ydl.extract_info(invidious_url, download=False)
                                if info is not None:
                                    return info
                                errors.append("Метод через инвидиус вернул None")
                        except Exception as e:
                            errors.append(f"Метод через инвидиус не сработал: {str(e)}")

                        # Пробуем другие инвидиус-зеркала
                        try:
                            invidious_url2 = f"https://yewtu.be/watch?v={video_id}"
                            logger.info(f"Пробуем через yewtu.be: {invidious_url2}")
                            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                info = ydl.extract_info(invidious_url2, download=False)
                                if info is not None:
                                    return info
                                errors.append("Метод через yewtu.be вернул None")
                        except Exception as e:
                            errors.append(f"Метод через yewtu.be не сработал: {str(e)}")

                        # Попытка через NewPipe API
                        try:
                            piped_url = f"https://piped.kavin.rocks/watch?v={video_id}"
                            logger.info(f"Пробуем через Piped API: {piped_url}")
                            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                info = ydl.extract_info(piped_url, download=False)
                                if info is not None:
                                    return info
                                errors.append("Метод через Piped API вернул None")
                        except Exception as e:
                            errors.append(f"Метод через Piped API не сработал: {str(e)}")
                
                # Выбрасываем ошибку с деталями всех неудачных попыток
                err_msg = "\n".join(errors)
                logger.error(f"Все методы получения информации о видео для URL '{url}' не сработали:\n{err_msg}")
                raise ValueError(f"Не удалось получить информацию о видео после нескольких попыток:\n{err_msg}")
            
            info = await loop.run_in_executor(executor, _extract_info)
            
            # Проверяем, что info не None
            if info is None:
                logger.error("get_video_info: _extract_info вернул None вместо информации о видео")
                raise ValueError("Не удалось получить информацию о видео после нескольких попыток")
            
            # Получаем список доступных форматов
            formats = []
            if 'formats' in info:
                for f in info['formats']:
                    # Проверяем, что формат содержит видео
                    if f.get('vcodec') != 'none':
                        # Оцениваем размер файла, если он не указан
                        filesize = f.get('filesize')
                        if not filesize and f.get('tbr'):  # tbr - битрейт в кбит/с
                            # Оцениваем размер на основе битрейта и длительности
                            duration = info.get('duration', 0)
                            if duration > 0:
                                # Размер = битрейт * длительность / 8 (для байтов)
                                filesize = int(f['tbr'] * 1000 * duration / 8)
                        
                        format_info = {
                            'format_id': f.get('format_id'),
                            'ext': f.get('ext'),
                            'resolution': f.get('resolution', 'unknown'),
                            'height': f.get('height', 0),
                            'width': f.get('width', 0),
                            'filesize': filesize,
                            'fps': f.get('fps'),
                            'vcodec': f.get('vcodec'),
                            'acodec': f.get('acodec'),
                            'quality': f.get('quality', 0),
                            'tbr': f.get('tbr', 0)  # битрейт в кбит/с
                        }
                        formats.append(format_info)
            
            # Проверяем, есть ли у видео размер файла
            file_size = info.get('filesize')
            if not file_size and info.get('duration'):
                # Примерная оценка: 1 минута HD видео ~ 10MB
                estimated_size = (info.get('duration') / 60) * 10 * 1024 * 1024
                file_size = int(estimated_size)
            
            return {
                'title': info.get('title', 'Видео'),
                'filesize': file_size,
                'duration': info.get('duration'),
                'formats': formats,
                'thumbnail': info.get('thumbnail'),
                'url': url,
                'webpage_url': info.get('webpage_url', url)
            }
        except Exception as e:
            logger.error(f"Ошибка при получении информации о видео: {e}")
            raise
    
    def progress_hook(self, d):
        """Хук для отслеживания прогресса загрузки"""
        logger.debug(f"Progress hook called. Status: {d.get('status')}, Info: {d.get('info_dict', {}).get('webpage_url', 'N/A')}")
        if d['status'] == 'downloading':
            url = d.get('info_dict', {}).get('webpage_url')
            if not url:
                logger.warning("Progress hook: URL not found in info_dict.")
                return
            
            # --- Изменено: Ищем исходный URL по НОРМАЛИЗОВАННОМУ каноническому --- 
            normalized_canonical_hook_url = normalize_url(url)
            if not normalized_canonical_hook_url:
                 logger.warning(f"Progress hook: Failed to normalize canonical URL '{url}'. Skipping update.")
                 return
                 
            with data_lock:
                original_url = canonical_url_map.get(normalized_canonical_hook_url)
                if not original_url:
                    # Логируем как нормализованный, так и исходный канонический URL для отладки
                    logger.warning(f"Progress hook: Cannot find original_url for normalized canonical '{normalized_canonical_hook_url}' (from '{url}') in map. Map keys: {list(canonical_url_map.keys())}")
                    return
                    
                if original_url not in active_downloads:
                    # --- Изменено: Улучшаем логирование для этого случая ---
                    logger.warning(
                        f"Progress hook: State for original URL '{original_url}' "
                        f"(mapped from norm_canon '{normalized_canonical_hook_url}') not found in active_downloads. "
                        f"This might be due to a race condition if the download just finished or errored. "
                        f"Current active_downloads keys: {list(active_downloads.keys())}"
                    )
                    return
                
                # --- Перенесено внутрь блокировки: Расчет процента --- 
                downloaded = d.get('downloaded_bytes')
                if d.get('total_bytes'):
                    percent = d['downloaded_bytes'] / d['total_bytes'] * 100
                elif d.get('total_bytes_estimate'):
                    percent = d['downloaded_bytes'] / d['total_bytes_estimate'] * 100
                else:
                    percent = 0 # Если общий размер неизвестен
                percent_rounded = round(percent)
                # --- Конец переноса --- 
                
                # Обновляем только данные о прогрессе в существующей записи по ИСХОДНОМУ URL
                active_downloads[original_url].update({
                    'percent': round(percent, 1), # Теперь percent доступен
                    'percent_rounded': percent_rounded, # Теперь percent_rounded доступен
                    'downloaded_bytes': downloaded, # Используем переменную
                    'speed': d.get('speed', 0),
                    'eta': d.get('eta', 0),
                    'filename': d.get('filename') # Обновляем имя файла здесь тоже
                })
                logger.debug(f"Progress hook update for URL '{original_url}' (canonical: '{url}'): Percent={percent_rounded}, Downloaded={downloaded}, Total={d.get('total_bytes') or d.get('total_bytes_estimate', 'N/A')}")
            # --- Конец изменений ---
        
        elif d['status'] == 'finished':
            url = d.get('info_dict', {}).get('webpage_url')
            if url:
                # --- Изменено: Используем блокировку и НОРМАЛИЗОВАННЫЙ URL ---
                normalized_canonical_hook_url = normalize_url(url)
                if not normalized_canonical_hook_url:
                     logger.warning(f"Progress hook (finished): Failed to normalize canonical URL '{url}'. Skipping final update.")
                     return
                     
                with data_lock:
                    original_url = canonical_url_map.get(normalized_canonical_hook_url)
                    if original_url and original_url in active_downloads:
                        active_downloads[original_url]['percent'] = 100
                        active_downloads[original_url]['percent_rounded'] = 100
                        # Финальное имя файла важно обновить здесь
                        active_downloads[original_url]['filename'] = d.get('filename') 
                        logger.debug(f"Progress hook marked finished for URL '{original_url}' (canonical: '{url}'). Filename: {d.get('filename')}")
                # --- Конец изменений ---

    async def download_video(self, url, format_id=None, user_id=None, chat_id=None, message_id=None):
        """Скачивание видео асинхронно"""
        try:
            # Проверяем наличие директории для скачивания
            if not os.path.exists(config.DOWNLOAD_DIR):
                os.makedirs(config.DOWNLOAD_DIR)
            
            # Создаем временную директорию для этого пользователя
            user_dir = os.path.join(config.DOWNLOAD_DIR, f"user_{user_id}" if user_id else "anonymous")
            if not os.path.exists(user_dir):
                os.makedirs(user_dir)
            
            # Проверяем кэш, если включен
            cached_video = None
            if config.CACHE_ENABLED:
                cached_video = self.db.get_cached_video(url, format_id)
                if cached_video and os.path.exists(cached_video['file_path']):
                    logger.info(f"Используем кэшированное видео: {cached_video['file_path']}")
                    if user_id:
                        self.db.log_download(user_id, url, "success_cache")
                    return cached_video

            # Если нет в кэше, то скачиваем
            logger.info(f"Attempting download with format: {format_id}")
            ydl_opts = self.get_ydl_options(format_id, user_dir, url)
            
            # Добавляем обработчик прогресса, если указан message_id
            if chat_id and message_id:
                with data_lock:
                    active_downloads[url]['chat_id'] = chat_id
                    active_downloads[url]['message_id'] = message_id
                
                ydl_opts['progress_hooks'] = [
                    lambda d: self._progress_hook(d, url)
                ]

            # Запускаем процесс скачивания
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(None, self._download_with_ydl, ydl_opts, url)
            
            if not info or 'filepath' not in info:
                raise ValueError(f"Не удалось получить информацию о загруженном файле для URL: {url}")
            
            file_path = info['filepath']
            # Проверяем, что файл имеет расширение
            _, file_ext = os.path.splitext(file_path)
            if not file_ext:
                # Если нет расширения, добавляем .mp4 и переименовываем файл
                new_file_path = file_path + '.mp4'
                os.rename(file_path, new_file_path)
                file_path = new_file_path
                logger.info(f"Добавлено расширение к файлу: {file_path}")
            
            # Проверяем существование файла
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Файл не найден после скачивания: {file_path}")
                
            # Получаем размер файла
            file_size = os.path.getsize(file_path)
            
            # Добавляем видео в кэш (если включено и успешно скачано)
            if config.CACHE_ENABLED:
                # Убедимся, что info не None перед доступом к title
                cache_title = info.get('title', 'Видео') if info else 'Видео'
                self.db.add_video_to_cache(url, cache_title, file_path, file_size, format_id)
            
            if user_id:
                self.db.log_download(user_id, url, "success_download")
            
            # Возвращаем результат (убедимся, что info есть)
            final_title = info.get('title', 'Видео') if info else 'Видео'
            return {
                'title': final_title,
                'file_path': file_path,
                'size': file_size
            }
            
        except Exception as e:
            # Логируем ошибку и пробрасываем дальше
            logger.error(f"Ошибка при скачивании видео '{url}' (в download_video): {e}")
            if user_id:
                self.db.log_download(user_id, url, "error", str(e))
            raise

        finally:
             # --- Блок finally остается прежним для очистки --- 
            # Гарантированно удаляем запись из активных загрузок и маппинга
            download_info = None
            final_filename = None
            canonical_url_to_remove = None
            with data_lock:
                if url in active_downloads:
                    download_info = active_downloads.pop(url) # Удаляем сразу под блокировкой
                    final_filename = download_info.get('filename')
                    canonical_url_to_remove = download_info.get('canonical_url')
                    
                    if canonical_url_to_remove and canonical_url_to_remove in canonical_url_map:
                        del canonical_url_map[canonical_url_to_remove]
                        logger.debug(f"Removed mapping for {canonical_url_to_remove}. Current map: {canonical_url_map}")
            # --- Конец изменений --- 
            
            # --- Изменено: Асинхронное удаление файла --- 
            if final_filename and not config.CACHE_ENABLED:
                 try:
                     if await aiofiles.os.path.exists(final_filename):
                         await aiofiles.os.remove(final_filename)
                         logger.info(f"Удален исходный файл (кэш отключен): {final_filename}")
                 except OSError as rm_err:
                     logger.warning(f"Не удалось удалить исходный файл {final_filename}: {rm_err}")
            # --- Конец изменений ---

    def get_download_progress(self, url):
        """Возвращает прогресс загрузки для указанного URL"""
        progress_data = active_downloads.get(url)
        logger.debug(f"get_download_progress called for URL '{url}'. Returning: {progress_data}")
        return progress_data

    def cancel_download(self, url):
        """Отменяет загрузку для указанного URL и удаляет временный файл."""
        # --- Изменено: Используем блокировку и пытаемся убить процесс --- 
        process_to_terminate = None
        filename_to_delete = None
        canonical_url_to_remove = None
        cancelled_in_dict = False

        with data_lock:
            if url in active_downloads:
                logger.info(f"Attempting to cancel download for URL: {url}")
                download_info = active_downloads.pop(url) # Удаляем из словаря под блокировкой
                cancelled_in_dict = True
                filename_to_delete = download_info.get('filename')
                canonical_url_to_remove = download_info.get('canonical_url')
                process_to_terminate = download_info.get('process') # Получаем Popen объект
                
                if canonical_url_to_remove and canonical_url_to_remove in canonical_url_map:
                    del canonical_url_map[canonical_url_to_remove]
                    logger.debug(f"Removed mapping for {canonical_url_to_remove} during cancel. Current map: {canonical_url_map}")
            else:
                 logger.warning(f"Attempted to cancel URL not found in active downloads: {url}")
        
        # Завершаем процесс и удаляем файл вне блокировки
        if process_to_terminate:
             try:
                 logger.info(f"Terminating process for URL {url} (PID: {process_to_terminate.pid})")
                 process_to_terminate.terminate() # Отправляем SIGTERM
                 # Можно добавить ожидание и kill, если terminate не сработает
                 # try:
                 #     process_to_terminate.wait(timeout=2) 
                 # except subprocess.TimeoutExpired:
                 #     logger.warning(f"Process {process_to_terminate.pid} did not terminate gracefully, killing.")
                 #     process_to_terminate.kill()
             except Exception as term_err:
                 logger.error(f"Error terminating process {process_to_terminate.pid} for URL {url}: {term_err}")
        else:
            # Это может случиться, если отмена вызвана до того, как процесс был сохранен
            if cancelled_in_dict: # Логируем, только если запись была найдена
                 logger.warning(f"Could not find process object to terminate for cancelled URL {url}")

        # --- Удаление файла (остается как было) --- 
        if filename_to_delete:
            # Пытаемся удалить файл, связанный с отмененной загрузкой
            # Даем небольшую паузу, чтобы файл мог освободиться, если он еще используется
            time.sleep(0.5) # Используем time.sleep
            try:
                # --- Изменено: Асинхронное удаление файла --- 
                # Используем синхронные операции, т.к. cancel_download не async
                # TODO: Сделать cancel_download асинхронной для использования aiofiles
                if os.path.exists(filename_to_delete):
                    os.remove(filename_to_delete)
                    logger.info(f"Deleted cancelled download file: {filename_to_delete}")
                else:
                    logger.warning(f"Cancelled download file not found: {filename_to_delete}")
                # --- Конец изменений (оставили синхронный вариант) --- 
            except OSError as rm_err:
                logger.error(f"Failed to delete cancelled download file {filename_to_delete}: {rm_err}")
        else:
            if cancelled_in_dict: # Логируем, только если запись была найдена
                logger.warning(f"Filename not found for cancelled download URL: {url}")

        return cancelled_in_dict # Возвращаем True, если запись была найдена и удалена из словаря
        # --- Конец изменений --- 

    async def split_large_video(self, file_path, max_segment_size=35):
        """
        Разделяет большие видео на части с использованием FFmpeg (асинхронные операции с файлами).
        Удален аварийный режим разделения по байтам.
        
        Args:
            file_path: Путь к исходному видео
            max_segment_size: Максимальный размер сегмента в MB (по умолчанию 35MB)
            
        Returns:
            list: Список путей к созданным файлам частей
        """
        output_files = []
        temp_files = []

        # --- Изменено: Асинхронная проверка файла --- 
        if not await aiofiles.os.path.exists(file_path):
            logger.error(f"Файл для разделения не найден: {file_path}")
            return []

        try:
            # 1. Получаем информацию о видео (длительность) через ffprobe
            cmd_probe = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_format', '-show_streams', file_path
            ]
            try:
                result = subprocess.run(
                    cmd_probe, capture_output=True, text=True,
                    encoding='utf-8', errors='ignore', check=True
                )
                ffprobe_output = result.stdout
            except (subprocess.CalledProcessError, FileNotFoundError) as ff_err:
                logger.error(f"Ошибка при выполнении ffprobe для {file_path}: {ff_err}")
                raise ValueError(f"Не удалось получить информацию о видео {file_path}") from ff_err
            except Exception as ff_generic_err:
                logger.error(f"Неожиданная ошибка при вызове ffprobe для {file_path}: {ff_generic_err}")
                raise ValueError(f"Не удалось получить информацию о видео {file_path}") from ff_generic_err

            duration_seconds = None
            if ffprobe_output:
                try:
                    info = json.loads(ffprobe_output)
                    if 'format' in info and 'duration' in info['format']:
                        duration_seconds = float(info['format']['duration'])
                    elif 'streams' in info and len(info['streams']) > 0 and 'duration' in info['streams'][0]:
                        duration_seconds = float(info['streams'][0]['duration'])
                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as json_err:
                    logger.error(f"Ошибка при разборе JSON от ffprobe для {file_path}: {json_err}")
                    # Ошибка разбора JSON не критична, если можем получить длительность иначе

            # Если из JSON не вышло, пробуем напрямую
            if not duration_seconds:
                cmd_duration = [
                    'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                    '-of', 'default=noprint_wrappers=1:nokey=1', file_path
                ]
                try:
                    result_duration = subprocess.run(
                        cmd_duration, capture_output=True, text=True,
                        encoding='utf-8', errors='ignore', check=True
                    )
                    if result_duration.stdout:
                        duration_seconds = float(result_duration.stdout.strip())
                except Exception as dur_err:
                    logger.warning(f"Не удалось получить длительность через format=duration для {file_path}: {dur_err}")

            # Если длительность все еще неизвестна, разделение невозможно
            if not duration_seconds or duration_seconds <= 0:
                logger.error(f"Не удалось определить валидную продолжительность для {file_path}. Разделение невозможно.")
                raise ValueError(f"Не удалось определить продолжительность видео {file_path}")

            # --- Изменено: Асинхронное получение размера --- 
            stat_result = await aiofiles.os.stat(file_path)
            file_size_mb = stat_result.st_size / (1024 * 1024)
            if file_size_mb <= max_segment_size:
                logger.info(f"Файл {file_path} ({file_size_mb:.2f}MB) не требует разделения.")
                return [file_path]

            logger.info(f"Начинаем разделение файла {file_path} ({file_size_mb:.2f}MB)")

            # 3. Вычисляем параметры разделения
            segment_count = math.ceil(file_size_mb / max_segment_size)
            # Убедимся, что segment_duration не слишком мало
            segment_duration = max(1.0, duration_seconds / segment_count)

            base_name, ext = os.path.splitext(file_path)
            temp_base = os.path.join(os.path.dirname(file_path), str(uuid.uuid4()))

            # 4. Создаем каждый сегмент с помощью ffmpeg
            for i in range(segment_count):
                start_time = i * segment_duration
                # Для последнего сегмента можно не указывать -t, чтобы захватить остаток
                duration_arg = ['-t', str(segment_duration)]
                if i == segment_count - 1:
                    duration_arg = []

                segment_temp_file = f"{temp_base}_part{i+1}{ext}"
                original_segment_file = f"{base_name}_part{i+1}{ext}"
                temp_files.append(segment_temp_file)

                cmd_ffmpeg = [
                    'ffmpeg', '-hide_banner', '-loglevel', 'error',
                    '-i', file_path,
                    '-ss', str(start_time),
                ] + duration_arg + [
                    '-c', 'copy',
                    '-avoid_negative_ts', 'make_zero',
                    '-y',
                    segment_temp_file
                ]

                try:
                    logger.info(f"Создание сегмента {i+1}/{segment_count}...")
                    subprocess.run(cmd_ffmpeg, check=True, capture_output=True)

                    # --- Изменено: Асинхронные операции с файлами --- 
                    segment_exists = await aiofiles.os.path.exists(segment_temp_file)
                    segment_size = 0
                    if segment_exists:
                         segment_stat = await aiofiles.os.stat(segment_temp_file)
                         segment_size = segment_stat.st_size
                         
                    if segment_exists and segment_size > 0:
                        if await aiofiles.os.path.exists(original_segment_file):
                            await aiofiles.os.remove(original_segment_file)
                        await aiofiles.os.rename(segment_temp_file, original_segment_file)
                        output_files.append(original_segment_file)
                        if segment_temp_file in temp_files: temp_files.remove(segment_temp_file) # Удаление из списка - синхронное
                        logger.info(f"Сегмент {i+1} успешно создан: {original_segment_file}")
                    else:
                        logger.error(f"FFmpeg завершился, но сегмент {segment_temp_file} не создан или пуст.")
                    # --- Конец изменений --- 

                except subprocess.CalledProcessError as ffmpeg_err:
                    logger.error(f"Ошибка FFmpeg при создании сегмента {i+1}: {ffmpeg_err}")
                    logger.error(f"FFmpeg stderr: {ffmpeg_err.stderr.decode('utf-8', 'ignore')}")
                except Exception as rename_err:
                    logger.error(f"Ошибка при переименовании сегмента {i+1}: {rename_err}")

            # Проверяем, созданы ли какие-либо файлы
            if not output_files:
                logger.error(f"Не удалось создать ни одного сегмента для {file_path}.")
                raise RuntimeError(f"Не удалось разделить видео {file_path}")

            return output_files

        except Exception as e:
            logger.error(f"Общая ошибка при разделении видео {file_path}: {e}")
            # Удаляем все успешно созданные выходные файлы, так как процесс не завершился полностью
            for f in output_files:
                try:
                    if await aiofiles.os.path.exists(f): await aiofiles.os.remove(f)
                except OSError: pass
            return []

        finally:
            # --- Изменено: Асинхронное удаление файлов --- 
            for temp_f in temp_files:
                try:
                    if await aiofiles.os.path.exists(temp_f):
                        await aiofiles.os.remove(temp_f)
                        logger.debug(f"Удален временный файл сегмента: {temp_f}")
                except OSError as rm_err:
                    logger.warning(f"Не удалось удалить временный файл сегмента {temp_f}: {rm_err}")
            # --- Конец изменений --- 

    async def get_optimal_quality(self, url, user_id=None):
        """Определяет оптимальное качество видео на основе доступных форматов и ограничений"""
        try:
            info = await self.get_video_info(url)
            formats = info.get('formats', [])
            
            if not formats:
                return "high"  # Возвращаем высокое качество по умолчанию
            
            # Сортируем форматы по качеству, безопасно обрабатывая None
            formats.sort(key=lambda x: x.get('quality') or 0, reverse=True)
            
            # Получаем настройки пользователя
            if user_id:
                user_format = db.get_user_settings(user_id)
            else:
                user_format = "high"
            
            # Определяем оптимальное качество
            if user_format == "low":
                return "low"
            elif user_format == "medium":
                return "medium"
            elif user_format == "high":
                # Проверяем, есть ли формат с разрешением 1080p или выше
                for f in formats:
                    if '1080' in f.get('resolution', ''):
                        return "high"
                return "medium"  # Если нет 1080p, возвращаем среднее качество
            else:
                return "high"
                
        except Exception as e:
            logger.error(f"Ошибка при определении оптимального качества: {e}")
            return "high"  # Возвращаем высокое качество по умолчанию в случае ошибки 

    async def get_playlist_info(self, playlist_url):
        """Получение информации о плейлисте (название, список видео URL)."""
        try:
            loop = asyncio.get_event_loop()
            ydl_opts = {
                'quiet': True,
                'extract_flat': 'in_playlist', # Получаем только базовую информацию о видео в плейлисте
                'force_generic_extractor': False,
                # Добавляем параметры обхода блокировок
                'nocheckcertificate': True,
                'ignoreerrors': True,
                'no_color': True,
                'geo_bypass': True,
                'geo_bypass_country': 'US',
                'source_address': '0.0.0.0',
                'socket_timeout': 15,
                'extractor_retries': 5,
                'extractor_args': {
                    'youtube': {
                        'player_client': ['android'],
                        'skip': ['hls', 'dash']
                    }
                }
            }

            def _extract_playlist_info():
                # Метод 1: Стандартный способ
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(playlist_url)
                        if info is None:
                            logger.warning(f"Стандартный метод получения плейлиста вернул None для {playlist_url}")
                            raise ValueError("Получен None от стандартного метода")
                        return info
                except Exception as e:
                    logger.warning(f"Standard playlist extraction failed: {e}")
                    
                # Метод 2: С другим user-agent
                try:
                    agent_opts = ydl_opts.copy()
                    agent_opts['user_agent'] = 'Mozilla/5.0 (Android 12; Mobile; rv:109.0) Gecko/113.0 Firefox/113.0'
                    with yt_dlp.YoutubeDL(agent_opts) as ydl:
                        info = ydl.extract_info(playlist_url)
                        if info is None:
                            logger.warning(f"User-agent метод получения плейлиста вернул None для {playlist_url}")
                            raise ValueError("Получен None от user-agent метода")
                        return info
                except Exception as e:
                    logger.warning(f"User-agent playlist method failed: {e}")
                    
                # Метод 3: Через альтернативный фронтенд, если это YouTube плейлист
                if 'youtube.com/playlist' in playlist_url and 'list=' in playlist_url:
                    playlist_id = playlist_url.split('list=')[1].split('&')[0]
                    
                    # Пробуем разные инвидиус-зеркала
                    for invidious_domain in ["invidious.snopyta.org", "yewtu.be", "piped.kavin.rocks", "inv.riverside.rocks"]:
                        try:
                            invidious_url = f"https://{invidious_domain}/playlist?list={playlist_id}"
                            logger.info(f"Trying playlist via {invidious_domain}: {invidious_url}")
                            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                info = ydl.extract_info(invidious_url)
                                if info is None:
                                    logger.warning(f"Метод через {invidious_domain} для плейлиста вернул None")
                                    continue
                                return info
                        except Exception as e:
                            logger.warning(f"Метод через {invidious_domain} для плейлиста не сработал: {e}")
                
                # Если все методы не сработали, повторно вызываем исключение
                logger.error(f"Все методы получения информации о плейлисте {playlist_url} не сработали")
                raise ValueError(f"Failed to extract playlist info after multiple attempts for {playlist_url}")
            
            info = await loop.run_in_executor(executor, _extract_playlist_info)
            
            # Проверка на None
            if info is None:
                logger.error(f"get_playlist_info: _extract_playlist_info вернул None для {playlist_url}")
                raise ValueError(f"Не удалось получить информацию о плейлисте {playlist_url}")
            
            entries = []
            if info and 'entries' in info:
                for entry in info.get('entries', []):
                    # Пропускаем недоступные видео (например, удаленные)
                    if entry and entry.get('url'):
                         entries.append({
                             'url': entry['url'],
                             'title': entry.get('title', 'Видео без названия')
                         })
            
            return {
                'title': info.get('title', 'Плейлист без названия'),
                'entries': entries
            }

        except yt_dlp.utils.DownloadError as e:
            # Обрабатываем случай, если плейлист не найден или недоступен
             if "This playlist does not exist or is private" in str(e) or "confirm your age" in str(e):
                  logger.warning(f"Ошибка получения информации о плейлисте {playlist_url}: {e}")
                  raise ValueError(f"Плейлист не найден или недоступен: {playlist_url}") from e
             else:
                  logger.error(f"Ошибка yt-dlp при получении информации о плейлисте {playlist_url}: {e}")
                  raise # Перебрасываем другие ошибки yt-dlp
        except Exception as e:
            logger.error(f"Неожиданная ошибка при получении информации о плейлисте {playlist_url}: {e}")
            raise 