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
import aiohttp

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
        # Добавляем ссылку на объект базы данных
        self.db = db
    
    async def get_video_info(self, url: str, ydl_opts: dict = None) -> dict:
        """Получает информацию о видео со всеми доступными форматами."""
        try:
            # Базовые опции для надежного получения информации о видео
            base_opts = {
                'quiet': True,
                'no_warnings': True,
                'ignoreerrors': True,
                'noplaylist': True,
                'nocheckcertificate': True,
                'skip_download': True,
                # Не указываем конкретный формат, чтобы получить все доступные
                'format': None
            }
            
            # Объединяем базовые опции с переданными
            final_opts = {**base_opts, **(ydl_opts or {})}
            
            # Выводим опции в лог для отладки
            logger.info(f"Опции yt-dlp для получения информации о видео: {final_opts}")
            
            try:
                # Попытка 1: Стандартный метод
                with yt_dlp.YoutubeDL(final_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    
                    if info and 'formats' in info:
                        logger.info(f"Получено {len(info['formats'])} форматов для видео {url} (стандартный метод)")
                        
                        # Логируем доступные разрешения
                        heights = set(fmt.get('height', 0) for fmt in info['formats'] if fmt.get('vcodec') != 'none')
                        logger.info(f"Доступные разрешения: {sorted(list(heights), reverse=True)}")
                        
                        # Если видеоформатов нет, попробуем другой метод
                        if not heights or max(heights, default=0) <= 360:
                            logger.warning("Не получены форматы высокого разрешения, пробуем альтернативный метод")
                            raise ValueError("Недостаточно форматов")
                        
                        return info
                
            except Exception as e:
                # Логируем ошибку первого метода, но не выходим из функции
                logger.warning(f"Ошибка при стандартном методе получения форматов: {e}")
            
            # Попытка 2: Используем другие параметры
            try:
                alt_opts = {
                    'quiet': True,
                    'no_warnings': True,
                    'ignoreerrors': True,
                    'noplaylist': True,
                    'nocheckcertificate': True,
                    'skip_download': True,
                    'youtube_include_dash_manifest': True,
                    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                }
                
                with yt_dlp.YoutubeDL(alt_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    
                    if info and 'formats' in info:
                        logger.info(f"Получено {len(info['formats'])} форматов для видео {url} (альтернативный метод)")
                        
                        # Логируем доступные разрешения
                        heights = set(fmt.get('height', 0) for fmt in info['formats'] if fmt.get('vcodec') != 'none')
                        logger.info(f"Доступные разрешения: {sorted(list(heights), reverse=True)}")
                        
                        return info
                
            except Exception as e:
                logger.warning(f"Ошибка при альтернативном методе получения форматов: {e}")
                
            # Попытка 3: Используем YouTube DL Raw
            try:
                raw_opts = {
                    'quiet': True,
                    'no_warnings': True,
                    'youtube_include_dash_manifest': True,
                    'extractor_args': {'youtube': {'skip': ['dash', 'hls']}},
                }
                
                with yt_dlp.YoutubeDL(raw_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    
                    if info and 'formats' in info:
                        logger.info(f"Получено {len(info['formats'])} форматов для видео {url} (метод YouTube Raw)")
                        
                        # Логируем доступные разрешения
                        heights = set(fmt.get('height', 0) for fmt in info['formats'] if fmt.get('vcodec') != 'none')
                        logger.info(f"Доступные разрешения: {sorted(list(heights), reverse=True)}")
                        
                        return info
                
            except Exception as e:
                logger.error(f"Ошибка при YouTube Raw методе получения форматов: {e}")
                
            # Если все методы не сработали, возвращаем пустой словарь
            logger.error(f"Не удалось получить информацию о видео {url} ни одним из методов")
            return {}
            
        except Exception as e:
            logger.error(f"Критическая ошибка при получении информации о видео {url}: {e}")
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

    async def download_video(self, url, format_id, user_id, chat_id=None, message_id=None, ydl_opts=None):
        """Скачивает видео с указанным форматом."""
        try:
            # Создаем директорию для пользователя, если она не существует
            user_dir = os.path.join(config.DOWNLOAD_DIR, f"user_{user_id}")
            os.makedirs(user_dir, exist_ok=True)
            
            # Базовые опции для yt-dlp
            base_opts = {
                'outtmpl': os.path.join(user_dir, '%(title)s-%(id)s.%(ext)s'),
                'quiet': True,
                'no_warnings': True,
                'progress_hooks': [lambda d: self._progress_hook(d, url)],
                'restrictfilenames': True,
                'no_color': True
            }
            
            # Добавляем ID пользователя в опции
            base_opts['user_id'] = user_id
            
            # Опции для формата (если не переданы в ydl_opts)
            if not ydl_opts or 'format' not in ydl_opts:
                if format_id == 'audio':
                    format_opts = {'format': 'bestaudio/best'}
                elif format_id == 'auto' or format_id == 'best':
                    format_opts = {'format': 'best'}
                else:
                    format_opts = {'format': format_id}
                
                # Объединяем базовые опции с опциями формата
                final_opts = {**base_opts, **format_opts}
            else:
                # Если переданы собственные опции для yt-dlp, используем их
                # но предварительно обеспечиваем сохранение базовых настроек
                final_opts = {**base_opts, **ydl_opts}
            
            # Логируем опции для отладки
            logger.info(f"Опции для yt-dlp: {final_opts}")
            
            # Добавляем информацию о загрузке в активные загрузки
            with data_lock:
                if url not in active_downloads:
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
            
            # Скачиваем видео
            with yt_dlp.YoutubeDL(final_opts) as ydl:
                # Получаем информацию о видео
                info = ydl.extract_info(url, download=False)
                
                # Сохраняем канонический URL
                with data_lock:
                    if url in active_downloads and 'webpage_url' in info:
                        # Преобразуем канонический URL в ключ для карты
                        canonical_url = info['webpage_url']
                        normalized_canonical = normalize_url(canonical_url)
                        if normalized_canonical:
                            # Сохраняем соответствие нормализованного канонического URL оригинальному
                            canonical_url_map[normalized_canonical] = url
                            # Сохраняем канонический URL в информации о загрузке
                            active_downloads[url]['canonical_url'] = canonical_url
                
                # Скачиваем видео
                if 'webpage_url' in info:
                    logger.info(f"Начинаем загрузку видео с URL {info['webpage_url']}")
                    # Сначала проверяем, не отменена ли загрузка
                    with data_lock:
                        if url in active_downloads and active_downloads[url]['cancelled']:
                            logger.info(f"Загрузка для {url} была отменена перед началом скачивания")
                            return {
                                'success': False,
                                'error': 'Загрузка была отменена пользователем'
                            }
                
                # Начинаем загрузку
                ydl.download([url])
                
                # Получаем имя файла
                filename = ydl.prepare_filename(info)
                
                # Скачиваем превью, если оно доступно
                thumbnail_path = None
                if 'thumbnail' in info:
                    try:
                        thumbnail_url = info['thumbnail']
                        thumbnail_path = os.path.join(user_dir, f"{os.path.basename(filename)}.jpg")
                        
                        async with aiohttp.ClientSession() as session:
                            async with session.get(thumbnail_url) as response:
                                if response.status == 200:
                                    with open(thumbnail_path, 'wb') as f:
                                        f.write(await response.read())
                    except Exception as e:
                        logger.error(f"Ошибка при скачивании превью: {e}")
                
                # Удаляем информацию о загрузке
                with data_lock:
                    if url in active_downloads:
                        del active_downloads[url]
                
                return {
                    'success': True,
                    'filename': filename,
                    'thumbnail': thumbnail_path,
                    'title': info.get('title', ''),
                    'duration': info.get('duration', 0),
                    'format': format_id
                }
        
        except Exception as e:
            logger.error(f"Ошибка при скачивании видео {url}: {e}")
            
            # Удаляем информацию о загрузке
            with data_lock:
                if url in active_downloads:
                    del active_downloads[url]
            
            return {
                'success': False,
                'error': str(e)
            }

    def get_download_progress(self, url):
        """Возвращает прогресс загрузки для указанного URL"""
        progress_data = active_downloads.get(url)
        logger.debug(f"get_download_progress called for URL '{url}'. Returning: {progress_data}")
        return progress_data

    def cancel_download(self, url):
        """Отменяет загрузку для указанного URL."""
        try:
            with data_lock:
                if url in active_downloads:
                    # Помечаем загрузку как отмененную
                    active_downloads[url]['cancelled'] = True
                    active_downloads[url]['status'] = 'cancelled'
                    logger.info(f"Загрузка {url} помечена как отмененная")
                    return True
                else:
                    logger.warning(f"URL {url} не найден в активных загрузках")
                    return False
        except Exception as e:
            logger.error(f"Ошибка при отмене загрузки {url}: {e}")
            return False

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

    def _progress_hook(self, d, url):
        """Хук для отслеживания прогресса загрузки."""
        try:
            # Получаем информацию о загрузке
            with data_lock:
                if url not in active_downloads:
                    logger.warning(f"URL {url} не найден в активных загрузках")
                    return
                
                download_info = active_downloads[url]
                
                # Проверяем, не отменена ли загрузка
                if download_info.get('cancelled', False):
                    logger.info(f"Загрузка {url} была отменена")
                    return
                
                # Обновляем данные о прогрессе
                if d['status'] == 'downloading':
                    if 'downloaded_bytes' in d:
                        download_info['downloaded_bytes'] = d['downloaded_bytes']
                    if 'total_bytes' in d:
                        download_info['total_bytes'] = d['total_bytes']
                    elif 'total_bytes_estimate' in d:
                        download_info['total_bytes'] = d['total_bytes_estimate']
                    
                    if 'speed' in d and d['speed']:
                        download_info['speed'] = d['speed']
                    if 'eta' in d:
                        download_info['eta'] = d['eta']
                    
                    if download_info['total_bytes'] > 0:
                        download_info['percent'] = (download_info['downloaded_bytes'] / download_info['total_bytes']) * 100
                        download_info['percent_rounded'] = round(download_info['percent'])
                    
                    download_info['status'] = 'downloading'
                    download_info['filename'] = d.get('filename')
                    
                    # Сохраняем ID пользователя, если он есть в данных
                    if 'user_id' in d:
                        download_info['user_id'] = d['user_id']
                
                elif d['status'] == 'finished':
                    download_info['status'] = 'finished'
                    download_info['percent'] = 100
                    download_info['percent_rounded'] = 100
                    
                    # Сохраняем имя файла
                    if 'filename' in d:
                        download_info['filename'] = d['filename']
                
                active_downloads[url] = download_info
                
                # Логируем прогресс
                logger.debug(f"Прогресс загрузки {url}: {download_info['percent_rounded']}% ({download_info['downloaded_bytes']}/{download_info['total_bytes']})")
        
        except Exception as e:
            logger.error(f"Ошибка в _progress_hook для {url}: {e}")

    def get_ydl_options(self, format_id, output_dir, url):
        """Создает словарь с настройками для yt-dlp на основе запрошенного формата"""
        # Определяем формат для yt-dlp на основе format_id
        format_specifier = None
        if format_id and format_id.isdigit():
            format_specifier = f"{format_id}+bestaudio/bestaudio[ext=m4a]/{format_id}"
        elif format_id == 'auto':
            format_specifier = config.DEFAULT_VIDEO_FORMAT
        else:
            format_specifier = config.VIDEO_FORMATS.get(format_id, config.DEFAULT_VIDEO_FORMAT)
        
        # Выходной шаблон имени файла
        output_template = os.path.join(output_dir, '%(title)s.%(ext)s')
        
        # Создаем словарь с настройками
        ydl_opts = {
            'format': format_specifier,
            'outtmpl': output_template,
            'quiet': True,
            'format_sort': ['res', 'ext:mp4:m4a'],
            'format_preference': ['mp4', 'm4a'],
            'merge_output_format': 'mp4',
            'file_access_retries': 10,
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
        
        # Настройки постобработки
        if format_id != 'audio':
            ydl_opts['postprocessors'] = [{'key': 'FFmpegVideoConvertor', 'preferedformat': 'mp4'}]
        else:
            ydl_opts['postprocessors'] = []
            ydl_opts['merge_output_format'] = None
        
        return ydl_opts
    
    def _download_with_ydl(self, ydl_opts, url):
        """Скачивает видео с использованием yt-dlp и возвращает информацию о нем"""
        try:
            logger.info(f"Скачивание видео: {url}, формат: {ydl_opts.get('format')}")
            
            # Метод 1: Стандартный способ
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if info:
                    filepath = ydl.prepare_filename(info)
                    return {'title': info.get('title', 'Видео'), 'filepath': filepath}
            
            # Если стандартный метод не сработал, пробуем другой user-agent
            agent_opts = ydl_opts.copy()
            agent_opts['user_agent'] = 'Mozilla/5.0 (Android 12; Mobile; rv:109.0) Gecko/113.0 Firefox/113.0'
            with yt_dlp.YoutubeDL(agent_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if info:
                    filepath = ydl.prepare_filename(info)
                    return {'title': info.get('title', 'Видео'), 'filepath': filepath}
            
            # Если и это не помогло, пробуем через альтернативный фронтенд для YouTube
            if 'youtube.com' in url or 'youtu.be' in url:
                video_id = None
                if 'youtube.com/watch' in url and 'v=' in url:
                    video_id = url.split('v=')[1].split('&')[0]
                elif 'youtu.be/' in url:
                    video_id = url.split('youtu.be/')[1].split('?')[0]
                elif 'youtube.com/shorts/' in url:
                    video_id = url.split('shorts/')[1].split('?')[0]
                
                if video_id:
                    for invidious_domain in ["invidious.snopyta.org", "yewtu.be", "piped.kavin.rocks"]:
                        try:
                            invidious_url = f"https://{invidious_domain}/watch?v={video_id}"
                            logger.info(f"Пробуем скачать через {invidious_domain}: {invidious_url}")
                            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                info = ydl.extract_info(invidious_url, download=True)
                                if info:
                                    filepath = ydl.prepare_filename(info)
                                    return {'title': info.get('title', 'Видео'), 'filepath': filepath}
                        except Exception as e:
                            logger.warning(f"Метод через {invidious_domain} не сработал: {e}")
            
            # Последняя попытка со стандартным форматом
            fallback_opts = ydl_opts.copy()
            fallback_opts['format'] = config.DEFAULT_VIDEO_FORMAT
            with yt_dlp.YoutubeDL(fallback_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if info:
                    filepath = ydl.prepare_filename(info)
                    return {'title': info.get('title', 'Видео'), 'filepath': filepath}
            
            raise ValueError(f"Не удалось скачать видео {url} после всех попыток")
            
        except Exception as e:
            logger.error(f"Ошибка при скачивании видео {url}: {e}")
            raise 