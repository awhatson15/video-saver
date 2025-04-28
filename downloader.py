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
# Словарь для сопоставления канонических URL (от yt-dlp) с исходными URL (от пользователя)
canonical_url_map = {}
# Блокировка для потокобезопасного доступа к словарям выше
data_lock = threading.Lock()

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
            }
            
            def _extract_info():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(url, download=False)
            
            info = await loop.run_in_executor(executor, _extract_info)
            
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
            
            # --- Изменено: Ищем исходный URL по каноническому ---
            with data_lock:
                original_url = canonical_url_map.get(url)
                if not original_url:
                    logger.warning(f"Progress hook: Original URL for canonical '{url}' not found in map. Map: {canonical_url_map}")
                    return
                    
                if original_url not in active_downloads:
                    logger.warning(f"Progress hook: Original URL '{original_url}' (mapped from '{url}') not found in active_downloads. Current keys: {list(active_downloads.keys())}")
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
                # --- Изменено: Используем блокировку ---
                with data_lock:
                    original_url = canonical_url_map.get(url)
                    if original_url and original_url in active_downloads:
                        active_downloads[original_url]['percent'] = 100
                        active_downloads[original_url]['percent_rounded'] = 100
                        # Финальное имя файла важно обновить здесь
                        active_downloads[original_url]['filename'] = d.get('filename') 
                        logger.debug(f"Progress hook marked finished for URL '{original_url}' (canonical: '{url}'). Filename: {d.get('filename')}")
                # --- Конец изменений ---

    async def download_video(self, url, video_format="high", user_id=None, chat_id=None, message_id=None):
        """Скачивание видео (предполагается, что active_downloads уже инициализирован)"""
        filename = None
        info = None # Инициализируем info
        loop = asyncio.get_event_loop()
        try:
            # Определяем параметры для _download
            output_template = os.path.join(config.DOWNLOAD_DIR, '%(title)s.%(ext)s')
            
            # --- Логика _download остается прежней --- 
            def _download():
                process = None
                downloaded_filename = None
                info = None
                try:
                    # Определяем формат для yt-dlp на основе video_format
                    format_specifier = None
                    if video_format.isdigit():
                        format_specifier = f"{video_format}+bestaudio/bestaudio[ext=m4a]/{video_format}"
                    elif video_format == 'auto':
                        logger.warning("'_download' called with 'auto' format, using default.")
                        format_specifier = config.DEFAULT_VIDEO_FORMAT
                    else:
                        format_specifier = config.VIDEO_FORMATS.get(video_format, config.DEFAULT_VIDEO_FORMAT)
                    
                    # Создаем опции для этого скачивания
                    ydl_opts_list = [
                        '--format', format_specifier,
                        '--output', output_template,
                        '--quiet',
                        # Передаем хук как параметр командной строки, если это возможно
                        # Важно: Это требует, чтобы yt-dlp мог вызывать Python хуки из командной строки
                        # На практике, проще оставить progress_hook в Python коде
                        # и вызывать ydl.download() внутри _download, как было раньше.
                        # НО! Чтобы получить Popen объект, нужно вызывать yt-dlp как процесс.
                        # Компромисс: НЕ ИСПОЛЬЗУЕМ progress_hook здесь, а парсим stdout?
                        # Или все же используем Python API yt-dlp внутри _download?
                        # Давайте вернемся к Python API для простоты хуков, но сохраним Popen.
                        # Нет, так не получится. Нужно выбрать: либо Popen и парсинг stdout,
                        # либо Python API и отсутствие прямого контроля над процессом.
                        
                        # --- Возвращаемся к Python API внутри потока --- 
                        # Это значит, что мы НЕ можем получить Popen объект и убить процесс.
                        # Отмена будет такой же, как и раньше (только удаление из словаря).
                        # TODO: Переделать на asyncio.create_subprocess_exec для реальной отмены.
                        # Пока оставляем старую логику вызова yt-dlp
                    ]

                    # Настройки постпроцессоров остаются в ydl_opts словаре для Python API
                    ydl_opts_dict = {
                        'format': format_specifier,
                        'outtmpl': output_template,
                        'quiet': True,
                        'progress_hooks': [self.progress_hook], # Оставляем хук
                        'format_sort': ['res', 'ext:mp4:m4a'],
                        'format_preference': ['mp4', 'm4a'],
                        # 'postprocessors': Будут добавлены ниже
                        'merge_output_format': 'mp4', # По умолчанию mp4
                    }
                    
                    if video_format != 'audio':
                        ydl_opts_dict['postprocessors'] = [{'key': 'FFmpegVideoConvertor','preferedformat': 'mp4'}]
                    else:
                         ydl_opts_dict['postprocessors'] = []
                         ydl_opts_dict['merge_output_format'] = None # Не нужно объединять для аудио

                    with yt_dlp.YoutubeDL(ydl_opts_dict) as ydl:
                        try:
                            logger.info(f"Attempting download with format: {ydl_opts_dict.get('format')}")
                            info = ydl.extract_info(url, download=True)
                            downloaded_filename = ydl.prepare_filename(info)
                            return info, downloaded_filename
                        except Exception as e:
                            logger.warning(f"Download failed with format '{ydl_opts_dict.get('format')}': {e}")
                            # Логика фоллбэка (остается прежней)
                            if ydl_opts_dict.get('format') != config.DEFAULT_VIDEO_FORMAT:
                                logger.info(f"Trying fallback format: {config.DEFAULT_VIDEO_FORMAT}")
                                fallback_opts = ydl_opts_dict.copy()
                                fallback_opts['format'] = config.DEFAULT_VIDEO_FORMAT
                                if video_format != 'audio': # Убедимся, что фоллбэк тоже конвертирует в mp4 если не аудио
                                     fallback_opts['postprocessors'] = [{'key': 'FFmpegVideoConvertor','preferedformat': 'mp4'}]
                                     fallback_opts['merge_output_format'] = 'mp4'
                                else:
                                    fallback_opts['postprocessors'] = []
                                    fallback_opts['merge_output_format'] = None
                                
                                with yt_dlp.YoutubeDL(fallback_opts) as fallback_ydl:
                                    info = fallback_ydl.extract_info(url, download=True)
                                    downloaded_filename = fallback_ydl.prepare_filename(info)
                                    return info, downloaded_filename
                            else:
                                logger.error(f"Fallback download with format '{config.DEFAULT_VIDEO_FORMAT}' also failed.")
                                raise e
                except Exception as download_err:
                     logger.error(f"Error in _download thread for {url}: {download_err}")
                     # Важно пробросить исключение, чтобы основной поток его увидел
                     raise
            # --- Конец логики _download --- 

            # Запускаем загрузку (остается run_in_executor)
            info, filename = await loop.run_in_executor(executor, _download)
            
            # Проверяем, существует ли файл после скачивания
            if not filename or not os.path.exists(filename):
                # Если info есть, используем его для сообщения об ошибке
                err_title = info.get('title', url) if info else url
                raise FileNotFoundError(f"Downloaded file not found after thread execution for {err_title}: {filename}")

            file_size = os.path.getsize(filename)
            
            # Добавляем видео в кэш (если включено и успешно скачано)
            if config.CACHE_ENABLED:
                # Убедимся, что info не None перед доступом к title
                cache_title = info.get('title', 'Видео') if info else 'Видео'
                db.add_video_to_cache(url, cache_title, filename, file_size, video_format)
            
            if user_id:
                db.log_download(user_id, url, "success_download")
            
            # Возвращаем результат (убедимся, что info есть)
            final_title = info.get('title', 'Видео') if info else 'Видео'
            return {
                'title': final_title,
                'file_path': filename,
                'size': file_size
            }
            
        except Exception as e:
            # Логируем ошибку и пробрасываем дальше
            logger.error(f"Ошибка при скачивании видео '{url}' (в download_video): {e}")
            if user_id:
                db.log_download(user_id, url, "error", str(e))
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
            
            # Удаляем файл вне блокировки
            if final_filename and not config.CACHE_ENABLED:
                try:
                    if os.path.exists(final_filename):
                        os.remove(final_filename)
                        logger.info(f"Удален временный файл: {final_filename}")
                except OSError as rm_err:
                    logger.error(f"Не удалось удалить временный файл {final_filename}: {rm_err}")

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
            # Используем asyncio.sleep (если cancel_download вызывается из async контекста) 
            # или time.sleep (если из синхронного). Так как вызывается из async callback, используем asyncio.sleep
            # await asyncio.sleep(0.5) # Это было неверно, т.к. cancel_download - синхронная
            time.sleep(0.5) # Используем time.sleep
            try:
                if os.path.exists(filename_to_delete):
                    os.remove(filename_to_delete)
                    logger.info(f"Deleted cancelled download file: {filename_to_delete}")
                else:
                    logger.warning(f"Cancelled download file not found: {filename_to_delete}")
            except OSError as rm_err:
                logger.error(f"Failed to delete cancelled download file {filename_to_delete}: {rm_err}")
        else:
            if cancelled_in_dict: # Логируем, только если запись была найдена
                logger.warning(f"Filename not found for cancelled download URL: {url}")

        return cancelled_in_dict # Возвращаем True, если запись была найдена и удалена из словаря
        # --- Конец изменений --- 

    def split_large_video(self, file_path, max_segment_size=45):
        """
        Разделяет большие видео на части с использованием FFmpeg.
        Удален аварийный режим разделения по байтам.
        
        Args:
            file_path: Путь к исходному видео
            max_segment_size: Максимальный размер сегмента в MB (по умолчанию 45MB)
            
        Returns:
            list: Список путей к созданным файлам частей
        """
        output_files = []
        temp_files = []

        # Проверяем существование файла
        if not os.path.exists(file_path):
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

            # 2. Проверяем размер и необходимость разделения
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
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

                    # Проверяем, что файл создан и не пустой
                    if os.path.exists(segment_temp_file) and os.path.getsize(segment_temp_file) > 0:
                        if os.path.exists(original_segment_file):
                            os.remove(original_segment_file)
                        os.rename(segment_temp_file, original_segment_file)
                        output_files.append(original_segment_file)
                        if segment_temp_file in temp_files: temp_files.remove(segment_temp_file)
                        logger.info(f"Сегмент {i+1} успешно создан: {original_segment_file}")
                    else:
                        logger.error(f"FFmpeg завершился, но сегмент {segment_temp_file} не создан или пуст.")

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
                    if os.path.exists(f): os.remove(f)
                except OSError: pass
            return []

        finally:
            # Гарантированно удаляем все временные файлы ffmpeg (_part<N>)
            for temp_f in temp_files:
                try:
                    if os.path.exists(temp_f):
                        os.remove(temp_f)
                        logger.debug(f"Удален временный файл сегмента: {temp_f}")
                except OSError as rm_err:
                    logger.warning(f"Не удалось удалить временный файл сегмента {temp_f}: {rm_err}")

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