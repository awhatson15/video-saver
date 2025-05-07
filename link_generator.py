import os
import uuid
import time
import hashlib
import json
from datetime import datetime, timedelta
import aiofiles
import config
import logging

logger = logging.getLogger(__name__)

class LinkGenerator:
    def __init__(self):
        # Используем путь из конфига
        self.storage_path = config.DIRECT_LINK_STORAGE
        # Директория создается при инициализации, если не существует
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path, exist_ok=True)
            
    async def generate_link(self, source_file_path, filename=None):
        """Создает прямую ссылку для скачивания файла"""
        try:
            # Проверка существования исходного файла
            if not os.path.exists(source_file_path):
                logger.error(f"Файл не существует: {source_file_path}")
                return None
                
            # Если имя файла не предоставлено, используем имя из пути
            if not filename:
                filename = os.path.basename(source_file_path)
                
            # Получаем расширение файла
            _, file_ext = os.path.splitext(filename)
            if not file_ext:
                # Если расширение не найдено, добавим .mp4 по умолчанию для видеофайлов
                file_ext = '.mp4'
            
            # Получаем название файла без расширения для использования в URL
            name_without_ext = os.path.splitext(filename)[0]
            
            # Создаем безопасное URL-friendly название (транслитерация и замена специальных символов)
            import re
            from transliterate import translit
            
            # Попытка транслитерации с русского на английский
            try:
                safe_name = translit(name_without_ext, 'ru', reversed=True)
            except:
                # Если не получилось (не русский или ошибка), используем оригинал
                safe_name = name_without_ext
                
            # Заменяем все недопустимые символы на дефисы
            safe_name = re.sub(r'[^a-zA-Z0-9-]', '-', safe_name)
            # Заменяем множественные дефисы на один
            safe_name = re.sub(r'-+', '-', safe_name)
            # Обрезаем до 50 символов для предотвращения слишком длинных URL
            safe_name = safe_name[:50].strip('-')
            
            # Создаем короткий хеш (8 символов) для уникальности
            short_hash = hashlib.md5(f"{source_file_path}_{time.time()}".encode()).hexdigest()[:8]
            
            # Формируем итоговое имя файла: название-хеш.расширение
            safe_filename = f"{safe_name}-{short_hash}{file_ext}"
            dest_path = os.path.join(self.storage_path, safe_filename)
            
            logger.debug(f"Генерация ссылки: исходный={filename}, безопасное имя={safe_name}, результат={safe_filename}")
            
            # Копируем файл (асинхронно)
            async with aiofiles.open(source_file_path, 'rb') as src_file:
                file_content = await src_file.read()
                
            async with aiofiles.open(dest_path, 'wb') as dest_file:
                await dest_file.write(file_content)
                
            logger.info(f"Файл скопирован: {dest_path}")
            
            # Записываем метаданные с информацией о сроке действия и оригинальном имени
            expire_time = datetime.now() + timedelta(hours=config.DIRECT_LINK_EXPIRE_HOURS)
            meta_data = {
                "original_filename": filename,
                "expires": expire_time.isoformat(),
                "source_path": source_file_path,
                "created": datetime.now().isoformat()
            }
            
            # Сохраняем метаданные
            async with aiofiles.open(f"{dest_path}.meta", "w", encoding='utf-8') as meta_file:
                await meta_file.write(json.dumps(meta_data, ensure_ascii=False))
                
            # Формируем URL для доступа к файлу
            download_url = f"https://{config.DIRECT_LINK_DOMAIN}/{safe_filename}"
            size_mb = round(os.path.getsize(dest_path) / (1024 * 1024), 2)
            
            # Возвращаем информацию о ссылке
            return {
                "url": download_url,
                "filename": filename,
                "expires": expire_time,
                "size_mb": size_mb,
                "file_path": dest_path
            }
            
        except Exception as e:
            logger.error(f"Ошибка при создании прямой ссылки: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
            
    async def cleanup_expired_links(self):
        """Удаляет просроченные файлы"""
        try:
            now = datetime.now()
            count_removed = 0
            
            # Получаем список всех метафайлов
            files = os.listdir(self.storage_path)
            meta_files = [f for f in files if f.endswith('.meta')]
            
            for meta_filename in meta_files:
                meta_path = os.path.join(self.storage_path, meta_filename)
                
                # Получаем данные метафайла
                try:
                    logger.debug(f"Обработка метафайла: {meta_path}")
                    async with aiofiles.open(meta_path, 'r', encoding='utf-8') as f:
                        content = await f.read()
                        
                    try:
                        meta_data = json.loads(content)
                    except json.JSONDecodeError as json_err:
                        logger.error(f"Ошибка разбора JSON в метафайле {meta_path}: {json_err}")
                        continue
                        
                    # Проверяем срок действия
                    expires_str = meta_data.get('expires')
                    if not expires_str:
                        logger.warning(f"Метафайл {meta_path} не содержит даты истечения. Пропускаем.")
                        continue
                    
                    try:
                        expire_time = datetime.fromisoformat(expires_str)
                    except ValueError as val_err:
                        logger.error(f"Некорректный формат даты истечения в {meta_path}: {val_err}")
                        continue
                    
                    if now > expire_time:
                        # Удаляем метафайл и соответствующий файл
                        file_path = meta_path[:-5]  # убираем .meta
                        
                        if os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                                logger.debug(f"Удален просроченный файл: {file_path}")
                            except OSError as os_err:
                                logger.error(f"Ошибка удаления файла {file_path}: {os_err}")
                            
                        try:
                            os.remove(meta_path)
                            logger.debug(f"Удален метафайл: {meta_path}")
                            count_removed += 1
                        except OSError as os_err:
                            logger.error(f"Ошибка удаления метафайла {meta_path}: {os_err}")
                        
                except Exception as e:
                    logger.error(f"Ошибка при обработке метафайла {meta_path}: {e}")
                    continue
                    
            if count_removed > 0:
                logger.info(f"Удалено {count_removed} просроченных файлов при очистке")
            return count_removed
            
        except Exception as e:
            logger.error(f"Ошибка при очистке просроченных ссылок: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return 0
            
    async def get_links_stats(self):
        """Возвращает статистику по активным ссылкам"""
        try:
            total_size = 0
            active_links = 0
            now = datetime.now()
            
            files = os.listdir(self.storage_path)
            meta_files = [f for f in files if f.endswith('.meta')]
            
            for meta_filename in meta_files:
                meta_path = os.path.join(self.storage_path, meta_filename)
                file_path = meta_path[:-5]  # убираем .meta
                
                if not os.path.exists(file_path):
                    continue
                
                # Получаем размер файла
                file_size = os.path.getsize(file_path)
                total_size += file_size
                
                # Проверяем, активна ли ссылка
                try:
                    async with aiofiles.open(meta_path, 'r', encoding='utf-8') as f:
                        content = await f.read()
                        
                    try:
                        meta_data = json.loads(content)
                    except json.JSONDecodeError:
                        logger.warning(f"Пропущен некорректный JSON в {meta_path}")
                        continue
                    
                    expires_str = meta_data.get('expires')
                    if not expires_str:
                        continue
                        
                    try:
                        expire_time = datetime.fromisoformat(expires_str)
                        if now < expire_time:
                            active_links += 1
                    except ValueError:
                        logger.warning(f"Некорректный формат даты в {meta_path}")
                except Exception as e:
                    logger.warning(f"Ошибка при чтении метафайла {meta_path}: {e}")
            
            return {
                "active_links": active_links,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "storage_path": self.storage_path
            }
        except Exception as e:
            logger.error(f"Ошибка при получении статистики ссылок: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "active_links": 0,
                "total_size_mb": 0,
                "error": str(e)
            } 