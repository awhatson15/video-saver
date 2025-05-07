#!/bin/bash

# Скрипт миграции файлов из старой директории в новую с правильными правами доступа

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Проверка прав root
if [ "$EUID" -ne 0 ]; then
  echo -e "${RED}Ошибка: Этот скрипт должен быть запущен с правами root${NC}"
  echo "Выполните: sudo $0"
  exit 1
fi

# Пути к директориям
OLD_DIR="/root/video-saver/downloads/shared"
NEW_DIR="/var/www/downloads/shared"

# Проверка существования старой директории
if [ ! -d "$OLD_DIR" ]; then
  echo -e "${YELLOW}Предупреждение: Исходная директория $OLD_DIR не найдена${NC}"
  echo "Создаем только новую директорию..."
else
  echo -e "${GREEN}Исходная директория $OLD_DIR найдена${NC}"
fi

# Создание новой директории если не существует
mkdir -p "$NEW_DIR"
echo -e "${GREEN}Директория $NEW_DIR создана/проверена${NC}"

# Копирование файлов
if [ -d "$OLD_DIR" ]; then
  echo -e "${YELLOW}Копирование файлов из $OLD_DIR в $NEW_DIR...${NC}"
  cp -r "$OLD_DIR"/* "$NEW_DIR"/ 2>/dev/null
  echo -e "${GREEN}Файлы скопированы${NC}"
fi

# Установка правильных прав доступа
echo -e "${YELLOW}Установка прав доступа...${NC}"
chown -R www-data:www-data "$NEW_DIR"
chmod -R 755 "$NEW_DIR"
echo -e "${GREEN}Права доступа установлены${NC}"

# Проверка количества файлов
OLD_COUNT=0
NEW_COUNT=0

if [ -d "$OLD_DIR" ]; then
  OLD_COUNT=$(find "$OLD_DIR" -type f | wc -l)
fi

NEW_COUNT=$(find "$NEW_DIR" -type f | wc -l)

echo -e "${GREEN}Миграция завершена${NC}"
echo -e "Файлов в исходной директории: ${YELLOW}$OLD_COUNT${NC}"
echo -e "Файлов в новой директории: ${YELLOW}$NEW_COUNT${NC}"

# Рекомендация по настройке конфига бота
echo -e "\n${YELLOW}ВАЖНО: Не забудьте обновить настройки в config.py:${NC}"
echo -e "  DIRECT_LINK_STORAGE = '/var/www/downloads/shared'"
echo -e "или использовать переменную окружения."

echo -e "\n${GREEN}Готово!${NC}" 