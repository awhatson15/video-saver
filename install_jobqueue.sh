#!/bin/bash

# Скрипт для установки компонента job-queue для python-telegram-bot

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Установка компонента job-queue для python-telegram-bot...${NC}"

# Определяем, какая команда pip доступна
if command -v pip3 &> /dev/null; then
    PIP_CMD="pip3"
elif command -v pip &> /dev/null; then
    PIP_CMD="pip"
else
    echo -e "${RED}Ошибка: pip не найден. Установите Python и pip.${NC}"
    exit 1
fi

# Устанавливаем необходимый компонент
$PIP_CMD install "python-telegram-bot[job-queue]==22.0"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Установка успешно завершена!${NC}"
    echo -e "${YELLOW}Теперь вы можете запустить бота снова.${NC}"
else
    echo -e "${RED}Произошла ошибка при установке.${NC}"
    echo -e "${YELLOW}Попробуйте выполнить команду вручную:${NC}"
    echo "pip3 install \"python-telegram-bot[job-queue]==22.0\""
    exit 1
fi

# Обновляем requirements.txt если он существует
if [ -f "requirements.txt" ]; then
    echo -e "${YELLOW}Обновление файла requirements.txt...${NC}"
    # Заменяем строку python-telegram-bot==22.0 на python-telegram-bot[job-queue]==22.0
    sed -i 's/python-telegram-bot==22.0/python-telegram-bot[job-queue]==22.0/' requirements.txt
    echo -e "${GREEN}Файл requirements.txt обновлен.${NC}"
fi

echo -e "${GREEN}Готово!${NC}" 