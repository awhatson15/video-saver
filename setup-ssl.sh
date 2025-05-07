#!/bin/bash

# Скрипт для настройки SSL-сертификатов с помощью Let's Encrypt (Certbot)
# Для домена dl.rox.su

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Проверка, запущен ли скрипт от root
if [ "$EUID" -ne 0 ]; then
  echo -e "${RED}Ошибка: Этот скрипт должен быть запущен с правами root${NC}"
  echo "Выполните: sudo $0"
  exit 1
fi

# Домен для сертификата
DOMAIN="dl.rox.su"

# Директория для статических файлов Nginx
WEBROOT="/var/www/html"

# Создаем директорию, если она не существует
if [ ! -d "$WEBROOT" ]; then
  mkdir -p "$WEBROOT"
  echo -e "${GREEN}Создана директория $WEBROOT${NC}"
fi

# Проверка наличия certbot
if ! command -v certbot &> /dev/null; then
  echo -e "${YELLOW}Certbot не установлен. Устанавливаем...${NC}"
  
  # Определение системы
  if [ -f /etc/debian_version ]; then
    # Debian/Ubuntu
    apt update
    apt install -y certbot python3-certbot-nginx
  elif [ -f /etc/redhat-release ]; then
    # CentOS/RHEL
    dnf install -y epel-release
    dnf install -y certbot python3-certbot-nginx
  else
    echo -e "${RED}Не удалось определить систему. Установите certbot вручную.${NC}"
    exit 1
  fi
  
  echo -e "${GREEN}Certbot успешно установлен${NC}"
fi

# Проверка и установка Nginx, если не установлен
if ! command -v nginx &> /dev/null; then
  echo -e "${YELLOW}Nginx не установлен. Устанавливаем...${NC}"
  
  # Определение системы
  if [ -f /etc/debian_version ]; then
    # Debian/Ubuntu
    apt update
    apt install -y nginx
  elif [ -f /etc/redhat-release ]; then
    # CentOS/RHEL
    dnf install -y nginx
  else
    echo -e "${RED}Не удалось определить систему. Установите nginx вручную.${NC}"
    exit 1
  fi
  
  echo -e "${GREEN}Nginx успешно установлен${NC}"
fi

# Создаем конфигурацию Nginx для Let's Encrypt
echo -e "${YELLOW}Создаем временную конфигурацию Nginx для проверки домена...${NC}"

# Путь к конфигурации Nginx
NGINX_CONF="/etc/nginx/sites-available/$DOMAIN"
NGINX_ENABLED="/etc/nginx/sites-enabled/$DOMAIN"

# Создаем базовую конфигурацию для валидации
cat > "$NGINX_CONF" << EOF
server {
    listen 80;
    server_name $DOMAIN;

    location /.well-known/acme-challenge/ {
        root $WEBROOT;
    }

    location / {
        return 301 https://\$host\$request_uri;
    }
}
EOF

# Создаем символическую ссылку, если нужно
if [ ! -f "$NGINX_ENABLED" ]; then
  ln -s "$NGINX_CONF" "$NGINX_ENABLED"
  echo -e "${GREEN}Создана символическая ссылка для конфигурации Nginx${NC}"
fi

# Перезапускаем Nginx
systemctl restart nginx
echo -e "${GREEN}Nginx перезапущен${NC}"

# Получаем сертификат Let's Encrypt
echo -e "${YELLOW}Получаем SSL-сертификат для домена $DOMAIN...${NC}"

certbot certonly --webroot --webroot-path="$WEBROOT" -d "$DOMAIN" --email admin@rox.su --agree-tos --non-interactive

if [ $? -eq 0 ]; then
  echo -e "${GREEN}SSL-сертификаты успешно получены!${NC}"
  
  # Проверяем наличие сертификатов
  if [ -f "/etc/letsencrypt/live/$DOMAIN/fullchain.pem" ]; then
    echo -e "${GREEN}Сертификаты установлены в /etc/letsencrypt/live/$DOMAIN/${NC}"
    
    # Копируем полную конфигурацию для Nginx
    echo -e "${YELLOW}Настраиваем Nginx для работы с SSL...${NC}"
    
    # Путь к полной конфигурации
    FULL_NGINX_CONF="./nginx-dl.rox.su.conf"
    
    if [ -f "$FULL_NGINX_CONF" ]; then
      # Обновляем путь к директории shared
      SHARED_DIR=$(pwd)/downloads/shared
      SHARED_DIR=${SHARED_DIR//\//\\/} # Экранируем слеши для sed
      
      # Копируем с заменой пути к shared директории
      sed "s/\/path\/to\/downloads\/shared\//$SHARED_DIR\//" "$FULL_NGINX_CONF" > "$NGINX_CONF"
      
      echo -e "${GREEN}Конфигурация Nginx обновлена${NC}"
    else
      echo -e "${RED}Не найден файл полной конфигурации Nginx: $FULL_NGINX_CONF${NC}"
      echo -e "${YELLOW}Вы можете настроить Nginx вручную позже${NC}"
    fi
    
    # Перезапускаем Nginx с новой конфигурацией
    systemctl restart nginx
    
    if [ $? -eq 0 ]; then
      echo -e "${GREEN}Nginx успешно настроен с SSL!${NC}"
      echo -e "${GREEN}Ваш сайт теперь доступен по адресу: https://$DOMAIN${NC}"
    else
      echo -e "${RED}Возникла ошибка при перезапуске Nginx. Проверьте конфигурацию.${NC}"
    fi
    
    # Настраиваем автоматическое обновление сертификатов
    echo -e "${YELLOW}Настраиваем автоматическое обновление сертификатов...${NC}"
    
    if [ -f /etc/crontab ]; then
      # Проверяем, нет ли уже строки для certbot
      if ! grep -q "certbot renew" /etc/crontab; then
        echo "0 3 * * * root certbot renew --quiet && systemctl reload nginx" >> /etc/crontab
        echo -e "${GREEN}Настроено автоматическое обновление сертификатов (каждый день в 3:00)${NC}"
      else
        echo -e "${YELLOW}Автоматическое обновление сертификатов уже настроено${NC}"
      fi
    else
      echo -e "${RED}Не найден файл /etc/crontab. Настройте обновление сертификатов вручную.${NC}"
      echo -e "${YELLOW}Рекомендуемая команда: certbot renew --quiet && systemctl reload nginx${NC}"
    fi
    
  else
    echo -e "${RED}Не найдены файлы сертификатов. Что-то пошло не так.${NC}"
  fi
else
  echo -e "${RED}Возникла ошибка при получении сертификатов.${NC}"
  echo -e "${YELLOW}Проверьте доступность домена $DOMAIN и настройки DNS.${NC}"
fi

echo -e "${GREEN}Настройка SSL-сертификатов завершена!${NC}"
echo -e "${YELLOW}Примечание: Для полной настройки может потребоваться дополнительная конфигурация ${NC}" 