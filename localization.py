import json
import os
import logging

logger = logging.getLogger(__name__)

_MESSAGES = {}
_DEFAULT_LANG = 'ru' # Язык по умолчанию

def load_localization(lang=_DEFAULT_LANG):
    """Загружает файл локализации для указанного языка."""
    global _MESSAGES
    locale_file = os.path.join('locales', f'{lang}.json')
    try:
        with open(locale_file, 'r', encoding='utf-8') as f:
            _MESSAGES = json.load(f)
        logger.info(f"Локализация для языка '{lang}' успешно загружена из {locale_file}")
    except FileNotFoundError:
        logger.error(f"Файл локализации не найден: {locale_file}")
        _MESSAGES = {}
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка разбора JSON в файле {locale_file}: {e}")
        _MESSAGES = {}
    except Exception as e:
        logger.error(f"Не удалось загрузить локализацию из {locale_file}: {e}")
        _MESSAGES = {}

def get_message(key, **kwargs):
    """Возвращает локализованное сообщение по ключу, форматируя его с kwargs."""
    message_template = _MESSAGES.get(key)
    if message_template is None:
        logger.warning(f"Ключ локализации не найден: '{key}'")
        # Возвращаем ключ в качестве запасного варианта
        return key 

    try:
        return message_template.format(**kwargs)
    except KeyError as e:
        # Ошибка, если в шаблоне есть плейсхолдер, а в kwargs нет значения
        logger.error(f"Отсутствует ключ '{e}' для форматирования сообщения '{key}'")
        return message_template # Возвращаем неформатированный шаблон
    except Exception as e:
        logger.error(f"Ошибка форматирования сообщения '{key}': {e}")
        return message_template # Возвращаем неформатированный шаблон

# Загружаем локализацию при импорте модуля
load_localization() 