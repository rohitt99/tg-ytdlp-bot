#!/usr/bin/env python3
"""
Скрипт для автоматического обновления кода из GitHub репозитория
Заменяет только *.py файлы, исключая конфигурационные файлы
"""

import os
import sys
import requests
import json
import shutil
from pathlib import Path
from datetime import datetime

# Конфигурация
REPO_URL = "https://github.com/chelaxian/tg-ytdlp-bot"
BRANCH = "newdesign"
API_BASE = "https://api.github.com/repos/chelaxian/tg-ytdlp-bot"

# Файлы и папки, которые НЕ должны обновляться
EXCLUDED_FILES = [
    "CONFIG/config.py",  # Основной конфигурационный файл
    #"requirements.txt",  # Зависимости могут отличаться
    ".env",              # Переменные окружения
    ".bot_pid",          # PID файл бота
    "bot.log",           # Логи бота
    "runtime.log",       # Логи времени выполнения
    "magic.session",     # Сессия Pyrogram
    "magic.session-journal",  # Журнал сессии
    "dump.json",         # Дамп Firebase
    "firebase_cache.json",  # Кэш Firebase
]

EXCLUDED_DIRS = [
    "CONFIG",            # Вся папка конфигурации
    "venv",              # Виртуальное окружение
    ".git",              # Git репозиторий
    "__pycache__",       # Кэш Python
    "_backup",           # Резервные копии
    "users",             # Пользовательские данные
    "cookies",           # Файлы cookies
    "TXT",               # Текстовые файлы
    "_arabic_fonts_amiri",  # Шрифты
    #"DOWN_AND_UP",       # Временные файлы загрузки
]

def log(message, level="INFO"):
    """Логирование с временной меткой"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {level}: {message}")

def get_file_content_from_github(file_path):
    """Получает содержимое файла из GitHub API"""
    try:
        url = f"{API_BASE}/contents/{file_path}?ref={BRANCH}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        if data.get("type") == "file":
            import base64
            content = base64.b64decode(data["content"]).decode('utf-8')
            return content
        else:
            return None
    except Exception as e:
        log(f"Ошибка получения файла {file_path}: {e}", "ERROR")
        return None

def get_repo_tree():
    """Получает дерево файлов из репозитория"""
    try:
        url = f"{API_BASE}/git/trees/{BRANCH}?recursive=1"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        return data.get("tree", [])
    except Exception as e:
        log(f"Ошибка получения дерева репозитория: {e}", "ERROR")
        return []

def should_update_file(file_path):
    """Проверяет, нужно ли обновлять файл"""
    # Проверяем исключенные файлы
    for excluded in EXCLUDED_FILES:
        if file_path == excluded:
            return False
    
    # Проверяем исключенные директории
    for excluded_dir in EXCLUDED_DIRS:
        if file_path.startswith(excluded_dir + "/"):
            return False
    
    # Обновляем только Python файлы
    if not file_path.endswith('.py'):
        return False
    
    return True

def backup_file(file_path):
    """Создает резервную копию файла"""
    try:
        if os.path.exists(file_path):
            backup_path = f"{file_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(file_path, backup_path)
            log(f"Создана резервная копия: {backup_path}")
            return backup_path
    except Exception as e:
        log(f"Ошибка создания резервной копии {file_path}: {e}", "ERROR")
    return None

def update_file(file_path, content):
    """Обновляет файл с новым содержимым"""
    try:
        # Создаем директории, если их нет
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Создаем резервную копию
        backup_path = backup_file(file_path)
        
        # Записываем новое содержимое
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        log(f"Обновлен файл: {file_path}")
        if backup_path:
            log(f"Резервная копия: {backup_path}")
        
        return True
    except Exception as e:
        log(f"Ошибка обновления файла {file_path}: {e}", "ERROR")
        return False

def main():
    """Основная функция обновления"""
    log("🚀 Запуск обновления кода из GitHub репозитория")
    log(f"Репозиторий: {REPO_URL}")
    log(f"Ветка: {BRANCH}")
    
    # Проверяем, что мы в правильной директории
    if not os.path.exists("magic.py"):
        log("❌ Файл magic.py не найден. Убедитесь, что скрипт запущен в папке с ботом.", "ERROR")
        return False
    
    # Получаем дерево файлов из репозитория
    log("📥 Получение списка файлов из репозитория...")
    tree = get_repo_tree()
    
    if not tree:
        log("❌ Не удалось получить список файлов из репозитория", "ERROR")
        return False
    
    # Фильтруем только Python файлы для обновления
    python_files = []
    for item in tree:
        if item.get("type") == "blob" and should_update_file(item["path"]):
            python_files.append(item["path"])
    
    log(f"📋 Найдено {len(python_files)} Python файлов для обновления")
    
    # Показываем список файлов, которые будут обновлены
    log("📝 Файлы для обновления:")
    for file_path in python_files:
        log(f"  - {file_path}")
    
    # Спрашиваем подтверждение
    response = input("\n🤔 Продолжить обновление? (y/N): ").strip().lower()
    if response not in ['y', 'yes', 'да']:
        log("❌ Обновление отменено пользователем")
        return False
    
    # Обновляем файлы
    updated_count = 0
    failed_count = 0
    
    for file_path in python_files:
        log(f"🔄 Обновление {file_path}...")
        
        content = get_file_content_from_github(file_path)
        if content is not None:
            if update_file(file_path, content):
                updated_count += 1
            else:
                failed_count += 1
        else:
            log(f"❌ Не удалось получить содержимое файла {file_path}", "ERROR")
            failed_count += 1
    
    # Результаты
    log("=" * 50)
    log("📊 Результаты обновления:")
    log(f"✅ Успешно обновлено: {updated_count}")
    log(f"❌ Ошибок: {failed_count}")
    log(f"📁 Всего файлов: {len(python_files)}")
    
    if failed_count == 0:
        log("🎉 Все файлы успешно обновлены!")
        return True
    else:
        log(f"⚠️ Обновлено с ошибками: {failed_count} файлов", "WARNING")
        return False

def show_excluded_files():
    """Показывает список исключенных файлов и папок"""
    log("📋 Исключенные файлы:")
    for file_path in EXCLUDED_FILES:
        log(f"  - {file_path}")
    
    log("📁 Исключенные папки:")
    for dir_path in EXCLUDED_DIRS:
        log(f"  - {dir_path}/")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--show-excluded":
        show_excluded_files()
    else:
        success = main()
        sys.exit(0 if success else 1)
