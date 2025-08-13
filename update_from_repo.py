#!/usr/bin/env python3
"""
Скрипт для автоматического обновления кода из GitHub репозитория
Скачивает репозиторий во временную папку и заменяет нужные файлы
"""

import os
import sys
import shutil
import tempfile
import subprocess
from pathlib import Path
from datetime import datetime

# Конфигурация
REPO_URL = "https://github.com/chelaxian/tg-ytdlp-bot.git"
BRANCH = "newdesign"

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

def clone_repository(temp_dir):
    """Клонирует репозиторий во временную папку"""
    try:
        log(f"📥 Клонирование репозитория в {temp_dir}...")
        
        # Команда для клонирования
        cmd = [
            'git', 'clone', 
            '--branch', BRANCH,
            '--depth', '1',  # Только последний коммит
            '--single-branch',
            REPO_URL, 
            temp_dir
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            log("✅ Репозиторий успешно клонирован")
            return True
        else:
            log(f"❌ Ошибка клонирования: {result.stderr}", "ERROR")
            return False
            
    except subprocess.TimeoutExpired:
        log("❌ Таймаут при клонировании репозитория", "ERROR")
        return False
    except Exception as e:
        log(f"❌ Ошибка клонирования: {e}", "ERROR")
        return False

def find_python_files(source_dir):
    """Находит все Python файлы в исходной директории"""
    python_files = []
    
    for root, dirs, files in os.walk(source_dir):
        # Исключаем ненужные директории
        dirs[:] = [d for d in dirs if d not in ['.git', '__pycache__', 'venv']]
        
        for file in files:
            if file.endswith('.py'):
                # Получаем относительный путь
                rel_path = os.path.relpath(os.path.join(root, file), source_dir)
                if should_update_file(rel_path):
                    python_files.append(rel_path)
    
    return sorted(python_files)

def update_file_from_source(source_file, target_file):
    """Обновляет файл из исходного репозитория"""
    try:
        # Создаем директории, если их нет
        os.makedirs(os.path.dirname(target_file), exist_ok=True)
        
        # Создаем резервную копию
        backup_path = backup_file(target_file)
        
        # Копируем файл
        shutil.copy2(source_file, target_file)
        
        log(f"Обновлен файл: {target_file}")
        if backup_path:
            log(f"Резервная копия: {backup_path}")
        
        return True
    except Exception as e:
        log(f"Ошибка обновления файла {target_file}: {e}", "ERROR")
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
    
    # Проверяем наличие git
    if not shutil.which('git'):
        log("❌ Git не найден. Установите Git для работы скрипта.", "ERROR")
        return False
    
    # Создаем временную директорию
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(prefix="tg-ytdlp-update-")
        log(f"📁 Создана временная директория: {temp_dir}")
        
        # Клонируем репозиторий
        if not clone_repository(temp_dir):
            return False
        
        # Находим Python файлы
        python_files = find_python_files(temp_dir)
        
        if not python_files:
            log("❌ Не найдено Python файлов для обновления", "ERROR")
            return False
        
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
            
            source_file = os.path.join(temp_dir, file_path)
            target_file = file_path
            
            if update_file_from_source(source_file, target_file):
                updated_count += 1
            else:
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
            
    except Exception as e:
        log(f"❌ Критическая ошибка: {e}", "ERROR")
        return False
    
    finally:
        # Удаляем временную директорию
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                log(f"🗑️ Временная директория удалена: {temp_dir}")
            except Exception as e:
                log(f"⚠️ Не удалось удалить временную директорию: {e}", "WARNING")

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
