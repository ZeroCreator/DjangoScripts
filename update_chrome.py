#!/usr/bin/env python3
"""
Обновить ТОЛЬКО Chrome браузер, не трогая chromedriver.
"""

import os
import shutil
import tempfile
import subprocess
from pathlib import Path


def update_chrome_only():
    """Обновить Chrome браузер в проекте."""

    project_root = Path.cwd()
    chrome_dir = project_root / "chrome" / "chrome-linux"
    chromedriver_dir = project_root / "chrome" / "chromedriver-linux64"

    print("=" * 60)
    print("Обновление Chrome браузера (сохраняем ChromeDriver)")
    print("=" * 60)

    # Проверить что chromedriver на месте
    if not chromedriver_dir.exists():
        print(f"⚠ ChromeDriver не найден: {chromedriver_dir}")
        response = input("Продолжить? (y/n): ")
        if response.lower() != 'y':
            return False

    # Проверить текущий Chrome
    chrome_binary = chrome_dir / "chrome"
    if chrome_binary.exists():
        try:
            result = subprocess.run(
                [chrome_binary, '--version'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print(f"Текущий Chrome: {result.stdout.strip()}")
        except:
            pass

    # Удалить старый Chrome (браузер)
    if chrome_dir.exists():
        print(f"Удаляю старый Chrome: {chrome_dir}")
        shutil.rmtree(chrome_dir)

    # Создать временную папку
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        print("Скачиваю новый Chrome...")

        # Скачать Chrome
        deb_file = temp_path / "chrome.deb"
        chrome_url = "https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb"

        # Используем curl или wget
        import urllib.request
        urllib.request.urlretrieve(chrome_url, deb_file)

        # Распаковать deb
        print("Распаковываю...")
        subprocess.run(["ar", "x", deb_file], cwd=temp_path, check=True)

        # Извлечь data.tar.xz
        data_tar = temp_path / "data.tar.xz"
        if data_tar.exists():
            subprocess.run(["tar", "-xf", "data.tar.xz"], cwd=temp_path, check=True)

        # Найти Chrome в распакованных файлах
        chrome_source = temp_path / "opt" / "google" / "chrome"

        if chrome_source.exists():
            # Скопировать в проект
            print(f"Копирую в проект: {chrome_dir}")
            shutil.copytree(chrome_source, chrome_dir)

            # Дать права
            chrome_binary = chrome_dir / "chrome"
            if chrome_binary.exists():
                os.chmod(chrome_binary, 0o755)

                # Проверить
                result = subprocess.run(
                    [chrome_binary, '--version'],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    print(f"✅ Новый Chrome: {result.stdout.strip()}")
                    return True
                else:
                    print("❌ Не удалось проверить новую версию")
                    return False
            else:
                print("❌ Chrome не найден в распакованных файлах")
                return False
        else:
            print("❌ Не удалось найти Chrome в архиве")
            return False


def main():
    print("Скрипт обновления Chrome браузера")
    print("Сохраняет ChromeDriver для других парсеров")
    print("-" * 60)

    # Проверить что мы в правильной директории
    if not (Path.cwd() / "chrome").exists():
        print("❌ Не найден каталог chrome/")
        print("Запустите скрипт из корня проекта")
        return

    if update_chrome_only():
        print("\n" + "=" * 60)
        print("✅ Chrome браузер успешно обновлен!")
        print("✅ ChromeDriver сохранен!")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("❌ Обновление не удалось")
        print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Прервано пользователем")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback

        traceback.print_exc()