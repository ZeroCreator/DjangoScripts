#!/usr/bin/env python3
import subprocess
import os
from pathlib import Path


def check_chrome_version():
    # Путь к Chrome в проекте
    project_root = Path.home() / 'Cartman' / 'cartman'
    chrome_path = project_root / 'chrome' / 'chrome-linux' / 'chrome'

    print(f"Chrome path: {chrome_path}")
    print(f"Exists: {chrome_path.exists()}")

    if not chrome_path.exists():
        print("❌ Chrome not found!")
        return

    # Проверить размер файла
    size_mb = chrome_path.stat().st_size / (1024 * 1024)
    print(f"Size: {size_mb:.1f} MB")

    # Проверить версию
    try:
        result = subprocess.run(
            [str(chrome_path), '--version'],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode == 0:
            version = result.stdout.strip()
            print(f"✅ Chrome version: {version}")

            # Проверить что это не старый Chromium 116
            if 'Chromium 116' in version:
                print("❌ STILL OLD Chromium 116!")
            elif '128.' in version or '129.' in version:
                print("✅ Good! Modern Chrome detected")
            else:
                print(f"⚠ Unknown version")
        else:
            print(f"❌ Error: {result.stderr}")

    except Exception as e:
        print(f"❌ Cannot run Chrome: {e}")


if __name__ == '__main__':
    check_chrome_version()