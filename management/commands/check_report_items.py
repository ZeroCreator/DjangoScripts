"""
Скрипт проверки сохранности товаров после процедуры удаления дубликатов.

НАЗНАЧЕНИЕ:
- Проверяет, что товары, которые должны были быть сохранены согласно отчету
  об удалении дубликатов, действительно присутствуют в базе данных
- Создает детальный отчет о результатах проверки

КОГДА ИСПОЛЬЗОВАТЬ:
- После выполнения команды remove_old_duplicate_items
- Для аудита результатов удаления дубликатов
- Для подтверждения корректности работы скрипта удаления

ЧТО ПРОВЕРЯЕТ:
1. Извлекает все ID товаров из раздела "Оставляем товар" в отчете
2. Проверяет наличие этих товаров в базе данных
3. Формирует статистику сохранности товаров
4. Выявляет пропавшие товары (если есть)

ВХОДНЫЕ ДАННЫЕ:
- Файл отчета duplicates_report_*.txt созданный скриптом удаления дубликатов

ВЫХОДНЫЕ ДАННЫЕ:
- Статистика в консоли
- Подробный отчет в файле (при использовании --output)

АВТОМАТИЗАЦИЯ:
Может использоваться в CI/CD процессах для проверки корректности выполнения
операций с товарами конкурентов.
"""
import re
import os
from datetime import datetime
from django.core.management.base import BaseCommand
from kenny.items.models import Item


class Command(BaseCommand):
    help = 'Проверяет сохраненные товары из файла отчета на наличие в базе данных'

    def add_arguments(self, parser):
        parser.add_argument('report_file', type=str, help='Путь к файлу отчета')
        parser.add_argument('--output', action='store_true', help='Сохранить полный отчет в файл')

    def handle(self, *args, **options):
        report_file = options['report_file']
        save_output = options['output']

        self.stdout.write('=== ПРОВЕРКА СОХРАНЕННЫХ ТОВАРОВ ИЗ ОТЧЕТА ===')
        self.stdout.write(f'Файл отчета: {report_file}')

        # Создаем файл для вывода если нужно
        output_file = None
        log_file = None
        if save_output:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f'check_report_{timestamp}.log'
            output_file = os.path.join(os.path.dirname(report_file), output_filename)

            try:
                log_file = open(output_file, 'w', encoding='utf-8')
                self.stdout.write(f'Файл логов создан: {output_file}')
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Ошибка создания файла логов: {e}'))
                log_file = None

        def write_output(message, style=None):
            """Универсальная функция для вывода в консоль и файл"""
            if style:
                formatted_message = style(message)
                self.stdout.write(formatted_message)
                if log_file:
                    log_file.write(f'{formatted_message}\n')
            else:
                self.stdout.write(message)
                if log_file:
                    log_file.write(f'{message}\n')

            if log_file:
                log_file.flush()

        try:
            with open(report_file, 'r', encoding='utf-8') as f:
                content = f.read()
        except FileNotFoundError:
            write_output(self.style.ERROR(f'Файл {report_file} не найден'))
            if log_file:
                log_file.close()
            return
        except Exception as e:
            write_output(self.style.ERROR(f'Ошибка чтения файла: {e}'))
            if log_file:
                log_file.close()
            return

        # Извлекаем только ID товаров из раздела "Оставляем товар"
        kept_item_ids = self.extract_kept_item_ids(content)

        write_output(f'Найдено ID сохраненных товаров в отчете: {len(kept_item_ids)}')

        if not kept_item_ids:
            write_output(self.style.WARNING('Не найдено ID сохраненных товаров в отчете'))
            if log_file:
                log_file.close()
            return

        # Проверяем сохраненные товары в базе данных
        self.check_kept_items_in_db(kept_item_ids, write_output)

        if log_file:
            write_output(f'\nПолный отчет сохранен в: {output_file}')
            log_file.close()

    def extract_kept_item_ids(self, content):
        """Извлекает ID товаров, которые должны были быть сохранены"""
        kept_ids = []

        # Ищем все вхождения "Оставляем товар:" и затем ID
        pattern = r'Оставляем товар:(.*?)(?=Удаляем товары:|\n\n|\Z)'
        keep_blocks = re.findall(pattern, content, re.DOTALL)

        for block in keep_blocks:
            # Ищем ID в блоке сохраненного товара
            id_match = re.search(r'ID: (\d+)', block)
            if id_match:
                kept_ids.append(int(id_match.group(1)))

        return kept_ids

    def check_kept_items_in_db(self, kept_item_ids, write_output):
        """Проверяет наличие сохраненных товаров в базе данных"""
        # Проверяем наличие товаров в базе
        existing_items = Item.objects.filter(id__in=kept_item_ids)
        existing_ids = set(existing_items.values_list('id', flat=True))
        missing_ids = set(kept_item_ids) - existing_ids

        write_output(f'\n--- РЕЗУЛЬТАТЫ ПРОВЕРКИ ---')
        write_output(f'Всего должно быть сохранено: {len(kept_item_ids)}')
        write_output(f'Найдено в базе: {len(existing_ids)}')
        write_output(f'Отсутствует в базе: {len(missing_ids)}')

        if missing_ids:
            write_output(self.style.ERROR(f'ПРОПАВШИЕ ТОВАРЫ ({len(missing_ids)}):'))
            # Покажем первые 20 пропавших ID
            for i, item_id in enumerate(sorted(list(missing_ids))[:20]):
                write_output(self.style.ERROR(f'  {i + 1}. ID: {item_id}'))
            if len(missing_ids) > 20:
                write_output(self.style.ERROR(f'  ... и еще {len(missing_ids) - 20} товаров'))

            # Записываем все пропавшие ID в файл
            if hasattr(write_output, 'log_file') and write_output.log_file:
                write_output.log_file.write('\nПОЛНЫЙ СПИСОК ПРОПАВШИХ ID:\n')
                for item_id in sorted(missing_ids):
                    write_output.log_file.write(f'{item_id}\n')

        # Простая проверка - только наличие товаров
        if existing_items:
            write_output(f'\n--- ИНФОРМАЦИЯ О НАЙДЕННЫХ ТОВАРАХ ---')
            write_output(f'Успешно сохранено товаров: {len(existing_ids)}')

            # Покажем несколько примеров
            sample_items = existing_items[:5]
            write_output('\nПримеры сохраненных товаров:')
            for i, item in enumerate(sample_items):
                article_display = f"'{item.article}'" if item.article else 'None'
                write_output(f'  {i + 1}. ID: {item.id}, Артикул: {article_display}')

        # Статистика
        write_output(f'\n--- СТАТИСТИКА ---')
        preservation_rate = (len(existing_ids) / len(kept_item_ids)) * 100 if kept_item_ids else 0

        write_output(f'Сохранено товаров: {len(existing_ids)}/{len(kept_item_ids)} ({preservation_rate:.1f}%)')

        if len(missing_ids) > 0:
            write_output(self.style.ERROR('\n❌ ВЫЯВЛЕНЫ ПРОБЛЕМЫ! Некоторые товары отсутствуют в базе!'))
        else:
            write_output(self.style.SUCCESS('\n✅ Все сохраненные товары на месте!'))

        # Дополнительная информация
        write_output(f'\n--- ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ ---')
        write_output(f'Время проверки: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        write_output(f'Проверено товаров: {len(kept_item_ids)}')

# Запуск команды:
# python manage.py check_report_items duplicates_report_142_20250919_180447.txt
# python manage.py check_report_items duplicates_report_142_20250919_180447.txt --output
