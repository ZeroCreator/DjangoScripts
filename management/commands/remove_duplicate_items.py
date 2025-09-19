"""
Скрипт удаления дублирующихся товаров по артикулу у конкурента.

НАЗНАЧЕНИЕ:
- Обнаруживает и удаляет дубликаты товаров по артикулу у указанного конкурента
- Сохраняет товары с пробелом в начале артикула, удаляет товары без пробела
- Создает детальный отчет о выполненных операциях

ПРОБЛЕМА, КОТОРУЮ РЕШАЕТ:
У конкурентов часто встречаются дубликаты товаров с одинаковыми артикулами,
но разным написанием (с пробелом в начале и без). Это мешает корректному
сопоставлению товаров и анализу цен.

АЛГОРИТМ РАБОТЫ:
1. Группировка товаров по нормализованному артикулу (без пробелов по краям)
2. Поиск артикулов с дубликатами (> 1 товара)
3. Для каждой группы дубликатов:
   - При наличии товара с пробелом в начале: оставить его, удалить остальные
   - При отсутствии товаров с пробелом: оставить самый новый, удалить старые
4. Создание отчета с детализацией операций

КРИТЕРИИ УДАЛЕНИЯ:
- Удаляются товары БЕЗ пробела в начале артикула
- Сохраняются товары С пробелом в начале артикула
- При отсутствии товаров с пробелом: сохраняется самый новый товар

РЕЖИМЫ РАБОТЫ:
--dry-run     : Предварительный просмотр без реального удаления
--output      : Сохранение детального отчета в файл
Без параметров: Реальное выполнение удаления с подтверждением

МЕРЫ ПРЕДОСТОРОЖНОСТИ:
- Требует подтверждения перед удалением
- Создает резервную копию в виде отчета
- Предоставляет возможность предварительного просмотра (dry-run)
- Точечно удаляет только дубликаты, не затрагивая уникальные товары

ВХОДНЫЕ ДАННЫЕ:
- ID конкурента
- Опциональные флаги dry-run и output

ВЫХОДНЫЕ ДАННЫЕ:
- Статистика выполнения в консоли
- Детальный отчет в файле (при использовании --output)
- Логирование всех операций

ИСПОЛЬЗОВАНИЕ В CI/CD:
Может быть интегрирован в процессы регулярной очистки данных конкурентов
для поддержания качества данных в системе.

АВТОМАТИЗАЦИЯ:
Рекомендуется сначала запускать с --dry-run и --output для проверки,
затем выполнять реальное удаление после анализа отчета.
"""

import os
import sys
from django.core.management.base import BaseCommand
from kenny.items.models import Competitor, Item
from datetime import datetime


class Command(BaseCommand):
    help = 'Удаляет дубликаты товаров по артикулу у указанного конкурента, оставляя товар с пробелом в начале артикула'

    def add_arguments(self, parser):
        parser.add_argument('competitor_id', type=int, help='ID конкурента')
        parser.add_argument('--dry-run', action='store_true', help='Только показать что будет удалено, без удаления')
        parser.add_argument('--output', action='store_true', help='Сохранить отчет в файл в корне проекта')

    def safe_input(self, prompt):
        """Безопасный ввод с обработкой проблем кодировки"""
        try:
            # Пытаемся прочитать ввод обычным способом
            return input(prompt)
        except (UnicodeDecodeError, EOFError):
            # Если возникает ошибка кодировки, используем альтернативный метод
            self.stdout.write(prompt)
            self.stdout.flush()
            line = sys.stdin.readline()
            return line.strip()

    def handle(self, *args, **options):
        competitor_id = options['competitor_id']
        dry_run = options['dry_run']
        save_output = options['output']

        self.stdout.write('=== НАЧАЛО ПРОЦЕДУРЫ УДАЛЕНИЯ ДУБЛИКАТОВ ПО АРТИКУЛУ ===')
        if dry_run:
            self.stdout.write(self.style.WARNING('РЕЖИМ ПРОСМОТРА (dry-run) - удаление не будет выполнено'))

        # Шаг 1: Поиск конкурента
        self.stdout.write(f'1. Поиск конкурента с ID {competitor_id}...')
        try:
            competitor = Competitor.objects.get(id=competitor_id)
            self.stdout.write(self.style.SUCCESS(f'   Найден конкурент: {competitor.name} (ID: {competitor.id})'))
        except Competitor.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'Конкурент с ID {competitor_id} не найден'))
            return

        # Шаг 2: Получение всех товаров конкурента
        self.stdout.write('2. Получение всех товаров конкурента...')
        items = list(Item.objects.filter(competitor=competitor))
        self.stdout.write(self.style.SUCCESS(f'   Всего товаров у конкурента: {len(items)}'))

        # Шаг 3: Нормализация артикула и группировка
        self.stdout.write('3. Нормализация и группировка товаров по артикулу...')
        normalized_articles = {}
        article_variations = {}  # Для отслеживания всех вариантов написания

        for item in items:
            # Нормализуем артикул - убираем пробелы с обеих сторон для группировки
            normalized_article = item.article.strip() if item.article else ''

            if not normalized_article:
                continue  # Пропускаем товары без артикула

            if normalized_article not in normalized_articles:
                normalized_articles[normalized_article] = []
                article_variations[normalized_article] = set()

            normalized_articles[normalized_article].append(item)
            # Сохраняем оригинальное написание для анализа пробелов
            article_variations[normalized_article].add(item.article)

        # Шаг 4: Поиск дубликатов
        duplicate_articles = {article: lst for article, lst in normalized_articles.items() if len(lst) > 1}
        self.stdout.write(f'4. Найдено артикулов с дубликатами: {len(duplicate_articles)}')

        if not duplicate_articles:
            self.stdout.write('Дубликатов не найдено.')
            return

        items_to_delete = []
        report_data = []

        # Шаг 5: Для каждого дублирующего артикула определяем что удалять
        self.stdout.write('5. Анализ дубликатов...')
        for article, duplicates_list in duplicate_articles.items():
            # Ищем товар С пробелом в начале артикула (который нужно оставить)
            items_with_leading_space = []
            items_without_leading_space = []

            for item in duplicates_list:
                if item.article and item.article.startswith(' '):
                    items_with_leading_space.append(item)
                else:
                    items_without_leading_space.append(item)

            # Логика удаления:
            # 1. Если есть товар с пробелом - оставляем его, удаляем все без пробела
            # 2. Если нет товара с пробелом, но есть несколько без пробела - оставляем самый новый

            if items_with_leading_space:
                # Оставляем первый товар с пробелом (обычно он один)
                item_to_keep = items_with_leading_space[0]
                items_to_delete.extend(items_without_leading_space)
                delete_reason = "не имеет пробела в начале артикула"
            else:
                # Если все товары без пробела в начале, оставляем самый новый
                sorted_items = sorted(duplicates_list, key=lambda x: x.date_create, reverse=True)
                item_to_keep = sorted_items[0]
                items_to_delete.extend(sorted_items[1:])
                delete_reason = "более старый товар без пробела в артикуле"

            # Собираем информацию для отчета
            variations = list(article_variations[article])
            report_entry = {
                'article': article,
                'total_count': len(duplicates_list),
                'delete_count': len(items_without_leading_space) if items_with_leading_space else len(
                    duplicates_list) - 1,
                'variations': variations,
                'keep_item': {
                    'id': item_to_keep.id,
                    'article': item_to_keep.article,
                    'date_create': item_to_keep.date_create,
                    'name': item_to_keep.name[:50] + '...' if item_to_keep.name and len(
                        item_to_keep.name) > 50 else item_to_keep.name,
                    'has_leading_space': item_to_keep.article.startswith(' ') if item_to_keep.article else False
                },
                'delete_items': [],
                'delete_reason': delete_reason
            }

            # Добавляем товары для удаления в отчет
            for item in items_without_leading_space if items_with_leading_space else duplicates_list[1:]:
                if item.id != item_to_keep.id:  # Убедимся, что не добавляем товар для сохранения
                    report_entry['delete_items'].append({
                        'id': item.id,
                        'article': item.article,
                        'date_create': item.date_create,
                        'name': item.name[:50] + '...' if item.name and len(item.name) > 50 else item.name,
                        'has_leading_space': item.article.startswith(' ') if item.article else False,
                        'reason': "не имеет пробела в начале артикула" if item.article and not item.article.startswith(
                            ' ') else "более старый товар"
                    })

            report_data.append(report_entry)

        self.stdout.write(self.style.SUCCESS(f'   Проанализировано дубликатов: {len(duplicate_articles)}'))
        self.stdout.write(self.style.SUCCESS(f'   Товаров к удалению: {len(items_to_delete)}'))

        # Шаг 6: Создание отчета
        output_file = None
        if save_output:
            # Определяем корень Django проекта
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"duplicates_report_{competitor_id}_{timestamp}.txt"
            output_file = os.path.join(base_dir, filename)

            self.stdout.write(f'6. Создание отчета в файле: {output_file}')
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(f'Отчет об удалении дубликатов для конкурента: {competitor.name}\n')
                    f.write(f'Дата создания: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
                    f.write(f'Всего дубликатов для удаления: {len(items_to_delete)}\n')
                    f.write('Критерий: удаляем товары БЕЗ пробела в начале артикула\n')
                    f.write('=' * 80 + '\n\n')

                    for entry in report_data:
                        variations_str = ', '.join([f'"{v}"' for v in entry['variations']])
                        f.write(f"Артикул: {entry['article']}\n")
                        f.write(f"Варианты написания: {variations_str}\n")
                        f.write(f"Всего товаров: {entry['total_count']}, удаляется: {entry['delete_count']}\n")

                        f.write("Оставляем товар:\n")
                        f.write(f"  ID: {entry['keep_item']['id']}\n")
                        f.write(f"  Артикул: '{entry['keep_item']['article']}'")
                        if entry['keep_item']['has_leading_space']:
                            f.write(" (С ПРОБЕЛОМ - СОХРАНЯЕМ!)")
                        f.write("\n")
                        f.write(f"  Дата создания: {entry['keep_item']['date_create']}\n")
                        f.write(f"  Название: {entry['keep_item']['name']}\n")

                        f.write("Удаляем товары:\n")
                        for del_item in entry['delete_items']:
                            f.write(f"  ID: {del_item['id']}\n")
                            f.write(f"  Артикул: '{del_item['article']}'")
                            if not del_item['has_leading_space']:
                                f.write(" (БЕЗ ПРОБЕЛА - УДАЛЯЕМ!)")
                            f.write("\n")
                            f.write(f"  Дата создания: {del_item['date_create']}\n")
                            f.write(f"  Название: {del_item['name']}\n")
                            f.write(f"  Причина удаления: {del_item['reason']}\n")
                            f.write("  ---\n")

                        f.write("\n" + "-" * 40 + "\n\n")

                self.stdout.write(self.style.SUCCESS(f'Отчет сохранен в файл: {filename}'))
                self.stdout.write(f'Полный путь: {os.path.abspath(output_file)}')

            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Ошибка при создании отчета: {e}'))
                import traceback
                self.stdout.write(self.style.ERROR(f'Трассировка: {traceback.format_exc()}'))

        # Шаг 7: Подтверждение и удаление
        self.stdout.write(f'\n7. Итого товаров к удалению: {len(items_to_delete)}')

        if dry_run:
            self.stdout.write(self.style.WARNING('Режим dry-run: удаление не выполнено'))
            return

        if not items_to_delete:
            self.stdout.write('Нет товаров для удаления.')
            return

        # Используем безопасный ввод
        confirm = self.safe_input('Вы уверены, что хотите удалить эти товары? (y/n): ')
        if confirm.lower() != 'y':
            self.stdout.write('Удаление отменено.')
            return

        # Удаление
        self.stdout.write('8. Выполнение удаления...')
        delete_ids = [item.id for item in items_to_delete]

        # Удаляем порциями, чтобы избежать проблем с большим количеством записей
        batch_size = 1000
        deleted_items_count = 0
        total_deleted_objects = 0  # для отслеживания общего количества удаленных объектов

        for i in range(0, len(delete_ids), batch_size):
            batch_ids = delete_ids[i:i + batch_size]
            deleted_info = Item.objects.filter(id__in=batch_ids).delete()

            # deleted_info[0] - общее количество удаленных объектов
            # deleted_info[1] - словарь с количеством по моделям
            items_deleted_in_batch = deleted_info[1].get('kenny.items.Item', 0)

            deleted_items_count += items_deleted_in_batch
            total_deleted_objects += deleted_info[0]

            self.stdout.write(f'   Удалено товаров: {deleted_items_count}/{len(delete_ids)}')
            self.stdout.write(f'   Всего удалено объектов: {total_deleted_objects}')

        self.stdout.write(self.style.SUCCESS(f'Удалено товаров: {deleted_items_count}'))
        self.stdout.write(self.style.SUCCESS(f'Всего удалено объектов в БД: {total_deleted_objects}'))

        self.stdout.write(self.style.SUCCESS(f'Удалено товаров: {deleted_items_count}'))
        self.stdout.write(self.style.SUCCESS(f'Всего удалено объектов в БД: {total_deleted_objects}'))

        ## Сохранение информации об удалении в лог
        if output_file:
            try:
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(f'\nРЕЗУЛЬТАТ УДАЛЕНИЯ:\n')
                    f.write(f'Удалено товаров: {deleted_items_count}\n')  # ← Исправлено
                    f.write(f'Всего удалено объектов в БД: {total_deleted_objects}\n')  # ← Добавлено
                    f.write(f'Дата удаления: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
            except Exception as e:
                self.stdout.write(self.style.WARNING(f'Не удалось добавить результат в отчет: {e}'))

# Запустите команду:
# python manage.py remove_duplicate_items 1  # где 1 - ID конкурента
# python manage.py remove_duplicate_items 142  # где 142 - ID Комус
# python manage.py remove_duplicate_items 142 --output  # для сохранения отчета
