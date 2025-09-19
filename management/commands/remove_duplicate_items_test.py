from datetime import datetime

from django.core.management.base import BaseCommand
from kenny.items.models import Competitor, Item

from linked.models import RecommendedLinked


class Command(BaseCommand):
    help = 'Удаляет товары с query-параметрами в URL, имеющие дубликаты по артикулу у указанного конкурента'

    def add_arguments(self, parser):
        parser.add_argument('competitor_id', type=int, help='ID конкурента')
        parser.add_argument('--preview-file', type=str, help='Путь к файлу для сохранения предварительного просмотра')

    def handle(self, *args, **options):
        competitor_id = options['competitor_id']
        preview_file_path = options.get('preview_file')

        # Если путь к файлу не указан, создаем автоматическое имя
        if not preview_file_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            preview_file_path = f'preview_deletion_{competitor_id}_{timestamp}.txt'

        self.stdout.write('=== НАЧАЛО ПРОЦЕДУРЫ УДАЛЕНИЯ ДУБЛИКАТОВ ===')

        # Шаг 1: Поиск конкурента
        self.stdout.write(f'\n1. Поиск конкурента с ID {competitor_id}...')
        try:
            competitor = Competitor.objects.get(id=competitor_id)
            self.stdout.write(self.style.SUCCESS(f'   Найден конкурент: {competitor.name} (ID: {competitor.id})'))
        except Competitor.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'Конкурент с ID {competitor_id} не найден'))
            return

        # Шаг 2: Получение всех товаров конкурента
        self.stdout.write('\n2. Получение всех товаров конкурента...')
        items = Item.objects.filter(competitor=competitor)
        self.stdout.write(f'   Всего товаров у конкурента: {items.count()}')

        # Шаг 3: Группировка товаров по нормализованным артикулам (без пробелов)
        self.stdout.write('\n3. Группировка товаров по нормализованным артикулам...')
        normalized_articles = {}

        for item in items:
            # Нормализуем артикул - убираем пробелы в начале и конце
            normalized_article = item.article.strip()

            if normalized_article not in normalized_articles:
                normalized_articles[normalized_article] = []

            normalized_articles[normalized_article].append(item)

        # Шаг 4: Поиск дубликатов
        self.stdout.write('\n4. Поиск дубликатов артикулов...')
        duplicate_articles = {}

        for article, items_list in normalized_articles.items():
            if len(items_list) > 1:
                duplicate_articles[article] = items_list

        self.stdout.write(f'   Найдено артикулов с дубликатами: {len(duplicate_articles)}')

        # Шаг 5: Поиск товаров с query-параметрами среди дубликатов
        self.stdout.write('\n5. Поиск товаров с query-параметрами среди дубликатов...')
        items_to_delete = []
        items_to_keep = []

        # Создаем список для хранения детальной информации о каждом артикуле
        detailed_article_info = []

        for article, items_list in duplicate_articles.items():
            clean_article = article.strip()
            variants = list(set(i.article for i in items_list))

            # Добавляем информацию в список для файла
            detailed_article_info.append(f"Артикул: '{clean_article}'")
            detailed_article_info.append(f'Варианты написания: {variants}')
            detailed_article_info.append(f'Всего товаров: {len(items_list)}')

            # Сортируем товары по дате создания (новые первыми)
            sorted_items = sorted(items_list, key=lambda x: x.date_create, reverse=True)

            # Ищем товар без параметров для сохранения
            kept_item = None
            for item in sorted_items:
                if '?' not in item.url and '%3F' not in item.url:
                    kept_item = item
                    items_to_keep.append(item)
                    detailed_article_info.append(f'Сохраняемый товар (без параметров): {item.id} - {item.url}')
                    break

            # Если нет товара без параметров, сохраняем самый старый
            if kept_item is None:
                kept_item = sorted_items[-1]  # Самый старый товар
                items_to_keep.append(kept_item)
                detailed_article_info.append(f'Сохраняемый товар (самый старый): {kept_item.id} - {kept_item.url}')

            # Добавляем товары с параметрами для удаления
            for item in sorted_items:
                if ('?' in item.url or '%3F' in item.url) and item != kept_item:
                    items_to_delete.append(item)
                    detailed_article_info.append(f'К удалению: {item.id} - {item.url}')

            # Добавляем разделитель между артикулами
            detailed_article_info.append('')

        # Шаг 6: Поиск рекомендаций для удаляемых товаров
        self.stdout.write('\n6. Поиск рекомендаций для удаляемых товаров...')
        item_ids_to_delete = [item.id for item in items_to_delete]

        if item_ids_to_delete:
            recommendations_to_delete = RecommendedLinked.objects.filter(item_id__in=item_ids_to_delete)
            self.stdout.write(f'   Найдено рекомендаций для удаления: {recommendations_to_delete.count()}')
        else:
            recommendations_to_delete = RecommendedLinked.objects.none()
            self.stdout.write('   Нет рекомендаций для удаления')

        # Шаг 7: Запись предварительного просмотра в файл
        self.stdout.write('\n7. ЗАПИСЬ ПРЕДВАРИТЕЛЬНОГО ПРОСМОТРА В ФАЙЛ...')

        try:
            with open(preview_file_path, 'w', encoding='utf-8') as f:
                f.write('=== ПРЕДВАРИТЕЛЬНЫЙ ПРОСМОТР УДАЛЕНИЯ ===\n\n')
                f.write(f'Конкурент: {competitor.name} (ID: {competitor.id})\n')
                f.write(f"Дата формирования отчета: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                f.write(f'Всего товаров будет удалено: {len(items_to_delete)}\n')
                f.write(f'Всего рекомендаций будет удалено: {recommendations_to_delete.count()}\n')
                f.write(f'Всего товаров будет сохранено: {len(items_to_keep)}\n\n')

                # Записываем детальную информацию о каждом артикуле
                f.write('ДЕТАЛЬНАЯ ИНФОРМАЦИЯ ПО АРТИКУЛАМ:\n')
                f.write('=' * 80 + '\n\n')
                for line in detailed_article_info:
                    f.write(line + '\n')

                if items_to_delete:
                    f.write('\nСВОДКА ПО ТОВАРАМ ДЛЯ УДАЛЕНИЯ:\n')
                    f.write('=' * 80 + '\n')
                    for item in items_to_delete:
                        f.write(f'ID: {item.id}\n')
                        f.write(f"Артикул: '{item.article}'\n")
                        f.write(f'URL: {item.url}\n')
                        f.write(f'Дата создания: {item.date_create}\n')
                        f.write('-' * 80 + '\n')

                if recommendations_to_delete.exists():
                    f.write('\nРЕКОМЕНДАЦИИ ДЛЯ УДАЛЕНИЯ:\n')
                    f.write('=' * 80 + '\n')
                    for rec in recommendations_to_delete:
                        f.write(f'ID рекомендации: {rec.id}\n')
                        f.write(f'ID товара: {rec.item_id}\n')
                        f.write(f'Код номенклатуры: {rec.nomenclature_code}\n')
                        f.write(f'Источник: {rec.source}\n')
                        f.write('-' * 80 + '\n')

            self.stdout.write(
                self.style.SUCCESS(f'   Предварительный просмотр сохранен в файл: {preview_file_path}'))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   Ошибка при записи в файл: {e}'))
            return

        # Шаг 8: Подтверждение удаления
        self.stdout.write('\n8. ПОДТВЕРЖДЕНИЕ УДАЛЕНИЯ:')
        if not items_to_delete and not recommendations_to_delete:
            self.stdout.write('   Нет данных для удаления.')
            return

        confirm = input('   Вы уверены, что хотите удалить эти товары и рекомендации? (y/n): ')

        if confirm.lower() != 'y':
            self.stdout.write('   Удаление отменено.')
            return

        # Шаг 9: Удаление рекомендаций
        self.stdout.write('\n9. УДАЛЕНИЕ РЕКОМЕНДАЦИЙ...')
        if recommendations_to_delete.exists():
            rec_deleted_count, _ = recommendations_to_delete.delete()
            self.stdout.write(f'   Удалено рекомендаций: {rec_deleted_count}')
        else:
            self.stdout.write('   Нет рекомендаций для удаления')

        # Шаг 10: Удаление товаров
        self.stdout.write('\n10. УДАЛЕНИЕ ТОВАРОВ...')
        if items_to_delete:
            deleted_count = Item.objects.filter(id__in=item_ids_to_delete).delete()[0]
            self.stdout.write(f'   Удалено товаров: {deleted_count}')
        else:
            self.stdout.write('   Нет товаров для удаления')

        self.stdout.write(self.style.SUCCESS(
            '\n=== УДАЛЕНИЕ ЗАВЕРШЕНО ===',
        ))
        self.stdout.write(self.style.SUCCESS(
            f'Удалено товаров: {len(items_to_delete)}',
        ))
        self.stdout.write(self.style.SUCCESS(
            f'Удалено рекомендаций: {recommendations_to_delete.count()}',
        ))
        self.stdout.write(self.style.SUCCESS(
            f'Сохранено товаров: {len(items_to_keep)}',
        ))

# Запустите команду:
# python manage.py remove_duplicate_items_prod 1  # где 1 - ID конкурента
# python manage.py remove_duplicate_items_prod 142  # где 142 - ID Комус
