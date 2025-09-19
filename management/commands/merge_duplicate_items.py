from datetime import datetime

from django.core.management.base import BaseCommand
from django.db.models import Count
from kenny.items.models import Competitor, Item


class Command(BaseCommand):
    help = 'Объединяет дубликаты товаров, перенося историю на товар с наибольшим количеством записей'

    def add_arguments(self, parser):
        parser.add_argument('competitor_id', type=int, help='ID конкурента')
        parser.add_argument('--preview-file', type=str, help='Путь к файлу для сохранения предварительного просмотра')
        parser.add_argument('--batch-size', type=int, default=500, help='Размер батча для обработки')
        parser.add_argument('--limit', type=int, help='Ограничение количества обрабатываемых артикулов')

    def handle(self, *args, **options):
        competitor_id = options['competitor_id']
        preview_file_path = options.get('preview_file')
        batch_size = options.get('batch_size', 500)
        limit = options.get('limit')

        if not preview_file_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            preview_file_path = f'preview_merge_{competitor_id}_{timestamp}.txt'

        self.stdout.write('=== НАЧАЛО ПРОЦЕДУРЫ ОБЪЕДИНЕНИЯ ДУБЛИКАТОВ ===')

        try:
            competitor = Competitor.objects.get(id=competitor_id)
            self.stdout.write(self.style.SUCCESS(f'Найден конкурент: {competitor.name}'))
        except Competitor.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'Конкурент с ID {competitor_id} не найден'))
            return

        # Шаг 1: Находим артикулы с дубликатами
        self.stdout.write('Поиск артикулов с дубликатами...')

        # Получаем все товары конкурента
        items = Item.objects.filter(competitor=competitor)

        # Группируем по нормализованным артикулам в памяти
        normalized_articles = {}
        for item in items:
            # Нормализуем артикул в Python
            normalized_article = item.article.strip().lower()
            if normalized_article not in normalized_articles:
                normalized_articles[normalized_article] = []
            normalized_articles[normalized_article].append(item)

        # Фильтруем только дубликаты
        duplicate_articles = {k: v for k, v in normalized_articles.items() if len(v) > 1}

        if limit:
            # Берем только первые limit артикулов
            duplicate_articles = dict(list(duplicate_articles.items())[:limit])

        total_duplicates = len(duplicate_articles)
        self.stdout.write(f'Найдено артикулов с дубликатами: {total_duplicates}')

        if total_duplicates == 0:
            self.stdout.write('Нет дубликатов для обработки')
            return

        # Шаг 2: Для каждого артикула определяем товар с наибольшей историей
        self.stdout.write('Определение товаров с наибольшей историей...')

        merge_candidates = []
        detailed_article_info = []

        # Получаем ID всех товаров, которые могут быть дубликатами
        all_item_ids = []
        for items_list in duplicate_articles.values():
            all_item_ids.extend([item.id for item in items_list])

        # Получаем количество истории для каждого товара одним запросом
        items_with_history = Item.objects.filter(id__in=all_item_ids).annotate(
            history_count=Count('history_info'),
        )

        # Создаем словарь для быстрого доступа
        history_count_map = {item.id: item.history_count for item in items_with_history}

        # Формируем список для объединения
        for article, items_list in duplicate_articles.items():
            # Добавляем количество истории к каждому товару
            for item in items_list:
                item.history_count = history_count_map.get(item.id, 0)

            # Сортируем по количеству истории (по убыванию)
            sorted_items = sorted(items_list, key=lambda x: x.history_count, reverse=True)

            master_item = sorted_items[0]
            slave_items = sorted_items[1:]

            merge_candidates.append((master_item, slave_items))

            # Добавляем информацию для отчета
            detailed_article_info.append(f"Артикул: '{article}'")
            detailed_article_info.append(f'Мастер-товар: {master_item.id} (историй: {master_item.history_count})')
            for item in slave_items:
                detailed_article_info.append(f'Подчинённый товар: {item.id} (историй: {item.history_count})')
            detailed_article_info.append('')

            # Выводим прогресс
            if len(merge_candidates) % 100 == 0:
                self.stdout.write(f'Обработано артикулов: {len(merge_candidates)}/{total_duplicates}')

        # Запись предварительного просмотра
        try:
            with open(preview_file_path, 'w', encoding='utf-8') as f:
                f.write('=== ПРЕДВАРИТЕЛЬНЫЙ ПРОСМОТР ОБЪЕДИНЕНИЯ ===\n\n')
                f.write(f'Конкурент: {competitor.name}\n')
                f.write(f'Дата формирования отчета: {datetime.now()}\n\n')
                f.write(f'Всего артикулов для объединения: {len(merge_candidates)}\n\n')

                for line in detailed_article_info:
                    f.write(line + '\n')

            self.stdout.write(self.style.SUCCESS(f'Предварительный просмотр сохранен в: {preview_file_path}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка при записи файла: {e}'))
            return

        # Подтверждение выполнения
        # confirm = input(f"Вы уверены, что хотите объединить {len(merge_candidates)} артикулов? (y/n): ")
        # if confirm.lower() != 'y':
        #     self.stdout.write("Объединение отменено.")
        #     return
        #
        # # Шаг 3: Объединение данных
        # self.stdout.write("Начинаем объединение...")
        #
        # with transaction.atomic():
        #     total_merged = 0
        #     for i, (master_item, slave_items) in enumerate(merge_candidates):
        #         for slave_item in slave_items:
        #             # Перенос истории
        #             ItemInfoHistory.objects.filter(item=slave_item).update(item=master_item)
        #
        #             # Перенос рекомендаций
        #             RecommendedLinked.objects.filter(item=slave_item).update(item=master_item)
        #
        #             # Удаление подчинённого товара
        #             slave_item.delete()
        #             total_merged += 1
        #
        #         # Выводим прогресс каждые 100 артикулов
        #         if i % 100 == 0:
        #             self.stdout.write(f"Объединено артикулов: {i}/{len(merge_candidates)}")
        #
        # self.stdout.write(self.style.SUCCESS(f"Все дубликаты успешно объединены! Объединено товаров: {total_merged}"))


# Запустите команду:
# python manage.py merge_duplicate_items 1  # где 1 - ID конкурента
# python manage.py merge_duplicate_items 142  # где 142 - ID Комус
