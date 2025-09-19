# Скрипт Django management command, который
# быстро находит дубликаты артикула с пробелами,
# группирует их по дате появления (от ранней к поздней) и
# сохраняет результат в файл
from datetime import datetime

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Max
from kenny.items.models import Competitor, Item, ItemInfo, ItemInfoHistory

from linked.models import RecommendedLinked


class Command(BaseCommand):
    help = 'Объединяет дубликаты товаров, перенося историю на товар с самой свежей информацией'

    def add_arguments(self, parser):
        parser.add_argument('competitor_id', type=int, help='ID конкурента')
        parser.add_argument('--preview-file', type=str, help='Путь к файлу для сохранения предварительного просмотра')
        parser.add_argument('--article', type=str, help='Конкретный артикул для обработки')
        parser.add_argument('--force', action='store_true', help='Выполнить без подтверждения')

    def handle(self, *args, **options):
        competitor_id = options['competitor_id']
        preview_file_path = options.get('preview_file')
        specific_article = options.get('article')
        force = options.get('force', False)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if not preview_file_path:
            preview_file_path = f'preview_merge_{competitor_id}_{timestamp}.txt'

        result_file_path = f'merge_result_{competitor_id}_{timestamp}.txt'

        self.stdout.write('=== НАЧАЛО ПРОЦЕДУРЫ ОБЪЕДИНЕНИЯ ДУБЛИКАТОВ ===')

        try:
            competitor = Competitor.objects.get(id=competitor_id)
            self.stdout.write(self.style.SUCCESS(f'Найден конкурент: {competitor.name}'))
        except Competitor.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'Конкурент с ID {competitor_id} не найден'))
            return

        # Получаем все товары конкурента
        items = Item.objects.filter(competitor=competitor)

        # Если указан конкретный артикул, фильтруем товары
        if specific_article:
            self.stdout.write(f'Поиск товаров с артикулом: {specific_article}')
            items = items.filter(article__icontains=specific_article)
            self.stdout.write(f'Найдено товаров: {items.count()}')

        # Группируем по нормализованным артикулам в памяти
        normalized_articles = {}
        for item in items:
            normalized_article = item.article.strip().lower()
            if normalized_article not in normalized_articles:
                normalized_articles[normalized_article] = []
            normalized_articles[normalized_article].append(item)

        # Фильтруем только дубликаты
        duplicate_articles = {k: v for k, v in normalized_articles.items() if len(v) > 1}

        # Если указан конкретный артикул, ищем его нормализованную версию
        if specific_article:
            normalized_specific = specific_article.strip().lower()
            if normalized_specific in duplicate_articles:
                duplicate_articles = {normalized_specific: duplicate_articles[normalized_specific]}
                self.stdout.write(
                    f'Найдены дубликаты для артикула {specific_article}: {len(duplicate_articles[normalized_specific])} товаров')
            else:
                self.stdout.write(f'Дубликаты для артикула {specific_article} не найдены')
                if normalized_specific in normalized_articles:
                    self.stdout.write(f'Найден 1 товар с артикулом {specific_article}, дубликатов нет')
                else:
                    self.stdout.write(f'Товаров с артикулом {specific_article} не найдено')
                return

        total_duplicates = len(duplicate_articles)
        self.stdout.write(f'Найдено артикулов с дубликатами: {total_duplicates}')

        if total_duplicates == 0:
            self.stdout.write('Нет дубликатов для обработки')
            return

        # Получаем ID всех товаров, которые могут быть дубликатами
        all_item_ids = []
        for items_list in duplicate_articles.values():
            all_item_ids.extend([item.id for item in items_list])

        # Получаем самую свежую дату обновления для каждого товара
        latest_info_dates = ItemInfo.objects.filter(
            item_id__in=all_item_ids,
        ).values('item_id').annotate(
            latest_date=Max('analyzed_at'),
        )

        # Создаем словарь для быстрого доступа
        latest_date_map = {info['item_id']: info['latest_date'] for info in latest_info_dates}

        # Формируем список для объединения
        merge_candidates = []
        detailed_article_info = []

        for article, items_list in duplicate_articles.items():
            # Добавляем дату последнего обновления к каждому товару
            for item in items_list:
                item.latest_date = latest_date_map.get(item.id, item.date_create)

            # Сортируем по дате последнего обновления (по убыванию)
            sorted_items = sorted(items_list, key=lambda x: x.latest_date, reverse=True)

            master_item = sorted_items[0]
            slave_items = sorted_items[1:]

            merge_candidates.append((master_item, slave_items))

            # Добавляем информацию для отчета
            detailed_article_info.append(f"Артикул: '{article}'")
            detailed_article_info.append(
                f'Мастер-товар: {master_item.id} (последнее обновление: {master_item.latest_date})')
            for item in slave_items:
                detailed_article_info.append(f'Подчинённый товар: {item.id} (последнее обновление: {item.latest_date})')
            detailed_article_info.append('')

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
        if not force:
            self.stdout.write(f"Для подтверждения объединения {len(merge_candidates)} артикулов введите 'y':")
            try:
                confirm = input().strip().lower()
            except UnicodeDecodeError:
                self.stdout.write(
                    'Обнаружена проблема с кодировкой ввода. Используйте параметр --force для выполнения без подтверждения.')
                return

            if confirm != 'y':
                self.stdout.write('Объединение отменено.')
                return

        # Шаг 3: Объединение данных
        self.stdout.write('Начинаем объединение...')

        with transaction.atomic():
            total_merged = 0
            results = []

            for i, (master_item, slave_items) in enumerate(merge_candidates):
                # Сохраняем информацию ДО объединения
                master_before = {
                    'history_count': ItemInfoHistory.objects.filter(item=master_item).count(),
                    'recommendations_count': RecommendedLinked.objects.filter(item=master_item).count(),
                    'info_count': ItemInfo.objects.filter(item=master_item).count(),
                }

                slaves_before = []
                for slave_item in slave_items:
                    slaves_before.append({
                        'id': slave_item.id,
                        'history_count': ItemInfoHistory.objects.filter(item=slave_item).count(),
                        'recommendations_count': RecommendedLinked.objects.filter(item=slave_item).count(),
                        'info_count': ItemInfo.objects.filter(item=slave_item).count(),
                    })

                for slave_item in slave_items:
                    # ВАЖНОЕ ИЗМЕНЕНИЕ: Сначала получаем все записи истории подчиненного товара
                    slave_history = list(ItemInfoHistory.objects.filter(item=slave_item))

                    # Переносим каждую запись истории по отдельности
                    for history_item in slave_history:
                        # Создаем новую запись истории для мастера
                        ItemInfoHistory.objects.create(
                            item=master_item,
                            analyzed_at=history_item.analyzed_at,
                            url=history_item.url,
                            catalog_url=history_item.catalog_url,
                            prices=history_item.prices,
                            competitor=history_item.competitor,
                            available_type=history_item.available_type,
                            item_info=history_item.item_info,
                        )

                    # Перенос рекомендаций
                    recommendations_count = RecommendedLinked.objects.filter(item=slave_item).update(item=master_item)

                    # Перенос актуальной информации
                    slave_infos = ItemInfo.objects.filter(item=slave_item).order_by('-analyzed_at')

                    if slave_infos.exists():
                        # Проверяем, есть ли уже запись ItemInfo для мастера
                        master_infos = ItemInfo.objects.filter(item=master_item)

                        if master_infos.exists():
                            # Если запись уже существует, обновляем ее, если данные новее
                            master_info = master_infos.first()
                            newest_slave_info = slave_infos.first()

                            if newest_slave_info.analyzed_at > master_info.analyzed_at:
                                master_info.analyzed_at = newest_slave_info.analyzed_at
                                master_info.url = newest_slave_info.url
                                master_info.catalog_url = newest_slave_info.catalog_url
                                master_info.prices = newest_slave_info.prices
                                master_info.available_type = newest_slave_info.available_type
                                master_info.save()
                        else:
                            # Если записи нет, создаем новую на основе самой новой информации подчиненного
                            newest_slave_info = slave_infos.first()
                            ItemInfo.objects.create(
                                competitor=master_item.competitor,
                                item=master_item,
                                analyzed_at=newest_slave_info.analyzed_at,
                                url=newest_slave_info.url,
                                catalog_url=newest_slave_info.catalog_url,
                                prices=newest_slave_info.prices,
                                available_type=newest_slave_info.available_type,
                            )

                    # Удаляем все записи ItemInfo для подчиненного товара
                    # ItemInfo.objects.filter(item=slave_item).delete()

                    # Удаляем историю подчиненного товара (теперь, когда мы ее перенесли)
                    # ItemInfoHistory.objects.filter(item=slave_item).delete()

                    # Удаление подчинённого товара
                    slave_item.delete()
                    total_merged += 1

                # Сохраняем информацию ПОСЛЕ объединения
                master_after = {
                    'history_count': ItemInfoHistory.objects.filter(item=master_item).count(),
                    'recommendations_count': RecommendedLinked.objects.filter(item=master_item).count(),
                    'info_count': ItemInfo.objects.filter(item=master_item).count(),
                }

                # Сохраняем результат
                results.append({
                    'master_item': master_item,
                    'slaves_before': slaves_before,
                    'master_before': master_before,
                    'master_after': master_after,
                })

                # Выводим прогресс
                self.stdout.write(f'Объединено артикулов: {i + 1}/{len(merge_candidates)}')

        self.stdout.write(self.style.SUCCESS(f'Все дубликаты успешно объединены! Объединено товаров: {total_merged}'))

        # Шаг 4: Проверка результатов и вывод истории
        self.stdout.write('\nПроверка результатов объединения и вывод истории...')

        # Запись результатов в файл
        try:
            with open(result_file_path, 'w', encoding='utf-8') as f:
                f.write('=== РЕЗУЛЬТАТЫ ОБЪЕДИНЕНИЯ ДУБЛИКАТОВ ===\n\n')
                f.write(f'Конкурент: {competitor.name}\n')
                f.write(f'Дата формирования отчета: {datetime.now()}\n\n')
                f.write(f'Всего объединено товаров: {total_merged}\n\n')

                for result in results:
                    master_item = result['master_item']
                    f.write(f'Артикул: {master_item.article}\n')
                    f.write(f'Мастер-товар ID: {master_item.id}\n\n')

                    # Выводим информацию ДО объединения
                    f.write('ДО объединения:\n')
                    f.write(f"  Мастер-товар: {result['master_before']['history_count']} записей истории, ")
                    f.write(f"{result['master_before']['recommendations_count']} рекомендаций, ")
                    f.write(f"{result['master_before']['info_count']} записей информации\n")

                    for i, slave in enumerate(result['slaves_before']):
                        f.write(
                            f"  Подчинённый товар {i + 1} (ID: {slave['id']}): {slave['history_count']} записей истории, ")
                        f.write(f"{slave['recommendations_count']} рекомендаций, ")
                        f.write(f"{slave['info_count']} записей информации\n")

                    # Выводим информацию ПОСЛЕ объединения
                    f.write('\nПОСЛЕ объединения:\n')
                    f.write(f"  Мастер-товар: {result['master_after']['history_count']} записей истории, ")
                    f.write(f"{result['master_after']['recommendations_count']} рекомендаций, ")
                    f.write(f"{result['master_after']['info_count']} записей информации\n")

                    # Проверяем, что история была перенесена
                    expected_history = result['master_before']['history_count'] + sum(
                        s['history_count'] for s in result['slaves_before'])
                    if result['master_after']['history_count'] == expected_history:
                        f.write(f'  ✓ История успешно перенесена: {expected_history} записей\n')
                    else:
                        f.write(
                            f"  ⚠ Проблема с переносом истории: ожидалось {expected_history}, получено {result['master_after']['history_count']}\n")

                    # Получаем и выводим историю изменений
                    history = ItemInfoHistory.objects.filter(item=master_item).order_by('-analyzed_at')
                    f.write(f'\nИстория изменений (первые 10 записей из {history.count()}):\n')
                    f.write('-' * 80 + '\n')

                    for h in history[:10]:
                        f.write(f'Дата: {h.analyzed_at}\n')
                        f.write(f'URL: {h.url}\n')
                        f.write(f'Каталог: {h.catalog_url}\n')
                        f.write(f'Цены: {h.prices}\n')
                        f.write('-' * 40 + '\n')

                    f.write('\n' + '=' * 80 + '\n\n')

            self.stdout.write(self.style.SUCCESS(f'Результаты объединения сохранены в: {result_file_path}'))

            # Дополнительно: вывод краткой информации в консоль
            self.stdout.write('\nКраткие результаты:')
            for result in results:
                master_item = result['master_item']
                expected = result['master_before']['history_count'] + sum(
                    s['history_count'] for s in result['slaves_before'])
                actual = result['master_after']['history_count']

                self.stdout.write(f'Товар {master_item.id} ({master_item.article}):')
                self.stdout.write(f'  - Ожидалось записей истории: {expected}')
                self.stdout.write(f'  - Фактически записей истории: {actual}')

                if actual == expected:
                    self.stdout.write(self.style.SUCCESS('  ✓ История успешно перенесена'))
                else:
                    self.stdout.write(self.style.ERROR('  ✗ Ошибка переноса истории'))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка при записи результатов: {e}'))

# python manage.py merge_duplicate_item 142 --article 1375258
# python manage.py merge_duplicate_item 142 --article 2081057 --force
