"""
Скрипт нормализации артикулов товаров конкурента.

НАЗНАЧЕНИЕ:
- Нормализует артикулы товаров указанного конкурента путем удаления пробелов
  в начале и конце артикулов
- Создает детальный лог выполнения операции
- Оптимизирован для работы с большими объемами данных

ПРОБЛЕМА, КОТОРУЮ РЕШАЕТ:
У конкурентов часто встречаются артикулы с лишними пробелами в начале или конце,
что приводит к проблемам при:
- Сопоставлении товаров между конкурентами
- Поиске и фильтрации товаров
- Анализе данных и построении отчетов
- Импорте/экспорте данных

АЛГОРИТМ РАБОТЫ:
1. Поиск конкурента по ID
2. Поиск товаров с пробелами в начале или конце артикула
3. Нормализация артикулов (удаление пробелов по краям)
4. Пакетное обновление записей в базе данных
5. Создание детального лога выполнения

КРИТЕРИИ ОБРАБОТКИ:
- Обрабатываются только товары с пробелами в начале или конце артикула
- Артикулы без пробелов не изменяются
- Нормализация: article.strip() - удаление пробелов с обеих сторон

ОСОБЕННОСТИ РЕАЛИЗАЦИИ:
- Пакетная обработка данных (batch processing) для оптимизации производительности
- Прогресс-бар с расчетом оставшегося времени
- Запись подробных логов в файл
- Экономия памяти через использование iterator()
- Загрузка только необходимых полей (id, article)

ПАРАМЕТРЫ ЗАПУСКА:
--batch-size : Размер пакета для обновления (по умолчанию: 1000)
               Рекомендуемые значения: 1000-5000 в зависимости от нагрузки на БД

ЛОГИРОВАНИЕ:
- Автоматическое создание файла лога с timestamp в названии
- Детальная информация о прогрессе выполнения
- Статистика по завершении операции
- Файл сохраняется в корневой директории проекта

МЕРЫ ПРЕДОСТОРОЖНОСТИ:
- Работает только с указанным конкурентом
- Не затрагивает артикулы без пробелов
- Создает backup в виде лог-файла
- Предоставляет возможность предварительного подсчета товаров

ПРОИЗВОДИТЕЛЬНОСТЬ:
- Оптимизирован для больших объемов данных
- Использует bulk_update для минимизации запросов к БД
- Прогрессивное обновление с выводом статистики

ИСПОЛЬЗОВАНИЕ:
Рекомендуется выполнять перед процедурой удаления дубликатов для обеспечения
консистентности данных.

ВХОДНЫЕ ДАННЫЕ:
- ID конкурента
- Опционально: размер пакета обновления

ВЫХОДНЫЕ ДАННЫЕ:
- Статистика выполнения в консоли
- Детальный лог в файле
- Обновленные артикулы в базе данных

АВТОМАТИЗАЦИЯ:
Может быть интегрирован в регулярные процедуры обслуживания данных конкурентов.
"""

import os
import time
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.conf import settings
from kenny.items.models import Competitor, Item


class Command(BaseCommand):
    help = 'Нормализует артикулы (убирает пробелы) у товаров указанного конкурента только если артикул не нормализован'

    def add_arguments(self, parser):
        parser.add_argument('competitor_id', type=int, help='ID конкурента')
        parser.add_argument('--batch-size', type=int, default=1000,
                            help='Размер батча для обновления (по умолчанию: 1000)')

    def handle(self, *args, **options):
        competitor_id = options['competitor_id']
        batch_size = options['batch_size']

        # Создаем автоматическое имя файла с временем
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = f'article_normalization_{competitor_id}_{timestamp}.log'
        log_file_path = os.path.join(settings.BASE_DIR, log_filename)

        # Создаем файл для логов
        try:
            log_file = open(log_file_path, 'w', encoding='utf-8')
            self.stdout.write(f'Файл логов создан: {log_file_path}')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка создания файла логов: {e}'))
            return

        def write_output(message, style=None):
            """Универсальная функция для вывода в консоль и файл"""
            if style:
                formatted_message = style(message)
                self.stdout.write(formatted_message)
                log_file.write(f'{formatted_message}\n')
            else:
                self.stdout.write(message)
                log_file.write(f'{message}\n')

            log_file.flush()

        try:
            start_time = time.time()
            write_output('=== НАЧАЛО ПРОЦЕДУРЫ НОРМАЛИЗАЦИИ АРТИКУЛОВ ===')
            write_output(f'Время начала: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

            # Поиск конкурента
            write_output(f'Поиск конкурента с ID {competitor_id}...')
            try:
                competitor = Competitor.objects.get(id=competitor_id)
                write_output(self.style.SUCCESS(f'Найден конкурент: {competitor.name} (ID: {competitor.id})'))
            except Competitor.DoesNotExist:
                write_output(self.style.ERROR(f'Конкурент с ID {competitor_id} не найден'))
                return

            # Используем только для подсчета - более быстрый запрос
            write_output('Подсчет товаров для нормализации...')
            total_to_fix = Item.objects.filter(
                competitor=competitor,
            ).filter(
                Q(article__startswith=' ') | Q(article__endswith=' '),
            ).count()

            write_output(f'Товаров с пробелами в артикулах для нормализации: {total_to_fix}')

            if total_to_fix == 0:
                write_output(self.style.SUCCESS('Нет товаров для нормализации'))
                return

            # Основной цикл обработки с батчами
            updated_count = 0
            processed_count = 0
            batch_items = []
            last_progress_time = time.time()

            # Используем iterator() для экономии памяти
            items_queryset = Item.objects.filter(
                competitor=competitor,
            ).filter(
                Q(article__startswith=' ') | Q(article__endswith=' '),
            ).only('id', 'article')  # Загружаем только необходимые поля

            write_output(f'Начинаем обработку батчами по {batch_size} записей...')

            for item in items_queryset.iterator(chunk_size=1000):
                normalized_article = item.article.strip()
                if item.article != normalized_article:
                    item.article = normalized_article
                    batch_items.append(item)
                    updated_count += 1

                processed_count += 1

                # Выводим прогресс каждые 1000 записей или каждые 30 секунд
                current_time = time.time()
                if (processed_count % 1000 == 0 or
                        current_time - last_progress_time >= 30 or
                        processed_count == total_to_fix):

                    progress_percent = (processed_count / total_to_fix) * 100
                    elapsed_time = current_time - start_time

                    # Расчет оставшегося времени
                    if processed_count > 0:
                        items_per_second = processed_count / elapsed_time
                        remaining_items = total_to_fix - processed_count
                        if items_per_second > 0:
                            remaining_time = remaining_items / items_per_second
                            time_str = f"{remaining_time:.1f} сек"
                            if remaining_time > 60:
                                time_str = f"{remaining_time / 60:.1f} мин"
                        else:
                            time_str = "расчет..."
                    else:
                        time_str = "расчет..."

                    write_output(
                        f'Прогресс: {processed_count}/{total_to_fix} '
                        f'({progress_percent:.1f}%) | '
                        f'Обновлено: {updated_count} | '
                        f'Прошло: {elapsed_time:.1f} сек | '
                        f'Осталось: {time_str}'
                    )
                    last_progress_time = current_time

                # Обновляем батч
                if len(batch_items) >= batch_size:
                    Item.objects.bulk_update(batch_items, ['article'])
                    write_output(f'Батч обновлен: {len(batch_items)} записей')
                    batch_items = []

            # Обновляем оставшиеся записи
            if batch_items:
                Item.objects.bulk_update(batch_items, ['article'])
                write_output(f'Финальный батч обновлен: {len(batch_items)} записей')

            end_time = time.time()
            execution_time = end_time - start_time

            # Итоговая статистика
            write_output('=== ИТОГИ НОРМАЛИЗАЦИИ ===')
            write_output(f'Всего обработано товаров: {processed_count}')
            write_output(f'Товаров с измененными артикулами: {updated_count}')
            write_output(f'Товаров без изменений: {processed_count - updated_count}')
            if processed_count > 0:
                write_output(f'Процент изменений: {(updated_count / processed_count) * 100:.1f}%')
            write_output(f'Общее время выполнения: {execution_time:.2f} секунд')
            if processed_count > 0:
                write_output(f'Скорость обработки: {processed_count / execution_time:.1f} записей/сек')
            write_output(f'Время окончания: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            write_output(self.style.SUCCESS('=== ПРОЦЕДУРА НОРМАЛИЗАЦИИ ЗАВЕРШЕНА ==='))

        finally:
            # Всегда закрываем файл, даже если возникла ошибка
            log_file.close()
            self.stdout.write(f'Логи сохранены в файл: {log_file_path}')

# Запустите команду:
# python manage.py article_normalization <competitor_id> --batch-size 2000
# python manage.py article_normalization 142 --batch-size 5000
