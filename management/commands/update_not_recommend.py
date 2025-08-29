from django.core.management.base import BaseCommand
from django.db import transaction, models
from linked.models import RecommendedLinked


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--chunk-size',
            type=int,
            default=1000,
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
        )

    def handle(self, *args, **options):
        chunk_size = options['chunk_size']
        dry_run = options.get('dry_run', False)
        updated_count = 0

        self.stdout.write('Starting update process...')
        if dry_run:
            self.stdout.write('DRY RUN: No changes will be made')

        # Получаем ID и nomenclature_code из бэкап-базы
        backup_data = RecommendedLinked.objects.using('backup').filter(
            not_recommend=True
        ).values_list('item_id', 'nomenclature_code')

        # Преобразуем в множество для быстрого поиска
        backup_set = set(backup_data)

        self.stdout.write(f'Found {len(backup_set)} records in backup database')

        if not backup_set:
            self.stdout.write(self.style.SUCCESS('No records to update'))
            return

        # Обрабатываем данные чанками для экономии памяти
        backup_list = list(backup_set)

        for i in range(0, len(backup_list), chunk_size):
            chunk = backup_list[i:i + chunk_size]

            # Создаем условия для фильтрации
            query = models.Q()
            for item_id, nomenclature_code in chunk:
                query |= models.Q(item_id=item_id, nomenclature_code=nomenclature_code)

            # Находим записи для обновления
            records_to_update = RecommendedLinked.objects.using('default').filter(
                query,
                not_recommend=False  # Обновляем только если текущее значение False
            )

            if dry_run:
                # Только подсчет без обновления
                count = records_to_update.count()
                updated_count += count
                self.stdout.write(f'Would update {count} records in chunk {i // chunk_size + 1}')
            else:
                # Выполняем обновление в транзакции
                with transaction.atomic(using='default'):
                    updated = records_to_update.update(not_recommend=True)
                    updated_count += updated
                    self.stdout.write(f'Updated {updated} records in chunk {i // chunk_size + 1}')

            self.stdout.write(f'Processed {min(i + chunk_size, len(backup_list))}/{len(backup_list)} records')

        if dry_run:
            self.stdout.write(
                self.style.WARNING(f'DRY RUN: Would update {updated_count} records total')
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(f'Successfully updated {updated_count} records')
            )
