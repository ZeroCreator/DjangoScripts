# Особенности скрипта:
# Поиск в бэкапе: Скрипт ищет товар и все связанные данные в бэкап-базе.
# Проверка существования: Проверяет, не существует ли уже товар в основной базе.
# Предварительный просмотр: Показывает, что будет восстановлено, без внесения изменений.
# Подтверждение: Запрашивает подтверждение перед восстановлением.
# Транзакционность: Все операции выполняются в транзакции для обеспечения целостности данных.
# Проверка результатов: После восстановления проверяет, что все данные были успешно восстановлены.

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from datetime import datetime
from kenny.items.models import Item, ItemInfo, ItemInfoHistory
from linked.models import RecommendedLinked


class Command(BaseCommand):
    help = 'Восстанавливает удаленную позицию из бэкапа вместе со всей историей'

    def add_arguments(self, parser):
        parser.add_argument('article', type=str, help='Артикул товара для восстановления')
        parser.add_argument('competitor_id', type=int, help='ID конкурента')
        parser.add_argument('--preview', action='store_true', help='Предварительный просмотр без восстановления')

    def handle(self, *args, **options):
        article = options['article']
        competitor_id = options['competitor_id']
        preview_mode = options.get('preview', False)

        self.stdout.write("=== ВОССТАНОВЛЕНИЕ УДАЛЕННОЙ ПОЗИЦИИ ИЗ БЭКАПА ===")
        self.stdout.write(f"Артикул: {article}")
        self.stdout.write(f"Конкурент ID: {competitor_id}")
        self.stdout.write(f"Режим предпросмотра: {'Да' if preview_mode else 'Нет'}")

        # Шаг 1: Поиск товара в бэкапе
        self.stdout.write("\n1. Поиск товара в бэкап-базе...")
        try:
            # Ищем товар в бэкапе
            backup_item = Item.objects.using('backup').get(
                article=article,
                competitor_id=competitor_id
            )
            self.stdout.write(
                self.style.SUCCESS(f"   Найден товар в бэкапе: {backup_item.name} (ID: {backup_item.id})"))
        except Item.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"   Товар с артикулом '{article}' не найден в бэкап-базе"))
            return
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   Ошибка при поиске в бэкапе: {e}"))
            return

        # Шаг 2: Проверка существования товара в основной базе
        self.stdout.write("\n2. Проверка существования товара в основной базе...")
        if Item.objects.filter(article=article, competitor_id=competitor_id).exists():
            self.stdout.write(
                self.style.WARNING("   Товар уже существует в основной базе. Восстановление не требуется."))
            return

        # Шаг 3: Поиск связанных данных в бэкапе
        self.stdout.write("\n3. Поиск связанных данных в бэкапе...")

        # Поиск информации о товаре
        try:
            backup_item_info = ItemInfo.objects.using('backup').get(item=backup_item)
            self.stdout.write(self.style.SUCCESS(f"   Найдена информация о товаре (ID: {backup_item_info.id})"))
        except ItemInfo.DoesNotExist:
            backup_item_info = None
            self.stdout.write(self.style.WARNING("   Информация о товаре не найдена в бэкапе"))

        # Поиск истории товара
        backup_history = ItemInfoHistory.objects.using('backup').filter(item=backup_item)
        self.stdout.write(self.style.SUCCESS(f"   Найдено записей истории: {backup_history.count()}"))

        # Поиск рекомендаций
        backup_recommendations = RecommendedLinked.objects.using('backup').filter(item=backup_item)
        self.stdout.write(self.style.SUCCESS(f"   Найдено рекомендаций: {backup_recommendations.count()}"))

        # Шаг 4: Предварительный просмотр
        self.stdout.write("\n4. Предварительный просмотр данных для восстановления:")
        self.stdout.write(f"   Товар: {backup_item.name}")
        self.stdout.write(f"   Артикул: {backup_item.article}")
        self.stdout.write(f"   URL: {backup_item.url}")
        self.stdout.write(f"   Дата создания: {backup_item.date_create}")

        if backup_item_info:
            self.stdout.write(f"   Информация о товаре: {backup_item_info.analyzed_at}")

        self.stdout.write(f"   Записей истории: {backup_history.count()}")
        self.stdout.write(f"   Рекомендаций: {backup_recommendations.count()}")

        # Если режим предпросмотра, останавливаемся здесь
        if preview_mode:
            self.stdout.write(self.style.SUCCESS("\nРежим предпросмотра. Данные не были восстановлены."))
            return

        # Шаг 5: Подтверждение восстановления
        self.stdout.write("\n5. Подтверждение восстановления...")
        confirm = input("   Вы уверены, что хотите восстановить этот товар и все связанные данные? (y/n): ")

        if confirm.lower() != 'y':
            self.stdout.write("   Восстановление отменено.")
            return

        # Шаг 6: Восстановление данных
        self.stdout.write("\n6. Восстановление данных...")

        try:
            with transaction.atomic():
                # Восстанавливаем товар
                self.stdout.write("   Восстановление товара...")
                backup_item.save(using='default')  # Сохраняем в основную базу

                # Восстанавливаем информацию о товаре
                if backup_item_info:
                    self.stdout.write("   Восстановление информации о товаре...")
                    backup_item_info.save(using='default')

                # Восстанавливаем историю
                self.stdout.write("   Восстановление истории...")
                for history_item in backup_history:
                    history_item.save(using='default')

                # Восстанавливаем рекомендации
                self.stdout.write("   Восстановление рекомендаций...")
                for recommendation in backup_recommendations:
                    recommendation.save(using='default')

                self.stdout.write(self.style.SUCCESS("   Все данные успешно восстановлены!"))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   Ошибка при восстановлении: {e}"))
            return

        # Шаг 7: Проверка восстановления
        self.stdout.write("\n7. Проверка восстановления...")

        # Проверяем, что товар появился в основной базе
        try:
            restored_item = Item.objects.get(article=article, competitor_id=competitor_id)
            self.stdout.write(self.style.SUCCESS(f"   Товар успешно восстановлен (ID: {restored_item.id})"))

            # Проверяем восстановление истории
            restored_history = ItemInfoHistory.objects.filter(item=restored_item)
            self.stdout.write(self.style.SUCCESS(f"   Восстановлено записей истории: {restored_history.count()}"))

            # Проверяем восстановление рекомендаций
            restored_recommendations = RecommendedLinked.objects.filter(item=restored_item)
            self.stdout.write(self.style.SUCCESS(f"   Восстановлено рекомендаций: {restored_recommendations.count()}"))

        except Item.DoesNotExist:
            self.stdout.write(self.style.ERROR("   Ошибка: товар не был восстановлен"))
            return

        self.stdout.write(self.style.SUCCESS("\n=== ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО УСПЕШНО ==="))

# Как использовать этот скрипт:
# Предварительный просмотр (без восстановления):
# python manage.py restore_item "1296452" 142 --preview
# Полное восстановление:
# python manage.py restore_item "1296452" 142
# Где:
# "1296452" - артикул товара
# 142 - ID конкурента (Komus)
# --preview - опциональный флаг для предварительного просмотра без восстановления