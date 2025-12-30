# check_database_issue.py
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'conf.docker')
django.setup()

from one_c_raw.models import SupplierNomenclature
from django.db import connections


def check_database_issue():
    """Проверяем проблему с разными базами данных"""

    uuid = '00ecc85c-b1fb-11e2-93f1-002655df3ac1'

    print("=== ПРОВЕРКА РАЗНЫХ БАЗ ДАННЫХ ===")

    # 1. Запрос через Django ORM (использует default базу)
    suppliers_orm = SupplierNomenclature.objects.filter(nomenclature_id=uuid)
    print(f"Django ORM (default база): {suppliers_orm.count()} записей")

    # 2. Проверим raw SQL запрос к default базе
    with connections['default'].cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM supplier_nomenclature WHERE nomenclature_id = %s", [uuid])
        count_default = cursor.fetchone()[0]
        print(f"Raw SQL (default база): {count_default} записей")

    # 3. Если есть другие базы, проверим их
    for db_name in connections:
        if db_name != 'default':
            try:
                with connections[db_name].cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM supplier_nomenclature WHERE nomenclature_id = %s", [uuid])
                    count_other = cursor.fetchone()[0]
                    print(f"Raw SQL ({db_name} база): {count_other} записей")
            except Exception as e:
                print(f"Ошибка при запросе к {db_name}: {e}")

    # 4. Проверим конкретно удаленную запись
    print(f"\n--- ПОИСК УДАЛЕННОЙ ЗАПИСИ ---")

    # В default базе
    removed_in_default = SupplierNomenclature.objects.filter(
        nomenclature_id=uuid,
        _mark_remove=True
    ).count()
    print(f"Удаленных записей в default базе: {removed_in_default}")

    # 5. Проверим все записи с их _mark_remove
    print(f"\n--- ВСЕ ЗАПИСИ В DEFAULT БАЗЕ ---")
    all_suppliers = list(SupplierNomenclature.objects.filter(nomenclature_id=uuid))
    for i, supplier in enumerate(all_suppliers, 1):
        status = "🗑️ УДАЛЕН" if supplier._mark_remove else "✅ АКТИВЕН"
        print(f"{i}. {status} | {supplier.name} | UUID: {supplier.uuid}")


if __name__ == "__main__":
    check_database_issue()

# python check_database_issue.py