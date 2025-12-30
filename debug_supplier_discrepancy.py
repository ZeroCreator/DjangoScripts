# debug_supplier_discrepancy.py
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'conf.docker')
django.setup()

from one_c_raw.models import Nomenclature
from linked.helpers import get_ones_nomenclature_qs
from django.conf import settings


def debug_sql_query(nomenclature_code):
    """Детальная отладка SQL запроса"""
    print("\n" + "=" * 60)
    print("ДЕТАЛЬНАЯ ОТЛАДКА SQL ЗАПРОСА")
    print("=" * 60)

    from django.db import connection

    # Получаем точный SQL запрос
    problem_qs = get_ones_nomenclature_qs().filter(code=nomenclature_code).filter(supplier___mark_remove=1)
    sql, params = problem_qs.query.sql_with_params()

    print("🔍 Полный SQL запрос с параметрами:")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    print()

    # Выполняем запрос вручную и смотрим результаты
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        results = cursor.fetchall()

        print(f"📊 Результаты запроса: {len(results)} строк")
        if results:
            print("Первые 3 строки результатов:")
            for i, row in enumerate(results[:3]):
                print(f"  {i + 1}. {row}")

        # Получим описание колонок
        description = cursor.description
        print(f"\n📋 Столбцы результата:")
        for desc in description:
            print(f"  - {desc.name} (type: {desc.type_code})")

    # Проверим COUNT вместо EXISTS
    count = problem_qs.count()
    print(f"\n📌 Результат count(): {count}")
    print(f"📌 Результат exists(): {problem_qs.exists()}")

def debug_supplier_discrepancy(nomenclature_code):
    """Дебаг расхождения между поставщиками в разных queryset"""

    print("=" * 60)
    print(f"ДЕБАГ РАСХОЖДЕНИЯ ПОСТАВЩИКОВ ДЛЯ НОМЕНКЛАТУРЫ {nomenclature_code}")
    print("=" * 60)

    try:
        nomen_base = Nomenclature.objects.get(code=nomenclature_code)
        nomen_ones = get_ones_nomenclature_qs().get(code=nomenclature_code)
    except Nomenclature.DoesNotExist:
        print(f"❌ Номенклатура с кодом {nomenclature_code} не найдена!")
        return

    check_table_info()
    print()
    compare_querysets(nomen_base, nomen_ones)
    print()
    check_filters(nomen_base)
    print()
    find_problem_suppliers(nomen_base, nomen_ones)
    print()
    check_sql_queries(nomen_base, nomen_ones)
    print()
    check_supplier_discrepancy_detailed(nomenclature_code)


def check_table_info():
    """Узнаем правильные имена таблиц и полей"""
    print("=== ИНФОРМАЦИЯ О МОДЕЛЯХ И ТАБЛИЦАХ ===")

    # Информация о модели Nomenclature
    nomen_model = Nomenclature
    print(f"📋 Модель Nomenclature:")
    print(f"   - Таблица: {nomen_model._meta.db_table}")
    print(f"   - Поле code: {nomen_model._meta.get_field('code').column}")
    print(f"   - Поле uuid: {nomen_model._meta.get_field('uuid').column}")

    # Информация о связанной модели SupplierNomenclature
    supplier_model = nomen_model.supplier.field.related_model
    print(f"\n📋 Модель SupplierNomenclature:")
    print(f"   - Таблица: {supplier_model._meta.db_table}")
    print(f"   - Поле связи: {nomen_model.supplier.field.column}")
    print(f"   - Поле _mark_remove: {supplier_model._meta.get_field('_mark_remove').column}")

    # Покажем все поля SupplierNomenclature
    print(f"   - Все поля: {[f.column for f in supplier_model._meta.fields]}")


def compare_querysets(nomen_base, nomen_ones):
    """Сравнение queryset'ов"""
    print("=== СРАВНЕНИЕ QUERYSET'ОВ ===")

    base_suppliers_count = nomen_base.supplier.count()
    ones_suppliers_count = nomen_ones.supplier.count()

    print(f"📊 Базовый queryset (Nomenclature.objects):")
    print(f"   - Поставщиков: {base_suppliers_count}")

    print(f"📊 Ones queryset (get_ones_nomenclature_qs()):")
    print(f"   - Поставщиков: {ones_suppliers_count}")

    if base_suppliers_count != ones_suppliers_count:
        print(f"🔍 РАСХОЖДЕНИЕ: {abs(base_suppliers_count - ones_suppliers_count)} поставщиков")
    else:
        print("✅ Количество поставщиков совпадает")


def check_filters(nomen_base):
    """Проверка фильтров get_ones_nomenclature_qs()"""
    print("=== ПРОВЕРКА ФИЛЬТРОВ get_ones_nomenclature_qs() ===")

    filters = [
        {
            'name': 'is_not_for_sale',
            'value': nomen_base.is_not_for_sale,
            'should_be': False
        },
        {
            'name': 'view в EXCLUDE_VIEW_NOMENCKATURE',
            'value': str(nomen_base.view) in getattr(settings, 'EXCLUDE_VIEW_NOMENCKATURE', []),
            'should_be': False
        },
        {
            'name': 'name начинается с "я"',
            'value': nomen_base.name.startswith('я') if nomen_base.name else False,
            'should_be': False
        }
    ]

    all_pass = True
    for filter_check in filters:
        status = "✅ ПРОЙДЕН" if filter_check['value'] == filter_check['should_be'] else "❌ НЕ ПРОЙДЕН"
        print(f"   - {filter_check['name']}: {status} ({filter_check['value']})")
        if filter_check['value'] != filter_check['should_be']:
            all_pass = False

    if all_pass:
        print("🎉 Все фильтры пройдены - номенклатура должна быть в queryset")
    else:
        print("⚠️ Некоторые фильтры не пройдены")


def find_problem_suppliers(nomen_base, nomen_ones):
    """Поиск проблемных поставщиков"""
    print("=== ПОИСК ПРОБЛЕМНЫХ ПОСТАВЩИКОВ ===")

    # Проверяем в базовом queryset
    base_problem_suppliers = nomen_base.supplier.filter(_mark_remove=1)
    print(f"📋 Базовый queryset - проблемных поставщиков: {base_problem_suppliers.count()}")

    # Проверяем в ones queryset
    ones_problem_suppliers = nomen_ones.supplier.filter(_mark_remove=1)
    print(f"📋 Ones queryset - проблемных поставщиков: {ones_problem_suppliers.count()}")

    if ones_problem_suppliers.exists():
        print("\n🔍 Найдены проблемные поставщики в ones queryset:")
        for sup in ones_problem_suppliers:
            print(f"   ❌ {sup.name}")
            print(f"      Артикул: '{sup.art}', _mark_remove: {sup._mark_remove}")

    # Показываем всех поставщиков из ones queryset для сравнения
    print(f"\n📋 ВСЕ поставщики в ones queryset ({nomen_ones.supplier.count()}):")
    for sup in nomen_ones.supplier.all():
        status = "❌ _mark_remove=1" if sup._mark_remove else "✅ АКТИВЕН"
        print(f"   - {status} | {sup.name} | артикул: '{sup.art}'")


def check_sql_queries(nomen_base, nomen_ones):
    """Показывает SQL запросы для анализа"""
    print("=== SQL ЗАПРОСЫ ДЛЯ АНАЛИЗА ===")

    # Получаем SQL для проблемных поставщиков в ones queryset
    problem_qs = get_ones_nomenclature_qs().filter(code=nomen_base.code).filter(supplier___mark_remove=1)

    print("🔍 SQL запрос для поиска проблемных поставщиков:")
    print(f"   {problem_qs.query}")
    print()

    # Проверяем exists()
    has_problems = problem_qs.exists()
    print(f"📌 Результат exists(): {has_problems}")


def check_supplier_discrepancy_detailed(nomenclature_code):
    """Детальная проверка расхождения поставщиков"""
    print("\n" + "=" * 60)
    print("ДЕТАЛЬНАЯ ПРОВЕРКА РАСХОЖДЕНИЯ")
    print("=" * 60)

    nomen_base = Nomenclature.objects.get(code=nomenclature_code)
    nomen_ones = get_ones_nomenclature_qs().get(code=nomenclature_code)

    # Получаем ID всех поставщиков из разных источников
    base_supplier_ids = set(nomen_base.supplier.all().values_list('uuid', flat=True))
    ones_supplier_ids = set(nomen_ones.supplier.all().values_list('uuid', flat=True))

    print(f"UUID поставщиков в базовом queryset: {len(base_supplier_ids)}")
    print(f"UUID поставщиков в ones queryset: {len(ones_supplier_ids)}")

    # Находим различия
    only_in_base = base_supplier_ids - ones_supplier_ids
    only_in_ones = ones_supplier_ids - base_supplier_ids

    if only_in_base:
        print(f"\n🔍 Поставщики ТОЛЬКО в базовом queryset: {len(only_in_base)}")
        for sup_id in list(only_in_base)[:3]:  # первые 3
            sup = nomen_base.supplier.get(uuid=sup_id)
            print(f"   - {sup.name} (_mark_remove: {sup._mark_remove})")

    if only_in_ones:
        print(f"\n🔍 Поставщики ТОЛЬКО в ones queryset: {len(only_in_ones)}")
        for sup_id in list(only_in_ones)[:3]:  # первые 3
            sup = nomen_ones.supplier.get(uuid=sup_id)
            print(f"   - {sup.name} (_mark_remove: {sup._mark_remove})")

    if not only_in_base and not only_in_ones:
        print("✅ Все поставщики одинаковы в обоих queryset'ах")


def simple_sql_check(nomenclature_code):
    """Простая проверка через ORM без сложных SQL"""
    print("\n" + "=" * 60)
    print("ПРОСТАЯ ПРОВЕРКА ЧЕРЕЗ ORM")
    print("=" * 60)

    nomen = Nomenclature.objects.get(code=nomenclature_code)

    # Простая статистика через ORM
    total_suppliers = nomen.supplier.count()
    active_suppliers = nomen.supplier.filter(_mark_remove=False).count()
    removed_suppliers = nomen.supplier.filter(_mark_remove=True).count()

    print(f"📊 Статистика поставщиков для номенклатуры {nomenclature_code}:")
    print(f"   - Всего поставщиков: {total_suppliers}")
    print(f"   - Активных: {active_suppliers}")
    print(f"   - Удаленных: {removed_suppliers}")

    if removed_suppliers > 0:
        print(f"\n🔍 Удаленные поставщики:")
        for sup in nomen.supplier.filter(_mark_remove=True):
            print(f"   ❌ {sup.name} (артикул: '{sup.art}')")


if __name__ == "__main__":
    debug_sql_query(171664)
    # Запускаем упрощенную версию
    debug_supplier_discrepancy(171664)

    # Дополнительная простая проверка
    simple_sql_check(171664)

# python debug_supplier_discrepancy.py