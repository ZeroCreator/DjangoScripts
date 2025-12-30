# debug_recommendations.py
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'conf.docker')
django.setup()

from django.db.models import Q
from django.conf import settings
from django.db.models.functions import Length

from one_c_raw.models import Nomenclature
from linked.helpers import get_ones_nomenclature_qs


def debug_recommendations(code_creates, code_not_creates):
    """code_creates - создает рекомендации, code_not_creates - не создает"""
    print("=== ДЕБАГ РЕКОМЕНДАЦИЙ ===")
    print(
        f"Номенклатура {code_creates} (СОЗДАЕТ рекомендации) vs Номенклатура {code_not_creates} (НЕ создает рекомендации)")
    print()

    try:
        nomen_creates = Nomenclature.objects.get(code=code_creates)
        nomen_not_creates = Nomenclature.objects.get(code=code_not_creates)
    except Nomenclature.DoesNotExist:
        print("Ошибка: Одна из номенклатур не найдена!")
        return

    check_suppliers(nomen_creates, nomen_not_creates)
    print()
    check_filtration_fast(nomen_creates, nomen_not_creates)  # Быстрая проверка фильтрации
    print()
    check_final_result_fast(nomen_creates, nomen_not_creates)  # Быстрая проверка финального результата


def check_suppliers(nomen_creates, nomen_not_creates):
    """Проверка связанных поставщиков"""
    print("=== ПРОВЕРКА ПОСТАВЩИКОВ ===")

    # Поставщики для номенклатуры которая СОЗДАЕТ рекомендации
    suppliers_creates = nomen_creates.supplier.all()
    print(f"Номенклатура {nomen_creates.code} (СОЗДАЕТ) - поставщики ({suppliers_creates.count()}):")
    for sup in suppliers_creates:
        status = "✅ АКТИВЕН" if not sup._mark_remove else "❌ УДАЛЕН"
        print(f"  - {sup.name} | {status} | art: '{sup.art}'")

    # Поставщики для номенклатуры которая НЕ создает рекомендации
    suppliers_not_creates = nomen_not_creates.supplier.all()
    print(f"\nНоменклатура {nomen_not_creates.code} (НЕ создает) - поставщики ({suppliers_not_creates.count()}):")
    for sup in suppliers_not_creates:
        status = "✅ АКТИВЕН" if not sup._mark_remove else "❌ УДАЛЕН"
        print(f"  - {sup.name} | {status} | art: '{sup.art}'")

    # Проверяем исключенных поставщиков
    excluded_creates = nomen_creates.supplier.filter(_mark_remove=1)
    excluded_not_creates = nomen_not_creates.supplier.filter(_mark_remove=1)

    print(f"\nИсключенные поставщики (_mark_remove=1):")
    print(f"  - Номенклатура {nomen_creates.code} (СОЗДАЕТ): {excluded_creates.count()}")
    print(f"  - Номенклатура {nomen_not_creates.code} (НЕ создает): {excluded_not_creates.count()}")


def check_filtration_fast(nomen_creates, nomen_not_creates):
    """Быстрая проверка условий фильтрации (без выполнения тяжелых запросов)"""
    print("=== ПРОВЕРКА УСЛОВИЙ ФИЛЬТРАЦИИ ===")

    blacklist = getattr(settings, 'BLACKLISTED_CODES_FOR_RECOMMENDATIONS', [])

    # Проверяем условия по отдельности для каждой номенклатуры
    print(f"\n1. Черный список:")
    in_blacklist_creates = nomen_creates.code in blacklist
    in_blacklist_not_creates = nomen_not_creates.code in blacklist
    print(
        f"   - {nomen_creates.code} (СОЗДАЕТ): {'❌ В ЧЕРНОМ СПИСКЕ' if in_blacklist_creates else '✅ НЕТ в черном списке'}")
    print(
        f"   - {nomen_not_creates.code} (НЕ создает): {'❌ В ЧЕРНОМ СПИСКЕ' if in_blacklist_not_creates else '✅ НЕТ в черном списке'}")

    print(f"\n2. Длина артикула:")
    art_length_creates = len(nomen_creates.art)
    art_length_not_creates = len(nomen_not_creates.art)
    art_too_short_creates = art_length_creates < 5
    art_too_short_not_creates = art_length_not_creates < 5
    print(
        f"   - {nomen_creates.code} (СОЗДАЕТ): {art_length_creates} символов {'❌ < 5' if art_too_short_creates else '✅ >= 5'}")
    print(
        f"   - {nomen_not_creates.code} (НЕ создает): {art_length_not_creates} символов {'❌ < 5' if art_too_short_not_creates else '✅ >= 5'}")

    print(f"\n3. Поставщики с _mark_remove=1 (проверяем быстро):")
    # Быстрая проверка через прямое обращение к связанным объектам
    problematic_suppliers_creates = nomen_creates.supplier.filter(_mark_remove=1).exists()
    problematic_suppliers_not_creates = nomen_not_creates.supplier.filter(_mark_remove=1).exists()
    print(
        f"   - {nomen_creates.code} (СОЗДАЕТ): {'❌ ЕСТЬ проблемные поставщики' if problematic_suppliers_creates else '✅ НЕТ проблемных поставщиков'}")
    print(
        f"   - {nomen_not_creates.code} (НЕ создает): {'❌ ЕСТЬ проблемные поставщики' if problematic_suppliers_not_creates else '✅ НЕТ проблемных поставщиков'}")


def check_final_result_fast(nomen_creates, nomen_not_creates):
    """Быстрая проверка финального результата"""
    print("=== ФИНАЛЬНЫЙ РЕЗУЛЬТАТ (БЫСТРАЯ ПРОВЕРКА) ===")

    # Вместо выполнения тяжелого запроса, проверяем условия логически
    blacklist = getattr(settings, 'BLACKLISTED_CODES_FOR_RECOMMENDATIONS', [])

    # Проверяем каждое условие исключения
    excluded_by_blacklist_creates = nomen_creates.code in blacklist
    excluded_by_blacklist_not_creates = nomen_not_creates.code in blacklist

    excluded_by_art_length_creates = len(nomen_creates.art) < 5
    excluded_by_art_length_not_creates = len(nomen_not_creates.art) < 5

    excluded_by_suppliers_creates = nomen_creates.supplier.filter(_mark_remove=1).exists()
    excluded_by_suppliers_not_creates = nomen_not_creates.supplier.filter(_mark_remove=1).exists()

    # Итоговый результат
    excluded_creates = excluded_by_blacklist_creates or excluded_by_art_length_creates or excluded_by_suppliers_creates
    excluded_not_creates = excluded_by_blacklist_not_creates or excluded_by_art_length_not_creates or excluded_by_suppliers_not_creates

    status_creates = "❌ ИСКЛЮЧЕНА" if excluded_creates else "✅ ВКЛЮЧЕНА"
    status_not_creates = "❌ ИСКЛЮЧЕНА" if excluded_not_creates else "✅ ВКЛЮЧЕНА"

    print(f"Логический анализ условий исключения:")
    print(f"  - Номенклатура {nomen_creates.code} (СОЗДАЕТ): {status_creates}")
    print(f"  - Номенклатура {nomen_not_creates.code} (НЕ создает): {status_not_creates}")

    # Детализация причин исключения
    if excluded_creates:
        print(f"\nПричины исключения {nomen_creates.code} (СОЗДАЕТ):")
        if excluded_by_blacklist_creates: print("  - В черном списке")
        if excluded_by_art_length_creates: print(f"  - Длина артикула {len(nomen_creates.art)} < 5")
        if excluded_by_suppliers_creates: print("  - Есть поставщики с _mark_remove=1")

    if excluded_not_creates:
        print(f"\nПричины исключения {nomen_not_creates.code} (НЕ создает):")
        if excluded_by_blacklist_not_creates: print("  - В черном списке")
        if excluded_by_art_length_not_creates: print(f"  - Длина артикула {len(nomen_not_creates.art)} < 5")
        if excluded_by_suppliers_not_creates: print("  - Есть поставщики с _mark_remove=1")

    # Анализ несоответствия
    print(f"\n=== АНАЛИЗ НЕСООТВЕТСТВИЯ ===")
    if not excluded_creates and excluded_not_creates:
        print("✅ Логично: одна включена, другая исключена")
    elif excluded_creates and not excluded_not_creates:
        print("❌ НЕЛОГИЧНО: та что создает рекомендации - исключена по условиям!")
        print("   Возможные причины:")
        print("   - Задача выполнялась в другое время, когда условия были другими")
        print("   - Состояние БД изменилось после выполнения задачи")
        print("   - Есть дополнительные условия в get_ones_nomenclature_qs()")
    elif not excluded_creates and not excluded_not_creates:
        print("🔍 ОБЕ должны обрабатываться - проблема в другом месте")
    else:
        print("❌ ОБЕ исключены - нужна дополнительная диагностика")


def check_items_matching_fast(nomen_creates, nomen_not_creates):
    """Быстрая проверка соответствия товаров"""
    print("\n=== ПРОВЕРКА СООТВЕТСТВИЯ ТОВАРОВ (БЫСТРАЯ) ===")

    try:
        from linked.helpers import get_items_with_is_linked_field

        # Берем только небольшое количество товаров для проверки
        items_sample = get_items_with_is_linked_field().exclude(
            Q(name='') | Q(is_linked=True) | Q(is_blocked=True),
        )[:1000]  # Ограничиваем выборку

        from linked.tasks import ArticleRecommendationsTask
        task = ArticleRecommendationsTask()

        # Проверяем только для артикулов номенклатур
        art_creates = nomen_creates.art.lower()
        art_not_creates = nomen_not_creates.art.lower()

        matching_creates = [item for item in items_sample if art_creates in item.name.lower()]
        matching_not_creates = [item for item in items_sample if art_not_creates in item.name.lower()]

        print(f"Товаров для {nomen_creates.code} (СОЗДАЕТ) в выборке: {len(matching_creates)}")
        print(f"Товаров для {nomen_not_creates.code} (НЕ создает) в выборке: {len(matching_not_creates)}")

        if matching_creates:
            print(f"\nПримеры товаров для {nomen_creates.code} (СОЗДАЕТ):")
            for item in matching_creates[:2]:
                print(f"  - {item.name}")

        if matching_not_creates:
            print(f"\nПримеры товаров для {nomen_not_creates.code} (НЕ создает):")
            for item in matching_not_creates[:2]:
                print(f"  - {item.name}")

    except Exception as e:
        print(f"Ошибка при проверке товаров: {e}")


if __name__ == "__main__":
    # 369342 - создает рекомендации, 171664 - не создает
    debug_recommendations(369342, 171664)

    # Дополнительная быстрая проверка товаров
    print("\n" + "=" * 60)
    nomen_creates = Nomenclature.objects.get(code=369342)
    nomen_not_creates = Nomenclature.objects.get(code=171664)
    check_items_matching_fast(nomen_creates, nomen_not_creates)

    # Дополнительная проверка товаров (раскомментируйте если нужно)
    # print("\n" + "="*50)
    # nomen1 = Nomenclature.objects.get(code=171664)
    # nomen2 = Nomenclature.objects.get(code=369342)
    # check_items_matching(nomen1, nomen2)
    # python debug_recommendations.py