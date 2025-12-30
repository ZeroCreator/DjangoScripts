# test_specific_issue.py
import os
import django
from django.db.models.functions import Length

from conf import settings, docker

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'conf.docker')
django.setup()

from django.db.models import Q
from linked.helpers import get_ones_nomenclature_qs
#
#
# def test_specific_issue():
#     print("=== ТЕСТ КОНКРЕТНОЙ ПРОБЛЕМЫ ===")
#
#     # 1. Проверим базовый queryset
#     base_qs = get_ones_nomenclature_qs()
#     print(f"1. В базовом queryset: {base_qs.filter(code=171664).exists()}")
#
#     # 2. Проверим исключение по supplier___mark_remove=1
#     excluded = base_qs.filter(code=171664).filter(supplier___mark_remove=1).exists()
#     print(f"2. Исключена по supplier___mark_remove=1: {excluded}")
#
#     # 3. Проверим другие условия исключения
#     blacklisted = 171664 in getattr(settings, 'BLACKLISTED_CODES_FOR_RECOMMENDATIONS', [])
#     print(f"3. В черном списке: {blacklisted}")
#
#     # 4. Проверим длину артикула
#     from one_c_raw.models import Nomenclature
#     nomen = Nomenclature.objects.get(code=171664)
#     art_too_short = len(nomen.art) < 5
#     print(f"4. Длина артикула < 5: {art_too_short} (артикул: '{nomen.art}', длина: {len(nomen.art)})")
#
#     # 5. Проверим финальный queryset
#     final_qs = (
#         base_qs
#         .exclude(
#             Q(supplier___mark_remove=1) | Q(code__in=getattr(settings, 'BLACKLISTED_CODES_FOR_RECOMMENDATIONS', [])))
#         .annotate(art_length=Length('art'))
#         .exclude(art_length__lt=5)
#     )
#     in_final = final_qs.filter(code=171664).exists()
#     print(f"5. В финальном queryset задачи: {in_final}")
#
#
# def debug_supplier_join():
#     """Детальная диагностика JOIN с поставщиками"""
#     print("\n=== ДИАГНОСТИКА JOIN С ПОСТАВЩИКАМИ ===")
#
#     # Способ 1: Проверим разные значения _mark_remove
#     from one_c_raw.models import Nomenclature
#     nomen = Nomenclature.objects.get(code=171664)
#
#     print("Проверка поставщиков через разные фильтры:")
#     print(f"  - _mark_remove=1: {nomen.supplier.filter(_mark_remove=1).count()}")
#     print(f"  - _mark_remove=True: {nomen.supplier.filter(_mark_remove=True).count()}")
#     print(f"  - _mark_remove=False: {nomen.supplier.filter(_mark_remove=False).count()}")
#
#     # Способ 2: Проверим сырые значения
#     print("\nСырые значения _mark_remove у поставщиков:")
#     for sup in nomen.supplier.all():
#         print(f"  - {sup.name}: _mark_remove={sup._mark_remove} (тип: {type(sup._mark_remove)})")
#
#
# if __name__ == "__main__":
#     test_specific_issue()
#     debug_supplier_join()

# python test_task_logic.py

from linked.helpers import get_ones_nomenclature_qs

join_result = get_ones_nomenclature_qs().filter(code=171664).filter(supplier___mark_remove=1).exists()
manual_result = get_ones_nomenclature_qs().get(code=171664).supplier.filter(_mark_remove=1).exists()

print(f"JOIN: {join_result}")
print(f"Ручная: {manual_result}")
print(f"Баг Django ORM: {join_result and not manual_result}")
# if __name__ == "__main__":
#     test_specific_issue()
#     debug_supplier_join()