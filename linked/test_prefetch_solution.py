# test_prefetch_solution_correct.py
import os
import django
import csv
from datetime import datetime
from django.db.models import Prefetch

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'conf.docker')
django.setup()

from linked.helpers import get_ones_nomenclature_qs
from one_c_raw.models import Nomenclature, SupplierNomenclature


def test_original_vs_prefetch():
    """Точное сравнение оригинального подхода и Prefetch"""

    print("=== ТОЧНОЕ СРАВНЕНИЕ ОРИГИНАЛЬНОГО ПОДХОДА И PREFETCH ===")

    # Берем тестовую выборку (первые 200 для скорости)
    test_nomenclatures = list(get_ones_nomenclature_qs()[:200])
    print(f"Тестируем на {len(test_nomenclatures)} номенклатурах")

    problem_nomenclatures = []

    for i, nomen in enumerate(test_nomenclatures, 1):
        if i % 50 == 0:
            print(f"Обработано {i}/{len(test_nomenclatures)}")

        try:
            # ⚠️ ОРИГИНАЛЬНЫЙ СПОСОБ (проблемный)
            # Именно так работает в find_problem_nomenclatures.py
            join_excludes = get_ones_nomenclature_qs().filter(
                code=nomen.code
            ).filter(
                supplier___mark_remove=1
            ).exists()

            # ⚠️ РУЧНАЯ ПРОВЕРКА (как в оригинале)
            manual_excludes = nomen.supplier.filter(_mark_remove=1).exists()

            if join_excludes != manual_excludes:
                # Собираем детальную информацию
                all_suppliers = list(nomen.supplier.all())
                removed_suppliers = list(nomen.supplier.filter(_mark_remove=1))

                problem_nomenclatures.append({
                    'code': nomen.code,
                    'art': nomen.art,
                    'name': nomen.name,
                    'join_excludes': join_excludes,
                    'manual_excludes': manual_excludes,
                    'all_suppliers_count': len(all_suppliers),
                    'removed_suppliers_count': len(removed_suppliers),
                    'suppliers_info': [
                        {
                            'name': s.name,
                            'art': s.art,
                            '_mark_remove': s._mark_remove,
                            'uuid': s.uuid
                        } for s in all_suppliers
                    ]
                })

                print(f"🔴 Расхождение: {nomen.code}")
                print(f"   JOIN: {join_excludes}, Ручная: {manual_excludes}")
                print(f"   Поставщиков: {len(all_suppliers)}, удаленных: {len(removed_suppliers)}")

        except Exception as e:
            print(f"❌ Ошибка с {nomen.code}: {e}")

    print(f"\n📊 Найдено проблем оригинальным способом: {len(problem_nomenclatures)}")
    return problem_nomenclatures


def test_prefetch_solution(problem_nomenclatures):
    """Тестируем Prefetch решение на проблемных номенклатурах"""

    if not problem_nomenclatures:
        print("Нет проблемных номенклатур для тестирования Prefetch")
        return []

    print("\n" + "=" * 60)
    print("=== ТЕСТИРОВАНИЕ PREFETCH РЕШЕНИЯ ===")

    # Получаем коды проблемных номенклатур
    problem_codes = [p['code'] for p in problem_nomenclatures]

    # Создаем Prefetch для удаленных поставщиков
    removed_suppliers_prefetch = Prefetch(
        'supplier',
        queryset=SupplierNomenclature.objects.filter(_mark_remove=1),
        to_attr='prefetched_removed_suppliers'
    )

    # Загружаем проблемные номенклатуры с Prefetch
    prefetched_nomens = get_ones_nomenclature_qs().filter(
        code__in=problem_codes
    ).prefetch_related(removed_suppliers_prefetch)

    # Создаем словарь для быстрого доступа
    prefetched_by_code = {nomen.code: nomen for nomen in prefetched_nomens}

    prefetch_results = []

    for problem in problem_nomenclatures:
        nomen = prefetched_by_code.get(problem['code'])
        if not nomen:
            continue

        # Оригинальные результаты
        original_join = problem['join_excludes']
        original_manual = problem['manual_excludes']

        # Prefetch результат
        prefetch_excludes = bool(nomen.prefetched_removed_suppliers)

        # Сравниваем Prefetch с ручной проверкой (они должны совпадать)
        prefetch_correct = (prefetch_excludes == original_manual)

        prefetch_results.append({
            **problem,
            'prefetch_excludes': prefetch_excludes,
            'prefetch_correct': prefetch_correct,
            'prefetched_count': len(nomen.prefetched_removed_suppliers)
        })

        if not prefetch_correct:
            print(f"❌ Prefetch ошибка: {problem['code']}")
            print(f"   Prefetch: {prefetch_excludes}, Ручная: {original_manual}")
            print(f"   Prefetch нашел: {len(nomen.prefetched_removed_suppliers)} удаленных")

    return prefetch_results


def analyze_original_problems(problem_nomenclatures):
    """Анализ проблем оригинального подхода"""

    if not problem_nomenclatures:
        return

    print(f"\n🔍 АНАЛИЗ ПРОБЛЕМ ОРИГИНАЛЬНОГО ПОДХОДА:")

    false_positives = [p for p in problem_nomenclatures if p['join_excludes'] and not p['manual_excludes']]
    false_negatives = [p for p in problem_nomenclatures if not p['join_excludes'] and p['manual_excludes']]

    print(f"   Ложные исключения JOIN: {len(false_positives)}")
    print(f"   Пропущенные исключения JOIN: {len(false_negatives)}")

    if false_positives:
        print(f"\n   📋 ПРИМЕРЫ ЛОЖНЫХ ИСКЛЮЧЕНИЙ JOIN (JOIN ошибается):")
        for p in false_positives[:3]:
            print(f"     Код: {p['code']}")
            print(f"     Артикул: '{p['art']}'")
            print(f"     JOIN говорит 'исключить', но ручная проверка не находит удаленных")
            print(f"     Всего поставщиков: {p['all_suppliers_count']}")
            print(f"     Удаленных поставщиков: {p['removed_suppliers_count']}")

    if false_negatives:
        print(f"\n   📋 ПРИМЕРЫ ПРОПУЩЕННЫХ ИСКЛЮЧЕНИЙ (JOIN не видит):")
        for p in false_negatives[:3]:
            print(f"     Код: {p['code']}")
            print(f"     Артикул: '{p['art']}'")
            print(f"     JOIN НЕ исключает, но ручная проверка находит удаленных")
            print(f"     Всего поставщиков: {p['all_suppliers_count']}")
            print(f"     Удаленных поставщиков: {p['removed_suppliers_count']}")


def analyze_prefetch_performance():
    """Тест производительности Prefetch vs Оригинальный подход"""

    print(f"\n⚡ ТЕСТ ПРОИЗВОДИТЕЛЬНОСТИ:")

    import time
    test_size = 100
    test_nomenclatures = list(get_ones_nomenclature_qs()[:test_size])

    # Оригинальный подход (N+1 запросов)
    print("   Оригинальный подход (N+1)...")
    start_time = time.time()

    original_problems = 0
    for nomen in test_nomenclatures:
        join_excludes = get_ones_nomenclature_qs().filter(
            code=nomen.code
        ).filter(
            supplier___mark_remove=1
        ).exists()
        manual_excludes = nomen.supplier.filter(_mark_remove=1).exists()
        if join_excludes != manual_excludes:
            original_problems += 1

    original_time = time.time() - start_time

    # Prefetch подход
    print("   Prefetch подход...")
    start_time = time.time()

    removed_suppliers_prefetch = Prefetch(
        'supplier',
        queryset=SupplierNomenclature.objects.filter(_mark_remove=1),
        to_attr='prefetched_removed_suppliers'
    )

    prefetched_nomens = get_ones_nomenclature_qs().filter(
        code__in=[n.code for n in test_nomenclatures]
    ).prefetch_related(removed_suppliers_prefetch)

    prefetch_problems = 0
    for nomen in prefetched_nomens:
        join_excludes = get_ones_nomenclature_qs().filter(
            code=nomen.code
        ).filter(
            supplier___mark_remove=1
        ).exists()
        prefetch_excludes = bool(nomen.prefetched_removed_suppliers)
        if join_excludes != prefetch_excludes:
            prefetch_problems += 1

    prefetch_time = time.time() - start_time

    print(f"   Результаты ({test_size} номенклатур):")
    print(f"   Оригинальный: {original_time:.2f}с, проблем: {original_problems}")
    print(f"   Prefetch:     {prefetch_time:.2f}с, проблем: {prefetch_problems}")

    if prefetch_time > 0:
        speedup = original_time / prefetch_time
        print(f"   Ускорение: {speedup:.1f} раз")


def save_detailed_results(problem_nomenclatures, prefetch_results):
    """Сохраняет детальные результаты"""

    if not problem_nomenclatures:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"prefetch_detailed_analysis_{timestamp}.csv"
    project_root = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(project_root, filename)

    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            'Код', 'Артикул', 'Название',
            'JOIN_исключает', 'Ручная_проверка', 'PREFETCH_исключает',
            'Всего_поставщиков', 'Удаленных_поставщиков', 'PREFETCH_нашел',
            'Тип_проблемы', 'PREFETCH_корректен'
        ])

        for problem in problem_nomenclatures:
            # Находим соответствующий Prefetch результат
            prefetch_data = next((p for p in prefetch_results if p['code'] == problem['code']), {})

            problem_type = "Ложное_исключение" if problem['join_excludes'] and not problem[
                'manual_excludes'] else "Пропуск_исключения"
            prefetch_correct = prefetch_data.get('prefetch_correct', False)
            prefetch_excludes = prefetch_data.get('prefetch_excludes', False)
            prefetched_count = prefetch_data.get('prefetched_count', 0)

            writer.writerow([
                problem['code'],
                problem['art'],
                problem['name'][:100] if problem['name'] else '',
                problem['join_excludes'],
                problem['manual_excludes'],
                prefetch_excludes,
                problem['all_suppliers_count'],
                problem['removed_suppliers_count'],
                prefetched_count,
                problem_type,
                prefetch_correct
            ])

    print(f"📁 Детальные результаты сохранены в: {filepath}")


if __name__ == "__main__":
    print("🚀 ЗАПУСК ТОЧНОГО СРАВНЕНИЯ PREFETCH И ОРИГИНАЛЬНОГО ПОДХОДА")
    print("=" * 70)

    try:
        # 1. Находим проблемы оригинальным способом (как в первом скрипте)
        original_problems = test_original_vs_prefetch()

        # 2. Анализируем типы проблем
        analyze_original_problems(original_problems)

        # 3. Тестируем Prefetch решение
        prefetch_results = test_prefetch_solution(original_problems)

        # 4. Тестируем производительность
        analyze_prefetch_performance()

        # 5. Сохраняем результаты
        save_detailed_results(original_problems, prefetch_results)

        print("\n" + "=" * 70)
        print("🎯 ИТОГИ:")

        if original_problems:
            print(f"✅ Подтверждено: оригинальный подход имеет {len(original_problems)} проблем")

            # Проверяем корректность Prefetch
            correct_prefetch = sum(1 for p in prefetch_results if p.get('prefetch_correct', False))
            total_prefetch = len(prefetch_results)

            if total_prefetch > 0:
                accuracy = correct_prefetch / total_prefetch * 100
                print(f"📊 Prefetch точность: {accuracy:.1f}% ({correct_prefetch}/{total_prefetch})")

                if accuracy == 100:
                    print("🎉 Prefetch РЕШАЕТ все проблемы оригинального подхода!")
                else:
                    print("⚠️  Prefetch решает большинство проблем, но есть расхождения")

        else:
            print("🤔 Не найдено проблем оригинальным способом - возможно, данные изменились")

        print("\n💡 РЕКОМЕНДАЦИЯ:")
        print("   Используйте Prefetch вместо JOIN для кросс-базовых отношений")
        print("   Это решит проблему расхождений и улучшит производительность")

    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        import traceback

        traceback.print_exc()


# python test_prefetch_solution.py