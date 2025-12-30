# find_problem_nomenclatures.py
import os
import django
import csv
from datetime import datetime

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'conf.docker')
django.setup()

from linked.helpers import get_ones_nomenclature_qs
from one_c_raw.models import Nomenclature


def find_problem_nomenclatures():
    """Находит все номенклатуры с расхождением между JOIN и ручной проверкой"""

    print("=== ПОИСК ПРОБЛЕМНЫХ НОМЕНКЛАТУР ===")

    # Берем ВСЕ номенклатуры для проверки
    all_nomenclatures = get_ones_nomenclature_qs().values('code', 'art', 'name')
    total_count = all_nomenclatures.count()

    print(f"Всего номенклатур для проверки: {total_count}")

    problem_nomenclatures = []
    processed = 0

    for nomen_data in all_nomenclatures:
        code = nomen_data['code']
        processed += 1

        # Прогресс каждые 100 номенклатур
        if processed % 100 == 0:
            print(f"Обработано: {processed}/{total_count} ({processed / total_count * 100:.1f}%)")

        try:
            # Проверяем расхождение
            join_result = get_ones_nomenclature_qs().filter(code=code).filter(supplier___mark_remove=1).exists()

            # Получаем объект номенклатуры для ручной проверки
            nomen_obj = get_ones_nomenclature_qs().get(code=code)
            manual_result = nomen_obj.supplier.filter(_mark_remove=1).exists()

            if join_result != manual_result:
                # Собираем информацию о всех поставщиках
                all_suppliers = nomen_obj.supplier.all()
                suppliers_info = []

                for supplier in all_suppliers:
                    suppliers_info.append({
                        'name': supplier.name,
                        'art': supplier.art,
                        '_mark_remove': supplier._mark_remove,
                        'uuid': supplier.uuid
                    })

                problem_nomenclatures.append({
                    'code': code,
                    'art': nomen_data['art'],
                    'name': nomen_data['name'],
                    'join_excludes': join_result,
                    'manual_check_excludes': manual_result,
                    'suppliers_count': len(all_suppliers),
                    'suppliers_with_mark_remove': len([s for s in all_suppliers if s._mark_remove]),
                    'suppliers': suppliers_info
                })

                print(f"🔴 Проблема: {code} - {nomen_data['art']} - {nomen_data['name'][:50]}...")
                print(f" .filter(supplier___mark_remove=1): {join_result} | .supplier.filter(_mark_remove=1): {manual_result}")
                print(
                    f"   Поставщиков всего: {len(all_suppliers)}, с _mark_remove=1: {len([s for s in all_suppliers if s._mark_remove])}")

        except Exception as e:
            print(f"❌ Ошибка с кодом {code}: {e}")

    print(f"\n=== ИТОГ ===")
    print(f"Всего проверено: {total_count}")
    print(f"Проблемных номенклатур: {len(problem_nomenclatures)}")
    print(f"Процент проблемных: {len(problem_nomenclatures) / total_count * 100:.2f}%")

    return problem_nomenclatures


def save_to_file(problem_nomenclatures):
    """Сохраняет проблемные номенклатуры в CSV файл с информацией о поставщиках"""
    if not problem_nomenclatures:
        print("Нет данных для сохранения")
        return

    # Создаем имя файла с временем
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"problem_nomenclatures_{timestamp}.csv"

    # Полный путь к файлу в корне проекта
    project_root = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(project_root, filename)

    # Сохраняем в CSV
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'code', 'art', 'name',
            'join_excludes', 'manual_check_excludes',
            'suppliers_count', 'suppliers_with_mark_remove',
            'suppliers_info'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for nomen in problem_nomenclatures:
            # Форматируем информацию о поставщиках для CSV
            suppliers_str = " | ".join([
                f"{s['name'][:30]}(арт:{s['art']},удален:{s['_mark_remove']})"
                for s in nomen['suppliers']
            ])

            writer.writerow({
                'code': nomen['code'],
                'art': nomen['art'],
                'name': nomen['name'],
                'join_excludes': nomen['join_excludes'],
                'manual_check_excludes': nomen['manual_check_excludes'],
                'suppliers_count': nomen['suppliers_count'],
                'suppliers_with_mark_remove': nomen['suppliers_with_mark_remove'],
                'suppliers_info': suppliers_str
            })

    print(f"✅ Результаты сохранены в: {filepath}")
    return filepath


def show_detailed_analysis(problem_nomenclatures):
    """Показывает детальный анализ проблемных номенклатур с поставщиками"""
    if not problem_nomenclatures:
        return

    print(f"\n=== ДЕТАЛЬНЫЙ АНАЛИЗ ПРОБЛЕМНЫХ НОМЕНКЛАТУР ===")

    for i, problem in enumerate(problem_nomenclatures[:10], 1):  # Первые 10 для анализа
        print(f"\n{i}. 🔴 ПРОБЛЕМНАЯ НОМЕНКЛАТУРА:")
        print(f"   Код: {problem['code']}")
        print(f"   Артикул: '{problem['art']}'")
        print(f"   Название: {problem['name'][:100]}...")
        print(f"   .filter(supplier___mark_remove=1): {problem['join_excludes']}")
        print(f"   .supplier.filter(_mark_remove=1): {problem['manual_check_excludes']}")
        print(f"   Всего поставщиков: {problem['suppliers_count']}")
        print(f"   Поставщиков с _mark_remove=1: {problem['suppliers_with_mark_remove']}")

        print(f"   📋 ВСЕ ПОСТАВЩИКИ:")
        for j, supplier in enumerate(problem['suppliers'], 1):
            status = "❌ УДАЛЕН" if supplier['_mark_remove'] else "✅ АКТИВЕН"
            print(f"      {j}. {status} | {supplier['name'][:50]}...")
            print(f"          Артикул: '{supplier['art']}', UUID: {supplier['uuid']}")

        print("   " + "=" * 80)


def show_problem_statistics(problem_nomenclatures):
    """Показывает статистику по проблемам"""
    if not problem_nomenclatures:
        return

    false_positives = [p for p in problem_nomenclatures if p['join_excludes'] and not p['manual_check_excludes']]
    false_negatives = [p for p in problem_nomenclatures if not p['join_excludes'] and p['manual_check_excludes']]

    print(f"\n=== СТАТИСТИКА ПРОБЛЕМ ===")
    print(f"Ложные исключения (JOIN ошибается): {len(false_positives)}")
    print(f"Пропущенные исключения (JOIN не видит): {len(false_negatives)}")

    if false_positives:
        print(f"\n--- ПЕРВЫЕ 3 ЛОЖНЫХ ИСКЛЮЧЕНИЯ ---")
        for p in false_positives[:3]:
            print(f"  Код {p['code']}: '{p['art']}'")
            print(f"    Название: {p['name'][:60]}...")
            print(f"    Поставщиков: {p['suppliers_count']}, с _mark_remove=1: {p['suppliers_with_mark_remove']}")


def auto_save(problem_nomenclatures):
    """Автоматически сохраняет проблемные номенклатуры в файл"""
    if not problem_nomenclatures:
        print("✅ Проблемных номенклатур не найдено!")
        return None

    print(f"\n=== АВТОМАТИЧЕСКОЕ СОХРАНЕНИЕ ===")

    filepath = save_to_file(problem_nomenclatures)

    print(f"✅ Результаты сохранены в файл: {filepath}")
    print(f"📊 Найдено проблемных номенклатур: {len(problem_nomenclatures)}")

    return filepath


if __name__ == "__main__":
    # Находим проблемные номенклатуры
    problems = find_problem_nomenclatures()

    # Показываем детальный анализ
    show_detailed_analysis(problems)

    # Показываем статистику
    show_problem_statistics(problems)

    # Предлагаем сохранить в файл
    auto_save(problems)


# python find_problem_nomenclatures.py