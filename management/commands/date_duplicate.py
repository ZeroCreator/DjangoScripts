from django.core.management.base import BaseCommand
from django.db.models import F, Count, Min
from django.db.models.functions import Lower, Trim

from kenny.items.models import Item, Competitor

class Command(BaseCommand):
    help = 'Находит дубликаты артикулов с пробелами и выводит дату их первого появления в отсортированном виде'

    def add_arguments(self, parser):
        parser.add_argument('competitor_id', type=int, help='ID конкурента для поиска дубликатов')
        parser.add_argument('--output', type=str, default='duplicates_report.txt', help='Файл для сохранения отчёта')

    def handle(self, *args, **options):
        competitor_id = options['competitor_id']
        output_file = options['output']

        try:
            competitor = Competitor.objects.get(id=competitor_id)
        except Competitor.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'Конкурент с ID {competitor_id} не найден'))
            return

        self.stdout.write(f'Поиск дубликатов для конкурента: {competitor}')

        items = Item.objects.filter(competitor=competitor).annotate(
            normalized_article=Lower(Trim(F('article')))
        )

        duplicate_articles = items.values('normalized_article').annotate(
            count=Count('id')
        ).filter(count__gt=1).values_list('normalized_article', flat=True)

        if not duplicate_articles:
            self.stdout.write('Дубликатов не найдено.')
            return

        duplicates_with_spaces = items.filter(normalized_article__in=duplicate_articles).exclude(
            article__iexact=F('normalized_article')
        )

        grouped = duplicates_with_spaces.values('normalized_article').annotate(
            count=Count('id'),
            first_date=Min('date_create')
        ).order_by('first_date')

        lines = ['Артикулы с дубликатами и даты первого появления (от ранней к поздней):\n']

        for dup in grouped:
            line = f"Артикул: {dup['normalized_article']}, Кол-во с пробелами: {dup['count']}, С даты: {dup['first_date']}"
            self.stdout.write(line)
            lines.append(line)

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))

        self.stdout.write(self.style.SUCCESS(f'Отчёт сохранён в файл: {output_file}'))


# python manage.py date_duplicate 142
# python manage.py date_duplicate <ID_конкурента> --output файл.txt