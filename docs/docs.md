## Django Scripts

1. **update_not_recommend.py**

Скрипт берет из таблицы `RecommendedLinked` бэкапа все позиции с `not_recommend=True` 
и обновляет такое же поле в тех рекомендациях продовской базы, которые находятся в этом списке.


