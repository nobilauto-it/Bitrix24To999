# Маппинг полей Bitrix → 999.md (по данным API GET /features)

Данные получены вызовом **GET https://partners-api.999.md/features?category_id=658&subcategory_id=659&offer_type=776&lang=ru**.

## Как обновить фичи с 999

- **GET** `/api/publish-999md/features-from-999?lang=ru` — сырой JSON всех полей и опций.
- **GET** `/api/publish-999md/features-from-999/map?lang=ru` — список фич в виде `feature_id`, `title`, `options: [{id, title}]`.
- **POST** `/api/publish-999md/features-from-999/save?lang=ru` — скачать фичи с 999 и сохранить в `999md_features_cars_sell.json` (далее публикация использует этот файл).

## Ключевые поля 999 (легковые, Продам)

| feature_id | Название на 999   | Тип                 | Пример опций (id → title) |
|------------|-------------------|---------------------|---------------------------|
| 19         | Год выпуска       | textbox_numeric     | число (год)               |
| 20         | Марка             | drop_down_options   | 22 → Mercedes, 47 → Toyota |
| 21         | Модель            | drop_down_options   | опции зависят от марки (GET /dependent_options) |
| 102        | Тип кузова        | drop_down_options   | 27 → Универсал, 6 → Седан, 68 → Комби |
| 104        | Пробег            | textbox_numeric_measurement | число + unit "km" |
| 2553       | Двигатель         | drop_down_options   | 43684 → 2.0 л, 43680 → 1.6 л |
| 151        | Тип топлива       | drop_down_options   | 24 → Дизель, 10 → Бензин   |
| 108        | Привод            | drop_down_options   | 25 → Задний, 5 → Передний, 17 → 4х4 |
| 1196       | Руль              | drop_down_options   | 21979 → Левый, 21978 → Правый |
| 101        | КПП               | drop_down_options   | 16 → Автомат, 4 → Механика |
| 7          | Регион            | drop_down_options   | 12900 → Кишинёу и др.      |
| 12         | Заголовок         | textbox_text        | строка                    |
| 13         | body              | textarea_text       | описание                  |
| 14         | Фото              | upload_images       | массив image_id           |
| 16         | Контакты          | contacts            | массив телефонов         |
| 2          | Цена              | textbox_numeric_measurement | число + unit eur/usd/mdl |

## Явный маппинг Bitrix → 999 option id (в коде BITRIX_TO_999_OPTION)

- **102 (Тип кузова):** Universal / Универсал / Комби → **27**; Sedan / Седан → **6**.
- **151 (Тип топлива):** Motorina / Diesel / Дизель → **24**; Бензин / Gasoline → **10**.
- **108 (Привод):** Spate / Rear / Задний → **25**; Fata / Front / Передний → **5**; 4x4 → **17**.
- **2553 (Двигатель):** 2.0 / 2,0 → **43684**; 1.6 / 1,6 → **43680**.

Год (19) и пробег (104) передаются числом, не option id.

## Заголовок под фото (feature 12)

На 999 под фото выводится **заголовок** — мы отправляем только **Марка + Модель** (например, «Volkswagen Passat»). Номер машины в объявление не попадает: ни в заголовок, ни в текст.

## Год (feature 19): маппинг с нашим Bitrix

На 999 у поля «Год выпуска» тип **textbox_numeric**, списка опций нет — принимается просто число (год). Маппинг:

- Берём год из Bitrix (поле «Anul producerii» или по названию, содержащему anul/год/year).
- Отправляем на 999 то же число, ограничивая диапазоном **1990–2030** (константы `YEAR_MIN_999`, `YEAR_MAX_999`).
- Никакого перевода в option id не требуется: наш год → то же значение в feature 19.
