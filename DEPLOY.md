# Развёртывание API 999.md на сервере

## 1. Что закинуть на сервер

Скопируй в каталог проекта (например `/opt/api_999` или домашний каталог) файлы:

- `app.py`
- `publish_999md.py`
- `requirements.txt`
- `.env` (если есть; не коммить секреты в git)

И один раз получить и положить рядом с `publish_999md.py` файл:

- **`999md_features_cars_sell.json`** — ответ API 999 «фичи для категории авто, продам»:

```bash
# С сервера (подставь свой токен):
curl -u "ТВОЙ_API_999MD_TOKEN:" "https://partners-api.999.md/features?category_id=658&subcategory_id=659&offer_type=776&lang=ru" -o 999md_features_cars_sell.json
```

Без этого файла публикация падает с `FileNotFoundError`.

---

## 2. Переменные окружения

В `.env` или в systemd-юните задай:

- `API_999MD_TOKEN` — токен 999.md (обязательно)
- `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASS` — доступ к БД с таблицами `b24_sp_f_1114`, `b24_meta_fields` и т.д.
- Вебхук Bitrix захардкожен в `publish_999md.py` (BITRIX_WEBHOOK_DEFAULT)
- По желанию: `PUBLISH_999MD_PHONE`, `HTTP_PORT_999MD` (по умолчанию 8086), `HTTP_HOST` (по умолчанию 0.0.0.0)

---

## 3. Виртуальное окружение и зависимости

```bash
cd /opt/api_999   # или твой путь
python3 -m venv venv
source venv/bin/activate   # Linux/macOS
# Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

## 4. Запуск вручную (проверка)

```bash
source venv/bin/activate
python app.py
# или:
uvicorn app:app --host 0.0.0.0 --port 8086
```

Проверка: `curl http://localhost:8086/health` и `http://SERVER:8086/docs`.

---

## 5. Сервис systemd (перезапуск сервиса)

Файл сервиса, например `/etc/systemd/system/api-999md.service`:

```ini
[Unit]
Description=API Publish 999.md
After=network.target

[Service]
Type=simple
User=www-data
Group=www-data
WorkingDirectory=/opt/api_999
EnvironmentFile=/opt/api_999/.env
ExecStart=/opt/api_999/venv/bin/uvicorn app:app --host 0.0.0.0 --port 8086
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Подставь свой `User`, `WorkingDirectory` и путь к `.env`.

Дальше на сервере:

```bash
sudo systemctl daemon-reload
sudo systemctl enable api-999md
sudo systemctl start api-999md
sudo systemctl status api-999md
```

**Перезапуск после обновления кода:**

```bash
sudo systemctl restart api-999md
```

Проверка логов:

```bash
sudo journalctl -u api-999md -f
```

---

## Ошибка «Не удалось загрузить ни одного фото (401/403 от Bitrix)»

Значит Bitrix не отдаёт файлы по вебхуку. Что проверить:

1. **В другом проекте работало** — там в raw у фото мог быть **urlMachine** (прямая ссылка). Здесь в raw только **url** (ajax) → запрос идёт через REST по вебхуку → 401/403.
2. **Вебхук** захардкожен в коде (`BITRIX_WEBHOOK_DEFAULT`). Если истёк — поменяй константу в `publish_999md.py`. В Bitrix24: права вебхука **CRM**.
3. **Либо** в источнике данных (скрипт/синхронизация, который пишет в `b24_sp_f_1114.raw`) добавь в каждое фото поле **urlMachine** — прямая ссылка на файл, без Bitrix REST. Тогда вебхук не нужен для фото.

В логах и в тексте ошибки теперь выводится код ответа и конец URL первого неудачного запроса.

---

## Кратко: что сделать на сервере

1. Закинуть в каталог проекта: `app.py`, `publish_999md.py`, `requirements.txt`, `.env`.
2. Скачать и положить `999md_features_cars_sell.json` (curl выше).
3. `python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt`
4. Создать/обновить `/etc/systemd/system/api-999md.service` (как выше).
5. **Перезапуск сервиса:** `sudo systemctl restart api-999md`
