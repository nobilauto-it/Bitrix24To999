#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Публикация объявлений о машинах на 999.md (ручная отправка, потом автоматизация).
API: https://partners-api.999.md
Токен из env API_999MD_TOKEN (или захардкожен).
Данные машины: только из b24_sp_f_1114.raw (jsonb). Названия полей — из b24_meta_fields.b24_title (entity_key=sp:1114).
Фото: только raw["ufCrm34_1756897294"], берём "url" (ajax.php), для скачивания — REST URL или urlMachine (вебхук подставляется из env BITRIX_WEBHOOK).
ID (iblock_element) расшифровываются через таблицу/кэш или REST lists.element.get, иначе остаются цифрами.

Важно: по доке 999.md access_policy только "private"|"public" (draft нет). Создаём с private и сразу ставим private — объявление в «Скрытые», не в «Активные».
Никогда не публикуем автоматически — сотрудник смотрит в «Черновики» на 999 и сам нажимает «Опубликовать».

Запуск как сервис (отдельный порт):
  API_999MD_TOKEN=... BITRIX_WEBHOOK=... PG_HOST=... PG_USER=... PG_PASS=... python publish_999md.py
Или подключить router в существующий FastAPI: app.include_router(publish_999md.router).
"""
import os
import re
import json
import sys
import unicodedata
import urllib.parse
from dataclasses import dataclass
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

API_BASE = "https://partners-api.999.md"

# -----------------------------
# DB (те же переменные, что в auto_send_tg — из env или дефолты; api_data не используем)
# -----------------------------
PG_HOST = os.getenv("PG_HOST", "194.33.40.197")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "crm")
PG_USER = os.getenv("PG_USER", "crm")
PG_PASS = os.getenv("PG_PASS", "crm")


def _pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        connect_timeout=10,
    )


# Таблица и поля как в auto_send_tg (b24_sp_f_1114)
DATA_TABLE_SP1114 = "public.b24_sp_f_1114"
CATEGORY_ID_SP1114 = "111"
PHOTO_RAW_KEY = "ufCrm34_1756897294"
RAW_FIELDS_MARCA = "ufCrm34_1748347910"
RAW_FIELDS_MODEL = "ufCrm34_1748431620"
RAW_FIELDS_YEAR = "ufCrm34_1748347979"
RAW_FIELDS_BODY = "ufCrm34_1749208724"      # Caroserie (IBLOCK 100)
RAW_FIELDS_ENGINE = "ufCrm34_1748431775"     # Volumul motorului (IBLOCK 42)
RAW_FIELDS_FUEL = "ufCrm34_1748431413"       # Tipul de combustibil (IBLOCK 36)
RAW_FIELDS_DRIVE = "ufCrm34_1748431272"      # Tracțiune (Привод): 4x4, Fața, Spate
RAW_FIELDS_TRANSMISSION = "ufCrm34_1748348015"  # Transmisie (КПП): Автомат, Механика, Вариатор, Робот, Типтроник, Полуавтоматическая
RAW_FIELDS_PRICE = "ufCrm34_1756980662"
RAW_FIELDS_MILEAGE = "ufCrm34_1748431531"
RAW_FIELDS_LINK = "ufCrm34_1756926228375"
LANG = "ru"
CATEGORY_ID = "658"
SUBCATEGORY_ID = "659"
OFFER_TYPE = "776"

# Режим «только локальные черновики»: не вызывать POST /adverts, только сохранять payload в 999md_drafts/.
# 0 = шлём POST на 999 (объявление уходит в «Скрытые», access_policy private). 1 = только локальный черновик (не шлём на 999).
PUBLISH_999MD_DRAFT_ONLY = os.getenv("PUBLISH_999MD_DRAFT_ONLY", "0").strip().lower() in ("1", "true", "yes")
DRAFTS_DIR = Path(__file__).resolve().parent / "999md_drafts"

# Авто-отправка на 999 (как в auto_send_tg): опрос БД раз в 10 мин, одна машина за раз, все поля заполнены, фото 5–10.
AUTO_PUBLISH_999_ENABLED = True
POLL_INTERVAL_999 = 600          # секунды (10 мин) — проверка машин раз в 10 минут, одна машина в 10 минут
SEND_WINDOW_START_HOUR = 9       # с 9:00
SEND_WINDOW_END_HOUR = 21        # до 21:00
SENT_999_TABLE = "public.b24_999_sent_items"
# Таблица отправленных в TG_AUTO (auto_send_tg). На 999 шлём ТОЛЬКО те машины, что уже есть в TG — одна логика публикации.
SENT_TG_TABLE = "public.tg_sent_items"
# === STRICT FILTER EXACTLY LIKE auto_send_tg.py ===
REQUIRE_ALL_FILLED_FIELDS = [
    "ufCrm34_1748347910",
    "ufCrm34_1748431620",
    "ufCrm34_1748431272",
    "ufCrm34_1748348015",
    "ufCrm34_1748347979",
    "ufCrm34_1748431775",
    "ufCrm34_1748431413",
    "ufCrm34_1748431531",
    "ufCrm34_1756980662",
    "ufCrm34_1756926228375",
    "ufCrm34_1756897294",  # ФОТО ОБЯЗАТЕЛЬНО
]
REQUIRE_ALL_FILLED_SCALAR_FIELDS = [
    "ufCrm34_1748347910",
    "ufCrm34_1748431620",
    "ufCrm34_1748431272",
    "ufCrm34_1748348015",
    "ufCrm34_1748347979",
    "ufCrm34_1748431775",
    "ufCrm34_1748431413",
    "ufCrm34_1748431531",
    "ufCrm34_1756980662",
    "ufCrm34_1756926228375",
]
MAX_AGE_DAYS_FOR_AUTO_999 = 14
MIN_PHOTOS_999 = 5
MAX_PHOTOS_999 = 10

# Telegram: уведомление при успешной отправке объявления на 999
TG_BOT_TOKEN_999 = "8531012748:AAG0DEWrzJgrpdapDuRM7xIKoPmpNR2vivc"
TG_CHAT_ID_999 = "-1003729467376"
# Имя воронки по categoryId (для текста уведомления). Пока одна: 111 -> Vinzari realizari
FUNNEL_NAMES: Dict[str, str] = {
    "111": "Vinzari realizari",
}

# Маппинг stageId (воронка 111) -> название этапа. Пока только подготовка, логика — позже.
STAGE_ID_TO_NAME: Dict[str, str] = {
    "DT1114_111:UC_83N1DP": "Nobil 1",
    "DT1114_111:UC_8NMLNS": "Nobil 2",
    "DT1114_111:UC_7R6IQX": "Nobil Arena",
}

# Дефолтные option id для payload 999 (чтобы API принимал объявление, если по Bitrix не удалось разрешить)
DEFAULTS_PAYLOAD_999: Dict[str, str] = {
    "775": "18592",
    "593": "18668",
    "1761": "29670",
    "1763": "29677",
    "795": "23241",
    "1196": "21979",
    "846": "19119",
    "102": "6",
    "2553": "43680",
    "151": "10",
    "108": "5",
    "101": "4",
    "7": "12900",
}

# 999.md фича 19 (год): textbox_numeric, не список опций — отправляем число. Диапазон по API.
YEAR_MIN_999 = 1990
YEAR_MAX_999 = 2030

# Явный маппинг Bitrix (значение, lower) -> 999 option id. По данным GET /features (lang=ru).
# 102=Тип кузова. Bitrix Caroserie (IBLOCK 100): Hatchback, MPV, Sedan, Universal, SUV, Bus|Passageri, Bus|Cargo, Coupe, Cabrio, Evacuator, Minivan.
# 999.md: Внедорожник=18, Кабриолет=156, Комби=68, Кроссовер=74, Купе=96, Микровэн=53, Минивэн=49, Пикап=61, Родстер=265, Седан=6, Универсал=27, Фургон=97, Хетчбэк=11.
BITRIX_TO_999_OPTION: Dict[str, Dict[str, str]] = {
    "102": {
        "universal": "27", "sedan": "6", "универсал": "27", "седан": "6", "комби": "27", "wagon": "27",
        "hatchback": "11", "хетчбэк": "11",
        "mpv": "49", "minivan": "49", "минивэн": "49", "микровэн": "53",
        "suv": "18", "внедорожник": "18",
        "coupe": "96", "купе": "96",
        "cabrio": "156", "кабриолет": "156", "cabriolet": "156",
        "evacuator": "97", "фургон": "97",
        "bus | passageri": "49", "bus | cargo": "97",
    },
    # 151=Тип топлива. Bitrix: Benzina 95, Electro, Gaz/Benzina, Hybrid/Benzina, Hybrid/Motorina, Motorina, Plug-in Hybrid.
    # 999.md: Бензин=10, Дизель=24, Газ/Бензин(пропан)=3, Гибрид=161, Электричество=12617, Плагин-гибрид(бензин)=22987, Плагин-гибрид(дизель)=43422, Мягкий гибрид(бензин)=43424, Мягкий гибрид(дизель)=43423.
    "151": {
        "motorina": "24", "diesel": "24", "дизель": "24",
        "benzina 95": "10", "benzina": "10", "бензин": "10", "gasoline": "10", "benzin": "10",
        "electro": "12617", "electricity": "12617", "электричество": "12617",
        "gaz/benzina": "3", "газ / бензин (пропан)": "3", "газ/бензин": "3",
        "hybrid/benzina": "161", "hybrid": "161", "гибрид": "161",
        "hybrid/motorina": "43423", "гибрид дизель": "43423",
        "plug-in hybrid": "22987", "plug-in hybrid (gasoline)": "22987", "плагин-гибрид (бензин)": "22987",
        "plug-in hybrid (diesel)": "43422", "плагин-гибрид (дизель)": "43422",
        "мягкий гибрид (бензин)": "43424", "мягкий гибрид (дизель)": "43423",
    },
    # 108=Привод. Bitrix Tracțiune: 4x4, Fața (передний), Spate (задний).
    "108": {"spate": "25", "rear": "25", "задний": "25", "fata": "5", "front": "5", "передний": "5", "faţa": "5", "fața": "5", "4x4": "17", "4х4": "17"},
    # 101=КПП. Bitrix Transmisie / 999: Автомат=16, Вариатор=1051, Механика=4, Полуавтоматическая=29422, Робот=1054, Типтроник=1052.
    "101": {
        "автомат": "16", "automat": "16", "automatic": "16",
        "механика": "4", "mecanica": "4", "manual": "4", "механик": "4",
        "вариатор": "1051", "variator": "1051", "cvt": "1051",
        "робот": "1054", "robot": "1054", "роботизированная": "1054", "роботизированная коробка": "1054",
        "типтроник": "1052", "tiptronic": "1052",
        "полуавтоматическая": "29422", "полуавтомат": "29422", "semi-automatic": "29422",
    },
    "2553": {"2.0": "43684", "2,0": "43684", "2.5": "43689", "2,5": "43689", "1.6": "43680", "1,6": "43680", "2": "43684", "1.8": "43682", "1,8": "43682"},
}

# Meta + iblock/enum для расшифровки raw (как в auto_send_tg)
META_TABLE = "public.b24_meta_fields"
ENTITY_KEY_SP1114 = "sp:1114"
IBLOCK_CACHE_TABLE = "public.b24_iblock_elements"
ENUM_CACHE_TABLE = "public.b24_field_enum_cache"
SMART_ENTITY_TYPE_ID = 1114
IBLOCK_TYPE_ID_FALLBACK = "lists"
IBLOCK_TYPE_ID_CANDIDATES = ["lists", "lists_socnet", "crm"]
ENABLE_IBLOCK_API_LOOKUP = True
ENABLE_ENUM_API_LOOKUP = True

# Токен 999.md: env API_999MD_TOKEN или запасной (подставь свой)
API_999MD_TOKEN_DEFAULT = os.getenv("API_999MD_TOKEN", "TreE0PnGG7MGZJUuxZUwDWN_UZNY")

# Вебхук Bitrix24: только захардкоженный (без .env)
BITRIX_WEBHOOK_DEFAULT = "https://nobilauto.bitrix24.ru/rest/18397/h5c7kw97sfp3uote"


def _bitrix_webhook() -> str:
    return BITRIX_WEBHOOK_DEFAULT


def _token() -> str:
    return (os.getenv("API_999MD_TOKEN") or "").strip() or API_999MD_TOKEN_DEFAULT


def _auth() -> tuple:
    return (_token(), "")


def _get(path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
    url = API_BASE + path
    r = requests.get(url, auth=_auth(), params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()


def get_features_from_api(lang: str = "ru") -> Dict[str, Any]:
    """Получить все поля (features) подкатегории «Легковые автомобили» с 999.md API.
    Параметры: category_id=658, subcategory_id=659, offer_type=776 (Продам)."""
    return _get("/features", params={
        "category_id": CATEGORY_ID,
        "subcategory_id": SUBCATEGORY_ID,
        "offer_type": OFFER_TYPE,
        "lang": lang,
    })


def _post_json(path: str, data: Dict[str, Any], params: Optional[Dict[str, str]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    url = API_BASE + path
    r = requests.post(url, auth=_auth(), json=data, params=params or {}, headers=headers or {}, timeout=60)
    if not r.ok:
        try:
            err_body = r.json()
        except Exception:
            err_body = r.text[:1000]
        raise RuntimeError(
            f"999.md API {r.status_code}: {err_body}"
        )
    return r.json()


def _patch_json(path: str, data: Dict[str, Any]) -> Dict[str, Any]:
    url = API_BASE + path
    r = requests.patch(url, auth=_auth(), json=data, timeout=60)
    if not r.ok:
        try:
            err_body = r.json()
        except Exception:
            err_body = r.text[:500]
        raise RuntimeError(f"999.md API PATCH {r.status_code}: {err_body}")
    return r.json()


def _load_features_json() -> Dict[str, Any]:
    """Фичи для подкатегории 659 (легковые). Сначала из API, если USE_999_FEATURES_API=1 и токен есть."""
    if os.getenv("USE_999_FEATURES_API", "").strip().lower() in ("1", "true", "yes") and _token():
        try:
            return get_features_from_api(lang=LANG)
        except Exception:
            pass
    p = Path(__file__).resolve().parent / "999md_features_cars_sell.json"
    if not p.exists():
        raise FileNotFoundError(
            f"999md_features_cars_sell.json not found: {p}. "
            "Вызови POST /api/publish-999md/features-from-999/save чтобы скачать фичи с 999.md."
        )
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def _find_feature_option(features_data: Dict, feature_id: str, title_normalize: str) -> Optional[str]:
    """Ищем option id по title в фиче (например Марка 20)."""
    title_norm = (title_normalize or "").strip().lower()
    if not title_norm:
        return None
    for grp in features_data.get("features_groups", []):
        for feat in grp.get("features", []):
            if str(feat.get("id")) != str(feature_id):
                continue
            for opt in feat.get("options") or []:
                t = (opt.get("title") or "").strip().lower()
                if t == title_norm or t.replace(" ", "") == title_norm.replace(" ", ""):
                    return str(opt.get("id"))
                if title_norm in t or t in title_norm:
                    return str(opt.get("id"))
    return None


def get_dependent_options(dependency_feature_id: str, parent_option_id: str) -> List[Dict]:
    """
    Опции, зависящие от родителя. Для моделей: dependency_feature_id=21 (модель), parent=id марки.
    """
    data = _get("/dependent_options", params={
        "subcategory_id": SUBCATEGORY_ID,
        "dependency_feature_id": dependency_feature_id,
        "parent_option_id": parent_option_id,
        "lang": LANG,
    })
    options = data.get("options") or data.get("Options") or data.get("dependent_options") or data.get("items")
    if isinstance(options, list):
        return options
    return []


def resolve_brand_option_id(marca: str, features_data: Optional[Dict] = None) -> Optional[str]:
    """Марка из нашей БД -> option id 999 (фича 20)."""
    if not marca or not str(marca).strip():
        return None
    data = features_data or _load_features_json()
    for raw in (marca.strip(), marca.strip().split()[0] if marca.strip() else ""):
        if not raw:
            continue
        opt_id = _find_feature_option(data, "20", raw)
        if opt_id:
            return opt_id
    return _find_feature_option(data, "20", marca)


def _normalize_for_match(s: str) -> str:
    """Нижний регистр, без пробелов, без диакритики (Scénic -> scenic)."""
    if not s:
        return ""
    s = (s or "").strip().lower().replace(" ", "")
    nfd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def resolve_model_option_id(brand_option_id: str, model: str) -> Optional[str]:
    """Модель из БД -> option id 999. C Class не должен матчиться с CLA (строгое сравнение)."""
    if not model or not str(model).strip():
        return None
    options = get_dependent_options("20", brand_option_id)
    model_norm = _normalize_for_match(model)
    model_first = model_norm[:4] if len(model_norm) >= 4 else model_norm
    for opt in options:
        raw_t = (opt.get("title") or opt.get("value") or "").strip()
        t_norm = _normalize_for_match(raw_t)
        if not t_norm:
            continue
        # Точное совпадение — приоритет
        if model_norm == t_norm:
            return str(opt.get("id"))
        # Наша модель — подстрока опции (C Class в C Class Coupe)
        if model_norm in t_norm:
            return str(opt.get("id"))
        # Опция — подстрока нашей модели только если длина близка (не CLA в C Class)
        if t_norm in model_norm:
            if len(t_norm) >= len(model_norm) - 1:
                return str(opt.get("id"))
            continue
        if model_norm.startswith(t_norm) or t_norm.startswith(model_norm):
            if len(t_norm) >= 3 and len(model_norm) >= 3 and abs(len(t_norm) - len(model_norm)) <= 2:
                return str(opt.get("id"))
        if model_first and len(model_first) >= 4 and t_norm.startswith(model_first):
            return str(opt.get("id"))
    if options:
        return str(options[0].get("id"))
    return None


def resolve_generation_option_id(model_option_id: str) -> Optional[str]:
    """Поколение (2095) зависит от модели (21)."""
    options = get_dependent_options("21", model_option_id)
    if options:
        return str(options[0].get("id"))
    return None


def upload_image(file_path: str) -> str:
    """Загрузить файл на 999, вернуть image_id."""
    url = API_BASE + "/images"
    with open(file_path, "rb") as f:
        files = {"file": (Path(file_path).name, f, "image/jpeg")}
        r = requests.post(url, auth=_auth(), files=files, timeout=60)
    r.raise_for_status()
    data = r.json()
    image_id = data.get("image_id")
    if not image_id:
        raise ValueError("No image_id in response: " + str(data))
    return image_id


def upload_image_from_url(image_url: str) -> str:
    """Скачать по URL и загрузить на 999, вернуть image_id."""
    resp = requests.get(image_url, timeout=30)
    resp.raise_for_status()
    url = API_BASE + "/images"
    name = "image.jpg"
    if "?" in image_url:
        name = image_url.split("?")[0].split("/")[-1] or name
    else:
        name = image_url.split("/")[-1] or name
    files = {"file": (name, resp.content, "image/jpeg")}
    r = requests.post(url, auth=_auth(), files=files, timeout=60)
    r.raise_for_status()
    data = r.json()
    image_id = data.get("image_id")
    if not image_id:
        raise ValueError("No image_id in response: " + str(data))
    return image_id


def _download_image_content(image_url: str) -> Tuple[Optional[bytes], Optional[int]]:
    """
    Скачать картинку по URL. Если URL — Bitrix REST getFile и вернул JSON с result (ссылка) — качаем по ней.
    Возвращает (content, None) при успехе, (None, status_code) при 401/403, (None, None) при иной ошибке.
    """
    try:
        resp = requests.get(image_url, timeout=30, allow_redirects=True)
        if resp.status_code in (401, 403):
            return (None, resp.status_code)
        resp.raise_for_status()
        ct = (resp.headers.get("Content-Type") or "").lower()
        if "json" in ct:
            data = resp.json()
            url = None
            if isinstance(data, dict):
                url = data.get("result") or data.get("url") or (data.get("result", {}).get("url") if isinstance(data.get("result"), dict) else None)
            if isinstance(url, str) and url.startswith("http"):
                r2 = requests.get(url, timeout=30, allow_redirects=True)
                if r2.status_code in (401, 403):
                    return (None, r2.status_code)
                r2.raise_for_status()
                return (r2.content, None)
            return (None, None)
        return (resp.content, None)
    except Exception:
        return (None, None)


def upload_image_from_url_optional(image_url: str) -> Tuple[Optional[str], Optional[int]]:
    """Скачать по URL и загрузить на 999. При 401/403 пропустить фото. Возвращает (image_id или None, status_code при 401/403)."""
    content, failed_code = _download_image_content(image_url)
    if not content:
        if failed_code:
            print(f"WARN: skip photo (Bitrix {failed_code}): {image_url[:100]}...", file=sys.stderr, flush=True)
        else:
            print(f"WARN: skip photo (empty or error): {image_url[:80]}...", file=sys.stderr, flush=True)
        return (None, failed_code)
    name = "image.jpg"
    if "?" in image_url:
        name = image_url.split("?")[0].split("/")[-1] or name
    else:
        name = image_url.split("/")[-1] or name
    try:
        r = requests.post(API_BASE + "/images", auth=_auth(), files={"file": (name, content, "image/jpeg")}, timeout=60)
        r.raise_for_status()
        image_id = r.json().get("image_id")
        return (image_id if image_id else None, None)
    except Exception as e:
        print(f"WARN: skip photo (upload to 999 failed): {e}", file=sys.stderr, flush=True)
        return (None, None)


# --- Фото из БД (raw b24_sp_f_1114), как в auto_send_tg: url (ajax) -> machine URL с текущим вебхуком ---
def _build_machine_url_from_ajax(ajax_url: str, bitrix_webhook: str) -> Optional[str]:
    """Из ajax.php URL собираем REST URL crm.controller.item.getFile (скачивание без cookies, вебхук подставляем)."""
    if not (ajax_url and bitrix_webhook):
        return None
    try:
        parsed = urllib.parse.urlparse(ajax_url)
        qs = urllib.parse.parse_qs(parsed.query)
        params = {
            "entityTypeId": (qs.get("entityTypeId") or [""])[0],
            "id": (qs.get("id") or [""])[0],
            "fieldName": (qs.get("fieldName") or [""])[0],
            "fileId": (qs.get("fileId") or [""])[0],
        }
        if all(params.values()):
            return f"{bitrix_webhook.rstrip('/')}/crm.controller.item.getFile/?{urllib.parse.urlencode(params)}"
    except Exception:
        pass
    return None


def extract_photo_urls_from_raw(raw: Dict[str, Any], bitrix_webhook: str = "") -> List[str]:
    """
    Фото из raw[ufCrm34_1756897294]. Как в рабочем проекте:
    приоритет: url (ajax) → REST getFile по вебхуку или ajax, затем urlMachine.
    """
    arr = raw.get(PHOTO_RAW_KEY)
    if not arr:
        return []
    if isinstance(arr, str):
        try:
            arr = json.loads(arr)
        except Exception:
            arr = None
    if isinstance(arr, dict):
        arr = [arr]
    if not isinstance(arr, list):
        return []

    urls: List[str] = []
    for it in arr:
        if isinstance(it, dict):
            ajax_url = it.get("url")
            u = None
            if isinstance(ajax_url, str) and ajax_url.startswith("http"):
                u = _build_machine_url_from_ajax(ajax_url, bitrix_webhook) or ajax_url
            if not u:
                u = it.get("urlMachine")
            if isinstance(u, str) and u.startswith("http"):
                urls.append(u)
        elif isinstance(it, str) and it.startswith("http"):
            urls.append(it)

    seen = set()
    out: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# -----------------------------
# Raw decode (meta, iblock, enum) — как в auto_send_tg
# -----------------------------
@dataclass
class MetaInfo:
    b24_field: Optional[str]
    b24_type: str
    iblock_id: Optional[int]
    iblock_type_id: Optional[str]
    b24_title: Optional[str]
    enum_map: Dict[str, str]


def _raw_key_to_column_name(raw_key: str) -> str:
    return (raw_key[:2].lower() + raw_key[2:]) if len(raw_key) >= 2 else raw_key.lower()


def _extract_enum_map_from_settings(settings: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not settings:
        return out
    if isinstance(settings, str):
        try:
            settings = json.loads(settings)
        except Exception:
            return out
    if not isinstance(settings, dict):
        return out
    for k in ("items", "values", "enum", "list", "options"):
        lst = settings.get(k)
        if not isinstance(lst, list):
            continue
        for it in lst:
            if not isinstance(it, dict):
                continue
            _id = it.get("ID") or it.get("id") or it.get("VALUE_ID") or it.get("value_id")
            _val = it.get("VALUE") or it.get("value") or it.get("NAME") or it.get("name") or it.get("TITLE") or it.get("title")
            if _id is not None and _val is not None:
                out[str(_id)] = str(_val)
    return out


def _extract_title_from_labels(labels: Any) -> Optional[str]:
    if not labels:
        return None
    if isinstance(labels, str):
        try:
            labels = json.loads(labels)
        except Exception:
            return None
    if isinstance(labels, dict):
        for k in ("title", "formLabel", "listLabel", "filterLabel", "label", "name"):
            v = labels.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def _extract_enum_map_from_labels(labels: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not labels:
        return out
    if isinstance(labels, str):
        try:
            labels = json.loads(labels)
        except Exception:
            return out
    if isinstance(labels, dict) and any(k in labels for k in ("title", "formLabel", "listLabel", "filterLabel", "label", "name")):
        return out
    if isinstance(labels, dict):
        for k in ("items", "values", "enum", "list", "options"):
            v = labels.get(k)
            if isinstance(v, list):
                return _extract_enum_map_from_settings({"items": v})
    if isinstance(labels, list):
        for it in labels:
            if not isinstance(it, dict):
                continue
            _id = it.get("ID") or it.get("id") or it.get("VALUE_ID") or it.get("value_id")
            _val = it.get("VALUE") or it.get("value") or it.get("NAME") or it.get("name") or it.get("TITLE") or it.get("title")
            if _id is not None and _val is not None:
                out[str(_id)] = str(_val)
    if isinstance(labels, dict) and all(str(k).isdigit() for k in labels.keys()):
        for k, v in labels.items():
            if isinstance(v, (str, int, float)):
                out[str(k)] = str(v)
    return out


def load_meta_map() -> Dict[str, MetaInfo]:
    out: Dict[str, MetaInfo] = {}
    try:
        conn = _pg_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""SELECT b24_field, column_name, b24_type, settings, b24_title, b24_labels
                   FROM {META_TABLE} WHERE entity_key = %s""",
                (ENTITY_KEY_SP1114,),
            )
            rows = cur.fetchall()
        conn.close()
        if not rows:
            return out
        for r in rows:
            col = r.get("column_name")
            if not col:
                continue
            col_key = str(col).strip().lower()
            b24_field = r.get("b24_field")
            b24_type = (r.get("b24_type") or "")
            if isinstance(b24_type, bytes):
                b24_type = b24_type.decode("utf-8", errors="replace")
            settings = r.get("settings")
            iblock_id = None
            iblock_type_id = None
            if isinstance(settings, dict) and "IBLOCK_ID" in settings:
                try:
                    iblock_id = int(settings.get("IBLOCK_ID"))
                except Exception:
                    pass
            if isinstance(settings, dict):
                for k in ("IBLOCK_TYPE_ID", "IBLOCK_TYPE", "IBLOCK_TYPE_ID_NAME"):
                    v = settings.get(k)
                    if isinstance(v, str) and v.strip():
                        iblock_type_id = v.strip()
                        break
            enum_map = _extract_enum_map_from_settings(settings)
            labels_raw = r.get("b24_labels")
            if labels_raw:
                enum_map.update(_extract_enum_map_from_labels(labels_raw))
            b24_title = (r.get("b24_title") or "").strip()
            if isinstance(b24_title, bytes):
                b24_title = b24_title.decode("utf-8", errors="replace")
            if not b24_title and labels_raw:
                tt = _extract_title_from_labels(labels_raw)
                if tt:
                    b24_title = tt
            out[col_key] = MetaInfo(
                b24_field=str(b24_field).strip() if b24_field else None,
                b24_type=b24_type or "",
                iblock_id=iblock_id,
                iblock_type_id=iblock_type_id,
                b24_title=b24_title or None,
                enum_map=enum_map,
            )
    except Exception as e:
        print(f"WARN load_meta_map: {e}", file=sys.stderr, flush=True)
    return out


def _get_raw_key_by_title(meta: Dict[str, MetaInfo], titles: List[str]) -> Optional[str]:
    """Найти b24_field по одному из названий поля (b24_title)."""
    titles_lower = [t.strip().lower() for t in titles if t]
    for mi in (meta or {}).values():
        t = (mi.b24_title or "").strip().lower()
        if t and t in titles_lower:
            return mi.b24_field
    return None


def _ensure_iblock_cache_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {IBLOCK_CACHE_TABLE} (
                iblock_id INTEGER NOT NULL,
                element_id INTEGER NOT NULL,
                name TEXT,
                raw JSONB,
                updated_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (iblock_id, element_id)
            )"""
        )
    conn.commit()


def _load_iblock_cache(conn, pairs: List[Tuple[int, int]]) -> Dict[Tuple[int, int], str]:
    if not pairs:
        return {}
    try:
        ors = []
        params: List[Any] = []
        for ib, eid in pairs:
            ors.append("(iblock_id=%s AND element_id=%s)")
            params.extend([ib, eid])
        q = f"SELECT iblock_id, element_id, name FROM {IBLOCK_CACHE_TABLE} WHERE " + " OR ".join(ors)
        out: Dict[Tuple[int, int], str] = {}
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(q, params)
            for r in cur.fetchall():
                try:
                    out[(int(r["iblock_id"]), int(r["element_id"]))] = str(r["name"])
                except Exception:
                    pass
        return out
    except Exception:
        return {}


def _save_iblock_cache(conn, entries: Dict[Tuple[int, int], str]) -> None:
    if not entries:
        return
    try:
        _ensure_iblock_cache_table(conn)
        rows = [(ib, eid, name) for (ib, eid), name in entries.items()]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"""INSERT INTO {IBLOCK_CACHE_TABLE} (iblock_id, element_id, name)
                    VALUES %s ON CONFLICT (iblock_id, element_id) DO UPDATE SET name = EXCLUDED.name, updated_at = now()""",
                rows,
            )
        conn.commit()
    except Exception as e:
        print(f"WARN _save_iblock_cache: {e}", file=sys.stderr, flush=True)


def _b24_call_get(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    webhook = _bitrix_webhook()
    if not webhook:
        return {}
    url = f"{webhook.rstrip('/')}/{method}.json"
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"Bitrix error: {data.get('error')} {data.get('error_description')}")
    return data


def _extract_list_items(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    res = data.get("result")
    if isinstance(res, list):
        return [r for r in res if isinstance(r, dict)]
    if isinstance(res, dict):
        for key in ("items", "item", "elements", "element"):
            v = res.get(key)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
            if isinstance(v, dict):
                return [v]
    return []


def _fetch_iblock_names_from_bitrix(
    pairs: List[Tuple[int, int]],
    iblock_type_map: Dict[int, Optional[str]],
) -> Dict[Tuple[int, int], str]:
    out: Dict[Tuple[int, int], str] = {}
    webhook = _bitrix_webhook()
    if not pairs or not webhook or not ENABLE_IBLOCK_API_LOOKUP:
        return out
    by_ib: Dict[int, List[int]] = {}
    for ib, eid in pairs:
        by_ib.setdefault(ib, []).append(eid)
    for ib, eids in by_ib.items():
        type_candidates = [iblock_type_map.get(ib), IBLOCK_TYPE_ID_FALLBACK] + IBLOCK_TYPE_ID_CANDIDATES
        type_candidates = [t for t in type_candidates if t]
        seen = set()
        type_candidates = [t for t in type_candidates if t not in seen and not seen.add(t)]
        for eid in sorted(set(eids)):
            for iblock_type in type_candidates:
                try:
                    params = {
                        "IBLOCK_TYPE_ID": iblock_type,
                        "IBLOCK_ID": ib,
                        "FILTER[ID]": eid,
                    }
                    data = _b24_call_get("lists.element.get", params)
                    items = _extract_list_items(data)
                    if items:
                        item = items[0]
                        name = item.get("NAME") or item.get("TITLE") or item.get("name") or item.get("title")
                        if name:
                            out[(ib, eid)] = str(name)
                            break
                except Exception:
                    continue
    return out


def resolve_iblock_names(
    pairs: List[Tuple[int, int]],
    iblock_type_map: Optional[Dict[int, Optional[str]]] = None,
) -> Dict[Tuple[int, int], str]:
    if not pairs:
        return {}
    iblock_type_map = iblock_type_map or {}
    try:
        conn = _pg_conn()
        _ensure_iblock_cache_table(conn)
        out = _load_iblock_cache(conn, pairs)
        missing = [p for p in pairs if p not in out]
        if missing and ENABLE_IBLOCK_API_LOOKUP:
            api_map = _fetch_iblock_names_from_bitrix(missing, iblock_type_map)
            if api_map:
                _save_iblock_cache(conn, api_map)
                out.update(api_map)
        conn.close()
        return out
    except Exception as e:
        print(f"WARN resolve_iblock_names: {e}", file=sys.stderr, flush=True)
        return {}


def _ensure_enum_cache_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {ENUM_CACHE_TABLE} (
                entity_type_id INTEGER NOT NULL,
                b24_field TEXT NOT NULL,
                enum_map JSONB NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (entity_type_id, b24_field)
            )"""
        )
    conn.commit()


def load_enum_cache() -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    try:
        conn = _pg_conn()
        _ensure_enum_cache_table(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT b24_field, enum_map FROM {ENUM_CACHE_TABLE} WHERE entity_type_id = %s",
                (SMART_ENTITY_TYPE_ID,),
            )
            for r in cur.fetchall():
                b24_field = str(r.get("b24_field") or "").strip()
                enum_map = r.get("enum_map") or {}
                if b24_field:
                    out[b24_field] = {str(k): str(v) for k, v in enum_map.items()}
        conn.close()
    except Exception as e:
        print(f"WARN load_enum_cache: {e}", file=sys.stderr, flush=True)
    return out


def _decode_enum_value(val: Any, enum_map: Dict[str, str]) -> str:
    if val is None:
        return ""
    if isinstance(val, list):
        parts = [_decode_enum_value(v, enum_map) for v in val]
        return ", ".join(p for p in parts if p)
    if isinstance(val, dict):
        for k in ("id", "ID", "value", "VALUE"):
            if k in val:
                return _decode_enum_value(val.get(k), enum_map)
        return str(val)
    s = str(val).strip()
    if not s or not enum_map:
        return s
    if s in enum_map:
        return enum_map[s]
    if "|" in s:
        parts = [p.strip() for p in s.split("|") if p.strip()]
        return ", ".join(enum_map.get(p, p) for p in parts)
    return s


def decode_value_for_raw_key(
    raw_key: str,
    raw_value: Any,
    meta: Dict[str, MetaInfo],
    iblock_names: Dict[Tuple[int, int], str],
    enum_cache: Dict[str, Dict[str, str]],
) -> str:
    txt = str(raw_value).strip() if raw_value is not None else ""
    if isinstance(raw_value, dict):
        txt = str(raw_value.get("value") or raw_value.get("id") or raw_value.get("title") or raw_value.get("name") or "")
    if isinstance(raw_value, list) and raw_value:
        return decode_value_for_raw_key(raw_key, raw_value[0], meta, iblock_names, enum_cache)
    if not txt:
        return ""
    col = _raw_key_to_column_name(raw_key)
    mi = meta.get(col.lower())
    if not mi:
        return txt
    if mi.b24_type == "iblock_element" and mi.iblock_id:
        t = txt[:-2] if txt.endswith(".0") else txt
        try:
            eid = int(float(t))
        except Exception:
            return txt
        return iblock_names.get((mi.iblock_id, eid), txt)
    enum_map = mi.enum_map or {}
    if not enum_map and mi.b24_field and mi.b24_field in enum_cache:
        enum_map = enum_cache[mi.b24_field]
    if enum_map:
        return _decode_enum_value(raw_value, enum_map) or txt
    return txt


def _raw_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, dict):
        return str(v.get("value") or v.get("id") or v.get("title") or v.get("name") or "")
    if isinstance(v, list) and v:
        return _raw_str(v[0])
    return str(v).strip()


def fetch_raw_by_item_id(item_id: int) -> Optional[Dict[str, Any]]:
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT raw FROM {DATA_TABLE_SP1114} WHERE (raw->>'id')::bigint = %s LIMIT 1",
                (int(item_id),),
            )
            row = cur.fetchone()
        conn.close()
        if row and row[0]:
            return row[0] if isinstance(row[0], dict) else None
    except Exception as e:
        print(f"ERROR fetch_raw_by_item_id: {e}", file=sys.stderr, flush=True)
    return None


def _is_filled_value(v: Any) -> bool:
    """Поле считается заполненным, если не пустое (как в auto_send_tg)."""
    if v is None:
        return False
    if isinstance(v, str):
        return bool(v.strip())
    if isinstance(v, (int, float)):
        return True
    if isinstance(v, list):
        return any(_is_filled_value(x) for x in v)
    if isinstance(v, dict):
        return any(_is_filled_value(x) for x in v.values())
    return bool(v)


def should_send_like_tg(raw: Dict[str, Any]) -> bool:
    """1-в-1 как should_send(raw) в auto_send_tg: все REQUIRE_ALL_FILLED_FIELDS заполнены."""
    for field in REQUIRE_ALL_FILLED_FIELDS:
        if not _is_filled_value(raw.get(field)):
            return False
    return True


def _ensure_999_sent_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SENT_999_TABLE} (
                item_id BIGINT PRIMARY KEY,
                sent_at TIMESTAMPTZ DEFAULT now()
            )
            """
        )
    conn.commit()


def _was_sent_to_999(conn, item_id: int) -> bool:
    _ensure_999_sent_table(conn)
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {SENT_999_TABLE} WHERE item_id = %s", (int(item_id),))
        return cur.fetchone() is not None


def _was_sent_to_tg(conn, item_id: int) -> bool:
    """Проверка: машина уже отправлена в TG_AUTO (есть в tg_sent_items). На 999 шлём только такие."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {SENT_TG_TABLE} WHERE item_id = %s", (int(item_id),))
            return cur.fetchone() is not None
    except Exception:
        return False


def _mark_sent_to_999(conn, item_id: int) -> None:
    _ensure_999_sent_table(conn)
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {SENT_999_TABLE} (item_id) VALUES (%s) ON CONFLICT (item_id) DO NOTHING",
            (int(item_id),),
        )
    conn.commit()


def fetch_next_raw_for_999() -> Optional[Dict[str, Any]]:
    """Следующая машина для 999. Только те, что уже ушли в TG (tg_sent_items), затем — ещё не на 999."""
    conn = None
    try:
        conn = _pg_conn()
        _ensure_999_sent_table(conn)
        created_expr = "COALESCE(NULLIF(t.raw->>'createdTime','')::timestamptz, NULLIF(t.raw->>'createdate','')::timestamptz)"
        scalar_filters = [f"COALESCE(t.raw->>'{k}', '') <> ''" for k in REQUIRE_ALL_FILLED_SCALAR_FIELDS]
        scalar_sql = " AND ".join(scalar_filters) if scalar_filters else "TRUE"
        params = (CATEGORY_ID_SP1114, PHOTO_RAW_KEY, PHOTO_RAW_KEY, PHOTO_RAW_KEY, MIN_PHOTOS_999, PHOTO_RAW_KEY, MAX_PHOTOS_999)
        for order_by in ("tg.sent_at DESC NULLS LAST", f"{created_expr} DESC NULLS LAST"):
            try:
                q = f"""
                    SELECT t.raw
                    FROM {DATA_TABLE_SP1114} t
                    INNER JOIN {SENT_TG_TABLE} tg ON tg.item_id = NULLIF(t.raw->>'id','')::bigint
                    LEFT JOIN {SENT_999_TABLE} s ON s.item_id = NULLIF(t.raw->>'id','')::bigint
                    WHERE s.item_id IS NULL
                      AND t.raw ? 'id'
                      AND COALESCE(t.categoryid::text, t.raw->>'categoryId') = %s
                      AND t.raw ? %s
                      AND jsonb_typeof(t.raw->%s) = 'array'
                      AND jsonb_array_length(t.raw->%s) >= %s
                      AND jsonb_array_length(t.raw->%s) <= %s
                      AND {created_expr} >= (now() AT TIME ZONE 'UTC') - interval '14 days'
                      AND {scalar_sql}
                    ORDER BY {order_by}
                    LIMIT 1
                """
                with conn.cursor() as cur:
                    cur.execute(q, params)
                    row = cur.fetchone()
                if row and row[0]:
                    return row[0] if isinstance(row[0], dict) else None
                return None
            except Exception as e:
                if "sent_at" in str(e).lower() or "column" in str(e).lower():
                    print(f"WARN fetch_next_raw_for_999: {e}, пробую без sent_at", file=sys.stderr, flush=True)
                    continue
                raise
    except Exception as e:
        print(f"ERROR fetch_next_raw_for_999: {e}", file=sys.stderr, flush=True)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    return None


def fetch_random_raw_for_999() -> Optional[Dict[str, Any]]:
    """Случайная машина для 999 — только из тех, что уже в tg_sent_items."""
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT t.raw
                FROM {DATA_TABLE_SP1114} t
                INNER JOIN {SENT_TG_TABLE} tg ON tg.item_id = NULLIF(t.raw->>'id','')::bigint
                WHERE COALESCE(t.categoryid::text, t.raw->>'categoryId') = %s
                  AND t.raw ? %s
                  AND jsonb_typeof(t.raw->%s) = 'array'
                  AND jsonb_array_length(t.raw->%s) > 0
                ORDER BY random()
                LIMIT 1
                """,
                (CATEGORY_ID_SP1114, PHOTO_RAW_KEY, PHOTO_RAW_KEY, PHOTO_RAW_KEY),
            )
            row = cur.fetchone()
        conn.close()
        if row and row[0]:
            return row[0] if isinstance(row[0], dict) else None
    except Exception as e:
        print(f"ERROR fetch_random_raw_for_999: {e}", file=sys.stderr, flush=True)
    return None


def get_item_id_from_raw(raw: Dict[str, Any]) -> Optional[int]:
    for k in ("id", "ID"):
        v = raw.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except Exception:
            pass
    return None


def car_data_from_raw(raw: Dict[str, Any]) -> Dict[str, Any]:
    bitrix_webhook = _bitrix_webhook()
    photo_urls = extract_photo_urls_from_raw(raw, bitrix_webhook)

    meta = load_meta_map()
    enum_cache = load_enum_cache()

    raw_fields_for_iblock = [RAW_FIELDS_MARCA, RAW_FIELDS_MODEL, RAW_FIELDS_YEAR, RAW_FIELDS_BODY, RAW_FIELDS_ENGINE, RAW_FIELDS_FUEL, RAW_FIELDS_DRIVE, RAW_FIELDS_TRANSMISSION]
    pairs: List[Tuple[int, int]] = []
    iblock_type_map: Dict[int, Optional[str]] = {}
    for rk in raw_fields_for_iblock:
        col = _raw_key_to_column_name(rk).lower()
        mi = meta.get(col)
        if not mi or mi.b24_type != "iblock_element" or not mi.iblock_id:
            continue
        v = raw.get(rk)
        t = _raw_str(v).strip()
        if not t:
            continue
        t = t[:-2] if t.endswith(".0") else t
        try:
            eid = int(float(t))
        except Exception:
            continue
        pairs.append((mi.iblock_id, eid))
        iblock_type_map[mi.iblock_id] = mi.iblock_type_id

    iblock_names = resolve_iblock_names(pairs, iblock_type_map) if pairs else {}

    def get(rk: str) -> str:
        return decode_value_for_raw_key(rk, raw.get(rk), meta, iblock_names, enum_cache)

    def _parse_year_from_value(v: Any) -> Optional[int]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            y = int(v)
            return y if 1900 <= y <= 2030 else None
        if isinstance(v, dict):
            n = v.get("value") or v.get("id") or v.get("VALUE") or v.get("ID")
            if n is not None:
                return _parse_year_from_value(n)
            return None
        s = re.sub(r"[^0-9]", "", str(v))[:4]
        if not s:
            return None
        y = int(s)
        return y if 1900 <= y <= 2030 else None

    # Год — одно поле ufCrm34_1748347979. В raw может быть число-год (2019) или id опции списка (56 → расшифровать через enum).
    year_val = None
    skip_keys = {RAW_FIELDS_PRICE, RAW_FIELDS_MILEAGE, RAW_FIELDS_LINK, RAW_FIELDS_MARCA, RAW_FIELDS_MODEL, PHOTO_RAW_KEY}
    try:
        raw_year = raw.get(RAW_FIELDS_YEAR)
        year_val = _parse_year_from_value(raw_year)
        if year_val is None and raw_year is not None:
            decoded = get(RAW_FIELDS_YEAR)
            year_val = _parse_year_from_value(decoded)
        if year_val is None:
            year_key = _get_raw_key_by_title(meta, ["Anul producerii", "Год выпуска", "Year"])
            if year_key:
                year_val = _parse_year_from_value(raw.get(year_key))
                if year_val is None:
                    year_val = _parse_year_from_value(get(year_key))
        if year_val is None:
            for col_key, mi in (meta or {}).items():
                t = (mi.b24_title or "").strip().lower()
                if not t or ("anul" not in t and "год" not in t and "year" not in t and "producerii" not in t and "выпуска" not in t):
                    continue
                rk = mi.b24_field
                if rk:
                    year_val = _parse_year_from_value(raw.get(rk))
                    if year_val is None:
                        year_val = _parse_year_from_value(get(rk))
                    if year_val is not None:
                        break
        if year_val is None:
            for rk, rv in raw.items():
                if rk in skip_keys or rv is None:
                    continue
                year_val = _parse_year_from_value(rv)
                if year_val is not None:
                    break
    except Exception:
        pass

    price_val = None
    try:
        raw_price = raw.get(RAW_FIELDS_PRICE)
        if raw_price is not None:
            if isinstance(raw_price, (int, float)):
                price_val = float(raw_price)
            elif isinstance(raw_price, dict):
                pv = raw_price.get("value") or raw_price.get("id") or raw_price.get("VALUE") or raw_price.get("ID")
                if pv is not None:
                    price_val = float(pv)
            if price_val is None or price_val < 0:
                p = get(RAW_FIELDS_PRICE)
                if p:
                    price_val = float(re.sub(r"[^0-9.,]", "", str(p)).replace(",", "."))
    except Exception:
        pass

    mileage_val = None
    try:
        raw_mileage = raw.get(RAW_FIELDS_MILEAGE)
        if raw_mileage is not None:
            if isinstance(raw_mileage, (int, float)):
                mileage_val = int(raw_mileage)
            elif isinstance(raw_mileage, dict):
                mv = raw_mileage.get("value") or raw_mileage.get("id") or raw_mileage.get("VALUE") or raw_mileage.get("ID")
                if mv is not None:
                    mileage_val = int(float(mv))
            if mileage_val is None or mileage_val < 0:
                m = get(RAW_FIELDS_MILEAGE)
                if m:
                    mileage_val = int(re.sub(r"[^0-9]", "", str(m)))
    except Exception:
        pass

    link = get(RAW_FIELDS_LINK)
    description = (raw.get("title") or "").strip() or ""
    if link and link.startswith("http"):
        description = (description + "\n\n" + link).strip()

    marca = (get(RAW_FIELDS_MARCA) or "").strip()
    model = (get(RAW_FIELDS_MODEL) or "").strip()
    # Номер авто из Bitrix — нигде не показываем (ни в заголовке, ни в описании)
    numar_key = _get_raw_key_by_title(meta, ["Numar Auto", "Номер авто", "Car number", "Numar auto"])
    numar_auto = (get(numar_key) or "").strip() if numar_key else ""
    # Убрать номер из описания
    description = _strip_numar_from_description(description, numar_auto) or description
    # Убрать номер из марки и модели
    marca = _strip_numar_from_title(marca) or marca
    model = _strip_numar_from_title(model) or model

    # Одно название объявления: либо Bitrix title, либо марка+модель (никогда не смешиваем)
    raw_title = (raw.get("title") or "").strip()
    raw_title = _strip_numar_from_title(raw_title) or raw_title
    if raw_title:
        listing_title = raw_title
    else:
        listing_title = f"{marca} {model}".strip()
        listing_title = _strip_numar_from_title(listing_title) or listing_title
    listing_title = re.sub(r"\s+", " ", (listing_title or "").strip()) or f"{marca} {model}".strip()

    # Кузов, топливо, объём двигателя, привод, тип коробки из Bitrix (по названию поля)
    body_type_option_id: Optional[str] = None
    fuel_option_id: Optional[str] = None
    engine_option_id: Optional[str] = None
    drive_option_id: Optional[str] = None
    transmission_option_id: Optional[str] = None
    body_key = _get_raw_key_by_title(meta, ["Caroserie", "Тип кузова", "Body type"])
    fuel_key = _get_raw_key_by_title(meta, ["Tipul de combustibil", "Тип топлива", "Fuel type"])
    engine_key = _get_raw_key_by_title(meta, ["Volumul motorului", "Объем двигателя", "Engine volume"])
    drive_key = _get_raw_key_by_title(meta, ["Tracţiune", "Tracțiune", "Привод", "Drive"])
    transmission_key = _get_raw_key_by_title(meta, ["Transmisie", "Cutie", "Кпп", "КПП", "Transmission", "Gearbox"])
    body_str = (get(body_key) or "").strip() if body_key else ""
    fuel_str = (get(fuel_key) or "").strip() if fuel_key else ""
    engine_str = (get(engine_key) or "").strip().replace(",", ".") if engine_key else ""
    drive_str = (get(drive_key) or "").strip() if drive_key else ""
    transmission_str = (get(transmission_key) or "").strip() if transmission_key else ""

    def _resolve_option(fid: str, val: str) -> Optional[str]:
        """Сначала явный маппинг BITRIX_TO_999_OPTION, потом _find_feature_option."""
        v = (val or "").strip().lower()
        if v and fid in BITRIX_TO_999_OPTION:
            opt = BITRIX_TO_999_OPTION[fid].get(v)
            if opt:
                return opt
            for k, opt_id in BITRIX_TO_999_OPTION[fid].items():
                if k in v or v in k:
                    return opt_id
        return _find_feature_option(features_data, fid, val)

    features_data = _load_features_json()
    if body_str:
        body_type_option_id = _resolve_option("102", body_str)
        if not body_type_option_id:
            for alias in ("Universal", "Универсал", "Wagon", "Sedan", "Седан"):
                body_type_option_id = _find_feature_option(features_data, "102", alias)
                if body_type_option_id:
                    break
    if fuel_str:
        fuel_option_id = _resolve_option("151", fuel_str)
        if not fuel_option_id and ("motorina" in fuel_str.lower() or "diesel" in fuel_str.lower() or "дизель" in fuel_str.lower()):
            fuel_option_id = BITRIX_TO_999_OPTION.get("151", {}).get("diesel") or _find_feature_option(features_data, "151", "Дизель")
        if not fuel_option_id and ("benzin" in fuel_str.lower() or "gasoline" in fuel_str.lower() or "бензин" in fuel_str.lower()):
            fuel_option_id = BITRIX_TO_999_OPTION.get("151", {}).get("бензин") or _find_feature_option(features_data, "151", "Бензин")
    if engine_str:
        engine_option_id = _resolve_option("2553", engine_str) or _resolve_option("2553", engine_str.replace(".", ","))
        if not engine_option_id:
            for variant in (engine_str, engine_str.rstrip("0").rstrip("."), engine_str.split(".")[0]):
                if variant:
                    engine_option_id = _find_feature_option(features_data, "2553", variant)
                    if engine_option_id:
                        break
    if drive_str:
        drive_option_id = _resolve_option("108", drive_str)
        if not drive_option_id:
            ds = drive_str.lower()
            if "spate" in ds or "rear" in ds or "зад" in ds or "задний" in ds:
                drive_option_id = BITRIX_TO_999_OPTION.get("108", {}).get("rear") or "25"
            elif "fata" in ds or "faţa" in ds or "front" in ds or "перед" in ds or "передний" in ds:
                drive_option_id = BITRIX_TO_999_OPTION.get("108", {}).get("front") or "5"
            elif "4x4" in ds or "полный" in ds or "full" in ds or "4х4" in ds:
                drive_option_id = BITRIX_TO_999_OPTION.get("108", {}).get("4x4") or "17"
    if transmission_str:
        transmission_option_id = _resolve_option("101", transmission_str)

    return {
        "marca": marca,
        "model": model,
        "listing_title": listing_title,
        "year": year_val or 2020,
        "price": price_val or 0,
        "price_unit": "eur",
        "mileage_km": mileage_val,
        "description": description,
        "numar_auto": numar_auto,
        "phone": os.getenv("PUBLISH_999MD_PHONE", "").strip(),
        "image_urls": photo_urls,
        "body_type_option_id": body_type_option_id,
        "fuel_option_id": fuel_option_id,
        "engine_option_id": engine_option_id,
        "drive_option_id": drive_option_id,
        "transmission_option_id": transmission_option_id,
    }


def _strip_numar_from_description(text: str, numar_auto: str = "") -> str:
    """Убрать номер машины из текста (описание): явно numar_auto и любые слова «буквы+цифры» / «цифры+буквы»."""
    if not text or not isinstance(text, str):
        return (text or "").strip()
    s = text.strip()
    if numar_auto:
        s = re.sub(re.escape(numar_auto), "", s, flags=re.IGNORECASE)
    # Удалить любые слова-номера (буквы+цифры или цифры+буквы) в любом месте текста
    s = re.sub(r"\b[A-Za-zА-Яа-я]+\d+\b", "", s)
    s = re.sub(r"\b\d+[A-Za-zА-Яа-я]+\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _strip_numar_from_title(s: str) -> str:
    """Убрать из названия номер машины любого формата и длины (VEL127, MDQ086, DDK269, ABC 123 и т.д.)."""
    if not s or not isinstance(s, str):
        return (s or "").strip()
    s = s.strip()
    # Повторяем, пока остаётся что-то похожее на номер (буквы+цифры или цифры+буквы)
    while True:
        prev = s
        # В конце: пробел + буквы + опционально пробел + цифры (в т.ч. с ведущим 0: 086)
        s = re.sub(r"[\s\u00a0]+[A-Za-zА-Яа-я]+\s*\d+\s*$", "", s)
        # В конце: пробел + цифры + опционально пробел + буквы
        s = re.sub(r"[\s\u00a0]+\d+\s*[A-Za-zА-Яа-я]+\s*$", "", s)
        # В конце без пробела: буквы + цифры (MDQ086, VEL127)
        s = re.sub(r"[A-Za-zА-Яа-я]+\d+\s*$", "", s)
        # В конце: цифры + буквы (123ABC)
        s = re.sub(r"\d+[A-Za-zА-Яа-я]+\s*$", "", s)
        # В начале: номер в начале строки (на случай "MDQ086 Dacia Duster")
        s = re.sub(r"^[A-Za-zА-Яа-я]+\s*\d+[\s\u00a0]+", "", s)
        s = re.sub(r"^\d+\s*[A-Za-zА-Яа-я]+[\s\u00a0]+", "", s)
        s = s.strip()
        if s == prev:
            break
    return s


def build_advert_payload(
    marca: str,
    model: str,
    year: int,
    price: float,
    price_unit: str = "eur",
    mileage_km: Optional[int] = None,
    description: str = "",
    numar_auto: str = "",
    phone: str = "",
    image_ids: Optional[List[str]] = None,
    region_option_id: Optional[str] = None,
    listing_title: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    features_data = _load_features_json()
    brand_id = resolve_brand_option_id(marca, features_data)
    if not brand_id:
        raise ValueError(f"Марка не найдена на 999: '{marca}'")
    model_id = resolve_model_option_id(brand_id, model)
    if not model_id:
        raise ValueError(f"Модель не найдена на 999 для марки '{marca}': '{model}'")
    generation_id = resolve_generation_option_id(model_id)

    features: List[Dict[str, Any]] = []

    def add(fid: str, value: Any, unit: Optional[str] = None) -> None:
        entry = {"id": fid, "value": value}
        if unit:
            entry["unit"] = unit
        features.append(entry)

    add("20", brand_id)
    add("21", model_id)
    if generation_id:
        add("2095", generation_id)
    # Одно название объявления: переданное listing_title ИЛИ марка+модель (никогда не смешиваем)
    if listing_title and str(listing_title).strip():
        title = str(listing_title).strip()
    else:
        title = f"{marca} {model}".strip()
    title = _strip_numar_from_title(title) or title
    title = re.sub(r"\s+[A-Za-zА-Яа-я]+\d+(?=\s|$)", "", title)
    title = re.sub(r"\s+\d+[A-Za-zА-Яа-я]+(?=\s|$)", "", title)
    if numar_auto and (numar_auto in title or numar_auto.upper() in title.upper()):
        title = re.sub(re.escape(numar_auto), "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s+", " ", title).strip() or f"{marca} {model}".strip()
    add("12", title)
    add("775", kwargs.get("registration_option_id") or DEFAULTS_PAYLOAD_999["775"])
    add("593", kwargs.get("state_option_id") or DEFAULTS_PAYLOAD_999["593"])
    add("1761", kwargs.get("availability_option_id") or DEFAULTS_PAYLOAD_999["1761"])
    add("1763", kwargs.get("origin_option_id") or DEFAULTS_PAYLOAD_999["1763"])
    add("795", kwargs.get("author_option_id") or DEFAULTS_PAYLOAD_999["795"])
    year_ok = max(YEAR_MIN_999, min(YEAR_MAX_999, int(year)))
    add("19", year_ok)
    add("1196", kwargs.get("steering_option_id") or DEFAULTS_PAYLOAD_999["1196"])
    add("846", kwargs.get("seats_option_id") or DEFAULTS_PAYLOAD_999["846"])
    add("102", kwargs.get("body_type_option_id") or DEFAULTS_PAYLOAD_999["102"])
    add("104", mileage_km if mileage_km is not None else 0, "km")
    add("2553", kwargs.get("engine_option_id") or DEFAULTS_PAYLOAD_999["2553"])
    add("151", kwargs.get("fuel_option_id") or DEFAULTS_PAYLOAD_999["151"])
    add("108", kwargs.get("drive_option_id") or DEFAULTS_PAYLOAD_999["108"])
    add("101", kwargs.get("transmission_option_id") or DEFAULTS_PAYLOAD_999["101"])
    add("7", region_option_id or DEFAULTS_PAYLOAD_999["7"])
    add("2", int(price), price_unit if price_unit in ("eur", "usd", "mdl") else "eur")

    body_text = _strip_numar_from_description(description or "", numar_auto) or ""
    # Номер машины нигде не показываем — ни в заголовке, ни в описании
    if body_text:
        add("13", body_text)

    if image_ids:
        add("14", image_ids)
    else:
        raise ValueError("999.md требует хотя бы одно фото (feature 14). Укажите image_urls или image_paths.")

    if phone:
        normalized = re.sub(r"[^\d+]", "", phone)
        if normalized.startswith("0"):
            normalized = "373" + normalized[1:]
        elif not normalized.startswith("373"):
            normalized = "373" + normalized
        add("16", [normalized])

    # Скрытое: access_policy "private" — объявление в «Скрытые» на 999.
    return {
        "category_id": CATEGORY_ID,
        "subcategory_id": SUBCATEGORY_ID,
        "offer_type": OFFER_TYPE,
        "features": features,
        "access_policy": "private",
    }


def _save_draft_payload(payload: Dict[str, Any], item_id: Optional[int] = None) -> str:
    DRAFTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"draft_{item_id}_{stamp}.json" if item_id else f"draft_{stamp}.json"
    path = DRAFTS_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return str(path)


def _payload_without_phone(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Копия payload без feature 16 (телефон). Возможно 999 сохранит как черновик «неполное» объявление."""
    out = dict(payload)
    feats = out.get("features")
    if isinstance(feats, list):
        out["features"] = [f for f in feats if str(f.get("id")) != "16"]
    return out


def post_advert(payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST /adverts — создание объявления (access_policy в payload, после создания ставим private)."""
    return _post_json("/adverts", payload)


def patch_advert_state(advert_id: str, state: str) -> Optional[Dict[str, Any]]:
    """PATCH /adverts/{id} с state — попытка перевести объявление в черновик сразу после создания."""
    try:
        return _patch_json(f"/adverts/{advert_id}", {"state": state})
    except Exception as e:
        print(f"WARN: patch_advert_state({advert_id}, {state}): {e}", file=sys.stderr, flush=True)
        return None


def set_advert_access_policy(advert_id: str, access_policy: str = "private") -> Dict[str, Any]:
    url = API_BASE + f"/adverts/{advert_id}/access_policy"
    r = requests.put(url, auth=_auth(), json={"access_policy": access_policy}, timeout=30)
    if not r.ok:
        try:
            err_body = r.json()
        except Exception:
            err_body = r.text[:500]
        raise RuntimeError(f"999.md API access_policy {r.status_code}: {err_body}")
    return r.json()


def send_telegram_notification_999(
    advert_id: Optional[str] = None,
    marca: Optional[str] = None,
    model: Optional[str] = None,
    numar_auto: Optional[str] = None,
    category_id: Optional[str] = None,
    photo_url: Optional[str] = None,
) -> None:
    """Отправить в Telegram уведомление по шаблону: воронка, марка/модель/номер, ссылки, и одна реальная фотка машины."""
    if not TG_BOT_TOKEN_999 or not TG_CHAT_ID_999:
        return
    funnel_name = FUNNEL_NAMES.get(str(category_id or "").strip(), "Авто") if category_id else "Авто"
    title_line = " ".join(x for x in [marca or "", model or "", numar_auto or ""] if x).strip() or "—"
    lines = [
        f"Создано новое объявление Авто {funnel_name}",
        title_line,
        "",
    ]
    if advert_id:
        link_ru = f"https://999.md/ru/{advert_id}"
        link_edit_ro = f"https://999.md/ro/{advert_id}/edit?offer_type=776"
        lines.append(f'🔗 <a href="{link_ru}">Открыть объявление</a>')
        lines.append("")
        lines.append("Зайди, проверь и опулбликуй объявление —")
        lines.append(f'🛠️ <a href="{link_edit_ro}">Редактировать объявление</a>')
    text = "\n".join(lines)
    base_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN_999}"
    try:
        r = requests.post(
            f"{base_url}/sendMessage",
            json={
                "chat_id": TG_CHAT_ID_999,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if not r.ok:
            print(f"WARN: Telegram уведомление 999: {r.status_code} {r.text[:200]}", file=sys.stderr, flush=True)
        elif photo_url and photo_url.strip().startswith("http"):
            try:
                img_resp = requests.get(photo_url.strip(), timeout=15)
                if img_resp.ok and img_resp.content:
                    ct = img_resp.headers.get("Content-Type") or "image/jpeg"
                    ext = ".jpg" if "jpeg" in ct or "jpg" in ct else ".png" if "png" in ct else ".webp" if "webp" in ct else ".jpg"
                    r2 = requests.post(
                        f"{base_url}/sendPhoto",
                        data={"chat_id": TG_CHAT_ID_999},
                        files={"photo": (f"photo{ext}", img_resp.content, ct)},
                        timeout=20,
                    )
                    if not r2.ok:
                        print(f"WARN: Telegram sendPhoto 999: {r2.status_code} {r2.text[:200]}", file=sys.stderr, flush=True)
                else:
                    print(f"WARN: не удалось скачать фото для Telegram: {img_resp.status_code}", file=sys.stderr, flush=True)
            except Exception as e2:
                print(f"WARN: Telegram фото 999 (скачать/отправить): {e2}", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"WARN: Telegram уведомление 999: {e}", file=sys.stderr, flush=True)


def get_adverts(page: int = 1, page_size: int = 30, states: Optional[str] = None, lang: str = "ru") -> Dict[str, Any]:
    params: Dict[str, Any] = {"lang": lang, "page": page, "page_size": page_size}
    if states:
        params["states"] = states
    return _get("/adverts", params)


def get_advert(advert_id: str, lang: str = "ru") -> Dict[str, Any]:
    return _get(f"/adverts/{advert_id}", {"lang": lang})


def get_advert_features(advert_id: str, lang: str = "ru") -> Dict[str, Any]:
    return _get(f"/adverts/{advert_id}/features", {"lang": lang})


# --- FastAPI router ---
router = APIRouter(prefix="/api/publish-999md", tags=["publish-999md"])


class PublishCarBody(BaseModel):
    item_id: Optional[int] = Field(None, description="ID из b24_sp_f_1114 — данные и фото из БД (raw)")
    marca: Optional[str] = Field(None, description="Марка (если без item_id)")
    model: Optional[str] = Field(None, description="Модель (если без item_id)")
    year: Optional[int] = Field(None, ge=1900, le=2030)
    price: Optional[float] = Field(None, gt=0)
    price_unit: str = Field("eur", description="eur, usd, mdl")
    mileage_km: Optional[int] = Field(None, ge=0)
    description: str = Field("", description="Описание")
    numar_auto: str = Field("", description="Номер авто")
    phone: str = Field("", description="Телефон 373...")
    image_urls: Optional[List[str]] = Field(None, description="URL фото или из БД по item_id")
    image_paths: Optional[List[str]] = Field(None, description="Локальные пути к фото")
    region_option_id: Optional[str] = Field(None, description="ID региона на 999")


@router.get("/publish-random")
def api_publish_random() -> Dict[str, Any]:
    """Взять случайную машину из БД и закинуть объявление на 999 (черновик). Вызвать в браузере: GET /api/publish-999md/publish-random"""
    if not _token():
        raise HTTPException(status_code=503, detail="999.md token not set")
    try:
        result = publish_random_car_to_999()
        if result is None:
            raise HTTPException(status_code=404, detail="Нет подходящей машины в БД (категория 111, с фото)")
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"ERROR api_publish_random: {e}", file=sys.stderr, flush=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/adverts-debug")
def api_get_adverts_debug(
    page: int = 1,
    page_size: int = 20,
    states: Optional[str] = None,
    lang: str = "ru",
) -> Dict[str, Any]:
    """Список объявлений 999.md (сырой ответ API). Посмотри state у черновика. Пример: ?states=draft или без states."""
    if not _token():
        raise HTTPException(status_code=503, detail="999.md token not set")
    return get_adverts(page=page, page_size=page_size, states=states, lang=lang)


@router.get("/adverts")
def api_get_adverts(
    page: int = 1,
    page_size: int = 30,
    states: Optional[str] = None,
    lang: str = "ru",
) -> Dict[str, Any]:
    if not _token():
        raise HTTPException(status_code=503, detail="999.md token not set")
    return get_adverts(page=page, page_size=page_size, states=states, lang=lang)


@router.get("/adverts/{advert_id}/features")
def api_get_advert_features(advert_id: str, lang: str = "ru") -> Dict[str, Any]:
    if not _token():
        raise HTTPException(status_code=503, detail="999.md token not set")
    try:
        return get_advert_features(advert_id, lang=lang)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Объявление не найдено")
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/features-from-999")
def api_get_features_from_999(lang: str = "ru") -> Dict[str, Any]:
    """Получить все поля (features) подкатегории «Легковые автомобили» с 999.md API.
    Сырой JSON — видно id фич, title, options (id, title). Для читаемой таблицы: GET /features-from-999/map"""
    if not _token():
        raise HTTPException(status_code=503, detail="999.md token not set")
    try:
        return get_features_from_api(lang=lang)
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=str(e))


def _features_to_map_list(features_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Преобразовать ответ /features в список: feature_id, title, type, options [{id, title}]."""
    out: List[Dict[str, Any]] = []
    for grp in features_data.get("features_groups", []):
        for feat in grp.get("features", []):
            fid = feat.get("id")
            title = feat.get("title") or ""
            ftype = feat.get("type") or ""
            opts = []
            for o in (feat.get("options") or []):
                opts.append({"id": o.get("id"), "title": o.get("title") or ""})
            out.append({"feature_id": fid, "title": title, "type": ftype, "options": opts})
    return out


@router.get("/features-from-999/map")
def api_get_features_from_999_map(lang: str = "ru") -> Dict[str, Any]:
    """Текстовые значения полей 999: по каждому полю (feature_id, title) — список опций (id, title).
    У нас (Bitrix) мапим на эти title. Пример: Bitrix «Motorina» -> 999 option title «Дизель» -> option id."""
    if not _token():
        raise HTTPException(status_code=503, detail="999.md token not set")
    try:
        data = get_features_from_api(lang=lang)
        return {"lang": lang, "features": _features_to_map_list(data)}
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/features-from-999/save")
def api_save_features_from_999(lang: str = "ru") -> Dict[str, Any]:
    """Загрузить фичи с 999.md API и сохранить в 999md_features_cars_sell.json.
    После этого маппинг (год, кузов, топливо, двигатель, привод и т.д.) будет по актуальным опциям 999."""
    if not _token():
        raise HTTPException(status_code=503, detail="999.md token not set")
    try:
        data = get_features_from_api(lang=lang)
        p = Path(__file__).resolve().parent / "999md_features_cars_sell.json"
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return {"ok": True, "path": str(p), "message": "Фичи 999 сохранены. Перезапустите публикацию."}
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/adverts/{advert_id}")
def api_get_advert(advert_id: str, lang: str = "ru") -> Dict[str, Any]:
    if not _token():
        raise HTTPException(status_code=503, detail="999.md token not set")
    try:
        return get_advert(advert_id, lang=lang)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Объявление не найдено")
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/publish")
def api_publish_car_manual(body: PublishCarBody) -> Dict[str, Any]:
    if not _token():
        raise HTTPException(status_code=503, detail="999.md token not set")

    if body.item_id is not None:
        raw = fetch_raw_by_item_id(body.item_id)
        if not raw:
            raise HTTPException(status_code=404, detail=f"Item id={body.item_id} not found in {DATA_TABLE_SP1114}")
        if not should_send_like_tg(raw):
            raise HTTPException(
                status_code=400,
                detail="Машина не проходит фильтр Telegram (не все обязательные поля заполнены). На 999 не публикуем.",
            )
        conn = _pg_conn()
        try:
            if not _was_sent_to_tg(conn, body.item_id):
                raise HTTPException(
                    status_code=400,
                    detail="На 999 шлём только машины, уже отправленные в TG_AUTO. Сначала отправьте в канал (должна быть запись в tg_sent_items).",
                )
        finally:
            conn.close()
        car = car_data_from_raw(raw)
        if not car.get("image_urls"):
            raise HTTPException(
                status_code=400,
                detail=f"В raw нет фото (ключ {PHOTO_RAW_KEY}, url/urlMachine). Добавьте фото в карточку.",
            )
        if not car.get("marca") or not car.get("model"):
            raise HTTPException(status_code=400, detail="В raw нет марки или модели.")
        if not car.get("price") or car["price"] <= 0:
            raise HTTPException(status_code=400, detail="В raw нет цены или она 0.")
        try:
            result = publish_car_manual(
                marca=car["marca"],
                model=car["model"],
                year=car["year"],
                price=car["price"],
                price_unit=car.get("price_unit") or "eur",
                mileage_km=car.get("mileage_km"),
                description=car.get("description") or "",
                numar_auto=car.get("numar_auto") or "",
                phone=car.get("phone") or body.phone or "",
                image_urls=car["image_urls"],
                image_paths=None,
                region_option_id=body.region_option_id,
                listing_title=car.get("listing_title"),
                category_id=raw.get("categoryId"),
                body_type_option_id=car.get("body_type_option_id"),
                fuel_option_id=car.get("fuel_option_id"),
                engine_option_id=car.get("engine_option_id"),
                drive_option_id=car.get("drive_option_id"),
                transmission_option_id=car.get("transmission_option_id"),
            )
            return {"ok": True, "999md": result, "source": "db", "item_id": body.item_id}
        except Exception as e:
            print(f"ERROR publish_999md item_id={body.item_id}: {e}", file=sys.stderr, flush=True)
            raise
    else:
        if not body.marca or not body.model or body.year is None or not body.price or body.price <= 0:
            raise HTTPException(
                status_code=400,
                detail="Без item_id нужны: marca, model, year, price",
            )
        if not (body.image_urls or body.image_paths):
            raise HTTPException(
                status_code=400,
                detail="Нужно хотя бы одно фото: image_urls или image_paths",
            )
        try:
            result = publish_car_manual(
                marca=body.marca,
                model=body.model,
                year=body.year,
                price=body.price,
                price_unit=body.price_unit or "eur",
                mileage_km=body.mileage_km,
                description=body.description or "",
                numar_auto=body.numar_auto or "",
                phone=body.phone or "",
                image_urls=body.image_urls,
                image_paths=body.image_paths,
                region_option_id=body.region_option_id,
            )
            return {"ok": True, "999md": result}
        except requests.HTTPError as e:
            msg = e.response.text if e.response is not None else str(e)
            print(f"ERROR publish_999md: HTTP {e.response.status_code if e.response else ''} {msg}", file=sys.stderr, flush=True)
            raise HTTPException(status_code=502, detail=f"999.md API error: {msg}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            print(f"ERROR publish_999md: {e}", file=sys.stderr, flush=True)
            raise HTTPException(status_code=500, detail=str(e))


def publish_car_manual(
    marca: str,
    model: str,
    year: int,
    price: float,
    price_unit: str = "eur",
    mileage_km: Optional[int] = None,
    description: str = "",
    numar_auto: str = "",
    phone: str = "",
    image_urls: Optional[List[str]] = None,
    image_paths: Optional[List[str]] = None,
    region_option_id: Optional[str] = None,
    listing_title: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    image_ids: List[str] = []
    first_failed_url: Optional[str] = None
    first_failed_code: Optional[int] = None
    if image_paths:
        for path in image_paths:
            image_ids.append(upload_image(path))
    if image_urls:
        for url in image_urls:
            img_id, failed_code = upload_image_from_url_optional(url)
            if img_id:
                image_ids.append(img_id)
            elif failed_code and first_failed_url is None:
                first_failed_url = url
                first_failed_code = failed_code
    # Строго как в TG: все фото должны загрузиться, частичная загрузка не допускается
    if image_urls and len(image_ids) < len(image_urls):
        msg = (
            "Не все фото загружены (401/403 от Bitrix или ошибка). Объявление не публикуется."
        )
        if first_failed_code and first_failed_url:
            msg += f" Первый запрос: {first_failed_code} для URL ...{first_failed_url[-80:]}"
        raise RuntimeError(msg)
    if image_urls and not image_ids:
        raise RuntimeError(
            "Не удалось загрузить ни одного фото. Объявление не публикуется."
        )

    payload = build_advert_payload(
        marca=marca,
        model=model,
        year=year,
        price=price,
        price_unit=price_unit,
        mileage_km=mileage_km,
        description=description,
        numar_auto=numar_auto,
        phone=phone,
        image_ids=image_ids if image_ids else None,
        region_option_id=region_option_id,
        listing_title=listing_title,
        **kwargs,
    )

    if PUBLISH_999MD_DRAFT_ONLY:
        draft_path = _save_draft_payload(payload, item_id=kwargs.get("item_id"))
        return {
            "ok": True,
            "draft": True,
            "message": "Сохранено в черновики локально. Payload в файле.",
            "draft_path": draft_path,
            "advert": None,
        }

    # Сначала пробуем без телефона — возможно 999 создаёт черновик для «неполного» объявления.
    result = None
    try:
        payload_no_phone = _payload_without_phone(payload)
        if payload_no_phone.get("features") != payload.get("features"):
            try:
                result = post_advert(payload_no_phone)
            except RuntimeError as e1:
                if "400" in str(e1) or "required" in str(e1).lower() or "invalid" in str(e1).lower():
                    result = None
                else:
                    raise
    except RuntimeError:
        pass
    if result is None:
        try:
            result = post_advert(payload)
        except RuntimeError as e:
            err_str = str(e)
            if "insufficient balance" in err_str.lower():
                draft_path = _save_draft_payload(payload, item_id=kwargs.get("item_id"))
                return {
                    "ok": True,
                    "draft": True,
                    "message": "На 999.md недостаточно баланса. Payload сохранён локально.",
                    "draft_path": draft_path,
                    "advert": None,
                    "999_error": err_str,
                }
            raise

    # Сразу после создания ставим access_policy "private" — объявление в «Скрытые».
    advert_id = (result.get("advert") or {}).get("id")
    if advert_id:
        try:
            set_advert_access_policy(str(advert_id), "private")
        except Exception as e:
            print(f"WARN: access_policy=private для {advert_id}: {e}", file=sys.stderr, flush=True)
        first_photo = image_urls[0] if image_urls else None
        send_telegram_notification_999(
            str(advert_id),
            marca=marca,
            model=model,
            numar_auto=numar_auto,
            category_id=kwargs.get("category_id"),
            photo_url=first_photo,
        )
    return result


def publish_random_car_to_999() -> Optional[Dict[str, Any]]:
    raw = fetch_random_raw_for_999()
    if not raw:
        return None
    car = car_data_from_raw(raw)
    if not car.get("image_urls"):
        return None
    if not car.get("marca") or not car.get("model"):
        return None
    if not car.get("price") or car["price"] <= 0:
        return None
    item_id = get_item_id_from_raw(raw)
    try:
        result = publish_car_manual(
            marca=car["marca"],
            model=car["model"],
            year=car["year"],
            price=car["price"],
            price_unit=car.get("price_unit") or "eur",
            mileage_km=car.get("mileage_km"),
            description=car.get("description") or "",
            numar_auto=car.get("numar_auto") or "",
            phone=car.get("phone") or "",
            image_urls=car["image_urls"],
            image_paths=None,
            region_option_id=None,
            listing_title=car.get("listing_title"),
            item_id=item_id,
            category_id=raw.get("categoryId"),
            body_type_option_id=car.get("body_type_option_id"),
            fuel_option_id=car.get("fuel_option_id"),
            engine_option_id=car.get("engine_option_id"),
            drive_option_id=car.get("drive_option_id"),
            transmission_option_id=car.get("transmission_option_id"),
        )
        return {"ok": True, "999md": result, "source": "random", "item_id": item_id}
    except Exception as e:
        print(f"ERROR publish_random_car_to_999: {e}", file=sys.stderr, flush=True)
        raise


def _auto_publish_loop() -> None:
    """Фоновая петля авто-отправки на 999 (как в auto_send_tg): раз в POLL_INTERVAL_999, в окне SEND_WINDOW_*."""
    while True:
        try:
            time.sleep(POLL_INTERVAL_999)
            if not _token():
                continue
            if PUBLISH_999MD_DRAFT_ONLY:
                continue
            now_local = datetime.now()
            if not (SEND_WINDOW_START_HOUR <= now_local.hour < SEND_WINDOW_END_HOUR):
                print("AUTO_999: вне окна отправки, жду...", flush=True)
                continue
            raw = fetch_next_raw_for_999()
            if not raw:
                print("AUTO_999: нет машин под условия (tg_sent + не на 999 + category 111 + 5–10 фото + скаляры), жду...", flush=True)
                continue
            item_id = get_item_id_from_raw(raw)
            print(f"AUTO_999: кандидат item_id={item_id}, проверки...", flush=True)
            if not should_send_like_tg(raw):
                item_id = get_item_id_from_raw(raw)
                print(
                    f"SKIP 999.md: item {item_id} does NOT pass Telegram filter",
                    file=sys.stderr,
                    flush=True,
                )
                continue
            if not item_id:
                continue
            photo_urls = extract_photo_urls_from_raw(raw, _bitrix_webhook())
            if not photo_urls:
                print(
                    f"SKIP 999.md: item {item_id} has NO valid photos (TG logic)",
                    file=sys.stderr,
                    flush=True,
                )
                continue
            conn = _pg_conn()
            try:
                if _was_sent_to_999(conn, item_id):
                    print(f"AUTO_999: item_id={item_id} уже на 999, пропуск", flush=True)
                    continue
            finally:
                conn.close()
            car = car_data_from_raw(raw)
            urls = car.get("image_urls") or []
            if not urls:
                print(
                    f"SKIP 999.md: item {item_id} has NO valid photos (TG logic)",
                    file=sys.stderr,
                    flush=True,
                )
                continue
            if not car.get("marca") or not car.get("model"):
                print(f"AUTO_999: item_id={item_id} без марки/модели, пропуск.", flush=True)
                continue
            if not car.get("price") or car["price"] <= 0:
                print(f"AUTO_999: item_id={item_id} без цены, пропуск.", flush=True)
                continue
            try:
                print(f"AUTO_999: публикуем на 999 item_id={item_id} ...", flush=True)
                publish_car_manual(
                    marca=car["marca"],
                    model=car["model"],
                    year=car["year"],
                    price=car["price"],
                    price_unit=car.get("price_unit") or "eur",
                    mileage_km=car.get("mileage_km"),
                    description=car.get("description") or "",
                    numar_auto=car.get("numar_auto") or "",
                    phone=car.get("phone") or "",
                    image_urls=car["image_urls"],
                    image_paths=None,
                    region_option_id=None,
                    listing_title=car.get("listing_title"),
                    item_id=item_id,
                    category_id=raw.get("categoryId"),
                    body_type_option_id=car.get("body_type_option_id"),
                    fuel_option_id=car.get("fuel_option_id"),
                    engine_option_id=car.get("engine_option_id"),
                    drive_option_id=car.get("drive_option_id"),
                    transmission_option_id=car.get("transmission_option_id"),
                )
                conn = _pg_conn()
                try:
                    _mark_sent_to_999(conn, item_id)
                finally:
                    conn.close()
                print(f"AUTO_999: отправлено на 999 item_id={item_id}", flush=True)
            except Exception as e:
                print(f"AUTO_999: ошибка публикации item_id={item_id}: {e}", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"AUTO_999: ошибка цикла: {e}", file=sys.stderr, flush=True)


def start_auto_publish_999_thread() -> None:
    """Запустить фоновый поток авто-отправки на 999 (если AUTO_PUBLISH_999_ENABLED=1)."""
    if not AUTO_PUBLISH_999_ENABLED:
        return
    t = threading.Thread(target=_auto_publish_loop, daemon=True)
    t.start()
    print("AUTO_999: фоновый поток авто-отправки на 999 запущен.", flush=True)
