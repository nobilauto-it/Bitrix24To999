# Вопрос в поддержку 999.md (Partners API)

**Цель:** создавать объявления через API так, чтобы они появлялись на 999.md в разделе **«Черновики»** и **не списывались деньги** (списание только при ручной публикации из черновика).

Сейчас при POST /adverts объявление создаётся как опубликованное (или скрытое) и сразу списываются средства. В документации API нет способа создать объявление в статусе черновик (state=draft).

**Вопрос для поддержки 999.md:**

```
Здравствуйте. Используем Partners API для размещения объявлений.

Как через API создать объявление в разделе «Черновики» (state=draft), 
чтобы деньги не списывались при создании, а только при ручной публикации 
черновика на сайте?

Пробуем при POST /adverts передавать в теле:
- access_policy: "draft"
- state: "draft"
и в query: ?state=draft
— объявление всё равно создаётся с списанием средств.

Есть ли официальный параметр или отдельный endpoint для создания черновика 
без списания? Спасибо.
```

**Вариант на румынском:**

```
Bună ziua. Folosim Partners API pentru a publica anunțuri.

Cum putem crea prin API un anunț în secțiunea «Ciorne» (state=draft), 
astfel încât banii să nu fie debitați la creare, ci doar la publicarea 
manuală a ciornei pe site?

La POST /adverts trimitem în body: access_policy: "draft", state: "draft" 
și în query: ?state=draft — anunțul se creează tot cu debitare.

Există parametru oficial sau endpoint separat pentru crearea ciornei 
fără debitare? Mulțumim.
```

---

После ответа поддержки — обновить `publish_999md.py` (параметры при создании или отдельный вызов).
