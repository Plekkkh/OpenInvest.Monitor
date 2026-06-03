# OpenInvest.Monitor

[![Python](https://img.shields.io/badge/Python->=3.11-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Django](https://img.shields.io/badge/Django->=6.0-blue.svg?logo=django&logoColor=white)](https://www.djangoproject.com/)
[![Pandas](https://img.shields.io/badge/Pandas->=3.0-blue.svg?logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![T-Invest API](https://img.shields.io/badge/T--Invest_API-Supported-yellow.svg)](https://opensource.tbank.ru/invest/invest-python)
[![Status](https://img.shields.io/badge/Status-MVP-success.svg)](TZ.md)

Платформа для консолидации инвестиционных данных и расчета реальной доходности портфеля с учетом пополнений, выводов и налогов. MVP ориентирован на интеграцию с Т-Банком (Т-Инвестиции) и масштабирование под других брокеров.

## Возможности
- Подключение брокерских счетов и безопасное хранение API‑токенов (шифрование `Fernet`).
- Синхронизация операций через API брокера с дедупликацией по `external_id`.
- Аналитика портфеля: **XIRR** (внутренняя норма доходности) и **TWR** (доходность, взвешенная по времени).
- Метрики прибыли: разница цен, реализованный P&L, дивиденды, налоги, комиссии, НКД.
- Дашборд с интерактивной диаграммой распределения активов.
- Фильтрация транзакций по счёту с AJAX-обновлением без перезагрузки страницы.
- Разделение доступа: пользователь видит только свои данные.

## Стек технологий
- **Backend:** Django, Django ORM
- **Аналитика:** pandas, pyxirr
- **Безопасность:** cryptography Fernet
- **Интеграции:** T‑Invest API, резервно MOEX ISS API
- **Frontend:** Bootstrap, ApexCharts

## Архитектура
- **Service Layer:** бизнес‑логика и интеграции в `portfolio/services/`.
- **Тонкие views и модели:** модели — структура данных, views — обработка запросов и вызов сервисов.
- **Адаптер брокера:** интеграция с API изолирована.

## Метрики портфеля

### XIRR (eXtended Internal Rate of Return)
**Внутренняя норма доходности** — учитывает размер и момент каждого денежного потока (депозит, вывод).
- Сценарий: пополнили \$1000 в начале года, добавили \$10000 в середине.
- XIRR показывает: сколько процентов в год заработали ваши деньги, с учётом того, что часть работала дольше.
- **Минус:** чувствительна к времени пополнений: пополнили перед падением — XIRR урежется.

### TWR (Time-Weighted Return)
**Доходность, взвешенная по времени** — показывает, как бы работали вложения без внешних потоков.
- Алгоритм: период разбивается на интервалы между пополнениями/выводами; считается доходность в каждом интервале; результаты перемножаются.
- **Плюс:** независима от размера и тайминга ваших пополнений, показывает только качество управления портфелем.
- **Использовать:** для оценки качества инвестиционных решений.

Пример:
```
Начало: 10 000 ₽.
Месяц 1: +500 ₽ (5% доходность).
Вы пополняете: +5 000 ₽ (всего 15 500 ₽).
Месяц 2: +500 ₽ (3.2% доходность).

XIRR ≈ 46% в год (большие деньги работали мало).
TWR = (1.05 × 1.032) − 1 ≈ 8.36% (чистая доходность портфеля).
```

## Модель данных
ER‑диаграмма:

![ER-диаграмма](ER-diagram.png)

Подробное описание и сценарии — в `TZ.md`.

## Быстрый старт (локально)
1) Создайте и активируйте виртуальное окружение.
2) Установите зависимости.
3) Настройте переменные окружения.
4) Примените миграции и запустите сервер.

Пример команд (Windows cmd):

```cmd
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

> Примечание: для установки `t-tech-investments` нужен индекс T-Bank:
> `pip install t-tech-investments --index-url https://opensource.tbank.ru/api/v4/projects/238/packages/pypi/simple`

Создайте файл `.env` на основе `example.env` и заполните значения:
- `SECRET_KEY`
- `DEBUG`
- `FERNET_KEY`

Далее примените миграции и запустите сервер:

```cmd
python manage.py migrate
python manage.py runserver
```

Чтобы заполнить базу демонстрационными данными для дашборда:

```cmd
python manage.py seed_demo_portfolio
```

По умолчанию команда создаёт пользователя `123` и счет `Test_Inv`.

## Production: PostgreSQL

Рекомендуется использовать PostgreSQL в продакшне.

- Установите зависимости:

```cmd
pip install -r requirements.txt
```

- Пример строки подключения (DATABASE_URL):

```
postgres://dbuser:dbpassword@dbhost:5432/dbname
```

- В `.env` добавьте или задайте переменные окружения:

```cmd
DATABASE_URL="postgres://dbuser:dbpassword@dbhost:5432/dbname"
DB_SSL="True"
ALLOWED_HOSTS="your-domain.com"
```

- Запустите миграции и соберите статику:

```cmd
python manage.py migrate
python manage.py collectstatic --noinput
```

Примечание: `config/settings.py` автоматически использует `DATABASE_URL` если она определена; в противном случае остаётся локальная SQLite.

## Скриншоты

![Login](screenshots/login.png)
![Dashboard](screenshots/dashboard.png)
![Transactions](screenshots/transactions.png)


## Безопасность
- API‑токены сохраняются только в зашифрованном виде.
- В UI выводится маскированный токен.
- Все запросы к данным фильтруются по пользователю.

## Тестирование

```cmd
python manage.py test
```

16 unit-тестов покрывают:
- Расчётные сервисы (Analytics, TInvest): XIRR, TWR, синхронизация операций.
- Защита от IDOR (Insecure Direct Object Reference).
- AJAX-сценарии фильтрации.
- Все внешние запросы замокированы: нет live API-вызовов.

## Структура проекта
```
OpenInvest.Monitor/
├─ config/                 # настройки Django
├─ portfolio/              # логика инвестиций, сервисы, шаблоны
├─ users/                  # пользователи, авторизация
├─ examples/               # примеры работы с API
├─ TZ.md                   # техническое задание
└─ manage.py
```

---

**Статус:** MVP готов.
