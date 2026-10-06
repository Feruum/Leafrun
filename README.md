# Leafrun

Синхронный движок на Go: `POST /api/render` скачивает актуальный Git-проект, запускает Typst 0.15.1 и возвращает PDF. Gitea 28.0.0 хранит проекты, историю и ветки; renderer хранит только удаляемый Git-кэш.

## Запуск

Нужны Docker с Linux-контейнерами и Docker Compose. В PowerShell из корня проекта:

```powershell
Copy-Item .env.example .env
$bytes = New-Object byte[] 32
$rng = [Security.Cryptography.RandomNumberGenerator]::Create()
$rng.GetBytes($bytes)
$rng.Dispose()
$key = -join ($bytes | ForEach-Object { $_.ToString('x2') })
(Get-Content .env) -replace '^API_KEY=.*$', "API_KEY=$key" | Set-Content .env
docker compose up -d --build
```

Команды работают в Windows PowerShell 5.1 и PowerShell 7. `.env` исключён из Git.

Откройте [Gitea](http://localhost:3000/), завершите первичную установку с SQLite и создайте администратора в форме установки. Пути БД и хранилища уже заданы Compose. Регистрация новых пользователей и SSH выключены; Git работает по HTTP. Если интерфейс предлагает менять внешний адрес, оставьте `http://localhost:3000/`.

API доступен по `http://localhost:8080/api/render`. Оба порта опубликованы на loopback. Состояние сервисов: `docker compose ps`; журнал движка: `docker compose logs renderer`.

## Добавить проект в Gitea

Для внешнего проекта выберите **+ → New Migration**, Git, вставьте публичный clone URL и имя нового репозитория. Отключите Mirror, если нужна самостоятельная редактируемая копия. Миграция сохраняет Git-историю. Последующие коммиты, ветки и merge делаются в Gitea или обычным Git.

Для тестового проекта создайте пустой публичный репозиторий `invoice` в Gitea и отправьте содержимое `examples/invoice/`:

```powershell
Set-Location examples/invoice
git init -b main
git add .
git commit -m "Add Typst invoice"
git remote add origin http://localhost:3000/YOUR_USER/invoice.git
git push -u origin main
Set-Location ../..
```

Git запросит учётные данные Gitea. Замените `your-user` в `examples/render-request.json` на владельца репозитория.

## Скачать PDF

PowerShell с UTF-8 JSON:

```powershell
$apiKey = ((Get-Content .env | Where-Object { $_ -match '^API_KEY=' }) -replace '^API_KEY=', '')
$body = Get-Content examples/render-request.json -Raw -Encoding UTF8
Invoke-WebRequest -Uri http://localhost:8080/api/render -Method Post -Headers @{ Authorization = "Bearer $apiKey" } -ContentType 'application/json; charset=utf-8' -Body ([Text.Encoding]::UTF8.GetBytes($body)) -OutFile document.pdf
```

Или curl, где `API_KEY` — значение из `.env`:

```sh
curl --fail-with-body \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  --data-binary @examples/render-request.json \
  --dump-header response-headers.txt \
  --output document.pdf \
  http://localhost:8080/api/render
```

В Windows используйте `curl.exe`. При ошибке тело ответа содержит JSON; проверяйте HTTP-статус перед открытием файла.

## Контракт

```json
{
  "repo_url": "http://localhost:3000/your-user/invoice.git",
  "ref": "main",
  "entrypoint": "main.typ",
  "data": {
    "title": "Счёт № 42",
    "customer": "Пример компании",
    "items": [{"description": "Разработка", "quantity": 1, "price": 50000}]
  }
}
```

| Поле | Значение |
| --- | --- |
| `repo_url` | Обязательный HTTP(S) clone URL без пароля, query и fragment |
| `ref` | По умолчанию актуальный удалённый `HEAD`; ветка, тег или полный достижимый SHA |
| `entrypoint` | По умолчанию `main.typ`; относительный путь, например `reports/monthly.typ` |
| `data` | По умолчанию `{}`; JSON-объект, а не массив или `null` |

Совпадающие имена ветки и тега дают `ambiguous_ref`: укажите `refs/heads/name` или `refs/tags/name`. Полный SHA должен быть достижим из текущих удалённых веток или тегов. После force-push удалённый недостижимый коммит использовать нельзя.

Успех: `200 application/pdf`, `Content-Disposition: attachment; filename="document.pdf"` и `X-Source-Commit` с использованным SHA. Неизвестные поля и несколько JSON-значений в одном теле отклоняются.

## Данные, изображения, шрифты и пакеты

Шаблон получает путь к уникальному JSON через `sys.inputs.data_file`:

```typst
#let data = json(sys.inputs.data_file)
= #data.at("title", default: "Отчёт")
```

Путь начинается с `/` и считается от корня проекта, поэтому он работает и во вложенном entrypoint. Типографические файлы, include и изображения сохраняют структуру репозитория. Передаётся `typst compile --root <project>`.

Системные шрифты не используются. Встроенные шрифты Typst, включая Libertinus Serif, доступны; собственные `.ttf`/`.otf` положите в `fonts/` и выберите семейство через `set text(font: "...")`.

Обычные импорты `@preview/package:version` поддерживаются. Typst скачивает официальные пакеты в отдельную папку запроса; нужен исходящий HTTPS-доступ к реестру Typst. Эта папка удаляется после запроса, поэтому первый импорт повторяется при следующей сборке.

Git LFS, submodules и symlinks отклоняются с `unsupported_project`. Используйте обычные Git-файлы. JSON и PDF не записываются обратно в репозиторий.

## Приватная Gitea

Создайте отдельного пользователя для движка, предоставьте ему **Read** только к нужным репозиториям и создайте access token. Укажите в `.env`:

```dotenv
GITEA_USERNAME=renderer-reader
GITEA_TOKEN=YOUR_TOKEN
```

Затем `docker compose up -d renderer`. Уровень доступа репозитория ограничивается правами пользователя; токен должен разрешать чтение репозиториев. Не используйте администратора для регулярных сборок.

Учётные данные передаются только Git-процессу, только для настроенного адреса Gitea. Они не сохраняются в clone URL, Git-конфиге кэша или журнале.

`GITEA_PUBLIC_URL=http://localhost:3000/` преобразуется в `GITEA_INTERNAL_URL=http://gitea:3000/`. В API можно передавать clone URL из интерфейса. При изменении внешнего порта согласуйте `GITEA_PORT` и `GITEA_PUBLIC_URL`.

Для внешних публичных репозиториев разрешены HTTPS-хосты из `GIT_ALLOWED_HOSTS` (по умолчанию `github.com,gitlab.com`). Используйте конечный clone URL: HTTP-редиректы отключены. Приватные внешние репозитории в v1 не поддерживаются.

## Кэш и ограничения

На каждый запрос выполняется forced atomic fetch с pruning веток и тегов, включая проверку актуального удалённого HEAD. Недоступный Git даёт ошибку; старый кэш не используется для обхода ошибки. Файлы отдельного detached worktree закреплены за SHA. Компиляции выполняются вне блокировки Git-кэша.

При отмене или таймауте движок завершает дерево процессов и ждёт выхода, затем удаляет рабочую папку. После передачи PDF удаляются JSON, PDF и скачанные пакеты. На старте убираются остатки прерванных запросов и Git-транзакций.

Простаивающие bare-кэши удаляются через 24 часа; проверка выполняется раз в минуту. Активные и ожидающие Git-запросы защищены от удаления. Каждый час свободные кэши очищаются от недостижимой истории после force-push. PDF собирается заново для каждого запроса.

| Переменная | По умолчанию |
| --- | --- |
| `REQUEST_TIMEOUT` | `60s` на чтение запроса, Git и компиляцию |
| `MAX_CONCURRENT` | `2`; при занятых слотах `429 busy` |
| `MAX_REQUEST_BYTES` | `1048576`, 1 MiB на всё тело JSON |
| `MAX_PROJECT_BYTES` | `104857600`, 100 MiB на файлы проекта |
| `CACHE_TTL` | `24h` |
| `API_PORT` / `GITEA_PORT` | `8080` / `3000` |
| `CACHE_DIR` / `WORK_DIR` | В Compose `/tmp/typst-render/cache` / `/tmp/typst-render/work` |

Лимит проекта проверяется до checkout по Git blobs и после checkout по фактическим размерам. История репозитория, пакеты и PDF не входят в эти 100 MiB. Compose ограничивает renderer 2 GiB памяти и общим tmpfs 1 GiB. Для больших историй или документов увеличьте ресурсы в Compose.

Один экземпляр движка владеет своим кэшем и рабочей папкой. Несколько экземпляров не должны разделять эти пути.

Gitea использует постоянные named volumes `gitea-data` и `gitea-config`. `docker compose down` сохраняет их. `docker compose down -v` удаляет проекты и учётные записи. Кэш renderer находится в tmpfs: пересоздание контейнера вызывает повторное скачивание проекта.

## Ошибки

```json
{"code":"typst_error","message":"Typst could not compile the project.","diagnostics":"..."}
```

| HTTP | Коды |
| --- | --- |
| `400` | `invalid_request`, `invalid_repository`, `invalid_ref`, `ref_not_found`, `ambiguous_ref`, `invalid_entrypoint`, `unsupported_project` |
| `401` | `unauthorized` — отсутствующий или неверный API key |
| `408` | `canceled` — отмена запроса, если соединение ещё доступно |
| `413` | `request_too_large`, `project_too_large` |
| `422` | `typst_error` |
| `429` | `busy` |
| `502` | `git_error` — fetch, checkout или отказ Git-сервера в доступе |
| `504` | `timeout` |
| `500` | `internal_error` |

Диагностика ограничена 16 KiB. Настроенные секреты и временные пути заменяются. Запросы и содержимое документов не логируются. Для отправки ошибки после вычислительного таймаута есть отдельное окно до 5 секунд; прерванную передачу уже начатого PDF нельзя заменить JSON.

## Разработка и проверки

Go 1.27, Git и Typst CLI **0.15.1**. Зависимостей Go вне стандартной библиотеки нет.

```powershell
$env:TYPST_BIN = 'C:\path\to\typst.exe'
go test ./... -count=1
go vet ./...
go build -o bin/renderd.exe ./cmd/renderd
```

Без `TYPST_BIN` реальные PDF-тесты пропускаются; остальные тесты используют настоящий Git во временных репозиториях и HTTP Git backend.

Проверка Linux с race detector, настоящим Typst, Git, шрифтом и официальным пакетом:

```sh
docker build --target test -t typst-render-tests .
docker compose config --quiet
```

Тесты проверяют новые коммиты, force-push, удаление веток и тегов, смену HEAD, достижимость SHA, отказ fetch, параллельные worktree и JSON, очистку и восстановление, лимиты, PDF, вложенный entrypoint, изображения, fonts/, пакеты и остановку дочерних процессов.

Исходное решение: `docs/superpowers/specs/2026-10-06-typst-renderer-design.md`. План и результаты проверки находятся в `docs/superpowers/`.

## Проектирование SaaS

[Архитектура Leafrun SaaS v1](docs/architecture/leafrun-saas-v1.md) описывает редактор, пользователей и ботов, Git-провайдеры, очередь, изоляцию, тарифы, аудит, масштабирование и восстановление. [Навигация по 16 разделам Miro и 12 схемам Mermaid](docs/architecture/board-index.md), [этапы реализации](docs/architecture/roadmap-v1.md) и [первый план PostgreSQL](docs/superpowers/plans/2026-10-07-workspace-foundation.md) сохранены в репозитории.

Сейчас реализован и проверен движок Git → Typst → PDF. Аккаунты, веб-редактор, Markdown-адаптер, команды, тарифы, очередь SaaS и автоматическое масштабирование остаются будущей разработкой.
