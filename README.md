# Leafrun

Одна Go-функция: локальная папка проекта → Typst → PDF.

```go
func Render(repoPath string) (string, error)
```

Компилирует `main.typ`, сохраняет `document.pdf` в той же папке и возвращает абсолютный путь. Существующий PDF перезаписывается. При ошибке возвращает пустой путь и диагностику Typst.

Нужны Go 1.27 и Typst CLI в `PATH`. Проверено с [Typst 0.15.1](https://github.com/typst/typst/releases/tag/v0.15.1).

## Вызов

```go
import "github.com/Feruum/Leafrun"

pdf, err := leafrun.Render("/path/to/local/repository")
```

Путь может быть абсолютным или относительным. Папка уже должна находиться локально; наличие `.git` не требуется. Функция запускает:

```sh
typst compile --root <project> <project>/main.typ <project>/document.pdf
```

Готовый пример:

```sh
go run ./examples/render ./examples/invoice
```

Результат — `examples/invoice/document.pdf`. Пример использует локальный `data.json`, изображение и вложенный Typst-файл.

## Проверка

```sh
typst --version
go test ./... -count=1 -v
go vet ./...
```

Тесты запускают настоящий Typst: проверяют PDF с изображением и JSON, относительный путь с пробелами, ошибку компиляции и отсутствие `main.typ`. Без Typst в `PATH` они пропускаются.

Если в этом рабочем каталоге уже скачан Windows-бинарник в `.tools`, добавить его в `PATH` текущего PowerShell можно так:

```powershell
$env:PATH = (Resolve-Path '.tools/typst-v0.15.1/typst-x86_64-pc-windows-msvc').Path + [IO.Path]::PathSeparator + $env:PATH
```

## История и проектирование

9 октября 2026 код упрощён до локальной функции. Предыдущий HTTP-движок, Git-кэш и контейнерная обвязка сохранены в истории Git на коммите `d77d9a9`.

[Архитектура SaaS](docs/architecture/leafrun-saas-v1.md), [схемы](docs/architecture/board-index.md) и [план развития](docs/architecture/roadmap-v1.md) сохранены как проектные материалы. Отметки готовности старого движка в этих документах относятся к версии до упрощения.
