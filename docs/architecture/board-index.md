# Leafrun: навигация по проектированию

7 октября 2026 года. [Открыть доску Miro](https://miro.com/app/board/uXjVEd6scg8=/). На доске 16 разделов, 12 редактируемых схем и 3 таблицы. [Подробная спецификация v1](leafrun-saas-v1.md) и [порядок разработки](roadmap-v1.md) определяют выбранное поведение.

Первые шесть разделов дают обзор и место для обсуждения. Разделы 07–16 раскрывают сценарии и эксплуатацию. Mermaid и SVG в репозитории сохраняют схемы как файлы; положение элементов и оформление на доске пользователь может менять независимо.

| Раздел в Miro | Файл для чтения или редактирования |
|---|---|
| [01. Общая архитектура](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188094) | [Mermaid](diagrams/01-overview.mmd) · [SVG](diagrams/01-overview.svg) |
| [02. От запроса до PDF](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188095) | [Mermaid](diagrams/02-render-flow.mmd) · [SVG](diagrams/02-render-flow.svg) |
| [03. Git и версии исходников](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188096) | [Mermaid](diagrams/03-git-storage.mmd) · [SVG](diagrams/03-git-storage.svg) |
| [04. Пользователи, роли и аудит](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188097) | [Mermaid](diagrams/04-domain-model.mmd) · [SVG](diagrams/04-domain-model.svg) |
| [05. Развёртывание и масштабирование](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188098) | [Mermaid](diagrams/05-scaling.mmd) · [SVG](diagrams/05-scaling.svg) |
| [06. Решения для обсуждения](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188099) | [Открытые вопросы v0](leafrun-saas-v0.md#порядок-реализации) |
| [07. Что готово и что предстоит](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180908) | [Спецификация: готовность](leafrun-saas-v1.md#1-продукт-и-фактическая-готовность) |
| [08. Сценарий обычного пользователя](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180909) | [Mermaid](diagrams/v1/08-user-journey.mmd) · [SVG](diagrams/v1/08-user-journey.svg) |
| [09. Сохранение и конфликт версий](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180910) | [Mermaid](diagrams/v1/09-editor-save.mmd) · [SVG](diagrams/v1/09-editor-save.svg) |
| [10. API для ботов](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180911) | [Mermaid](diagrams/v1/10-bot-api.mmd) · [SVG](diagrams/v1/10-bot-api.svg) |
| [11. Сборка, повторы и отмена](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180912) | [Mermaid](diagrams/v1/11-job-lifecycle.mmd) · [SVG](diagrams/v1/11-job-lifecycle.svg) |
| [12. Изоляция клиентов и исполнения](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180913) | [Mermaid](diagrams/v1/12-isolation.mmd) · [SVG](diagrams/v1/12-isolation.svg) |
| [13. Тарифы и расход ресурсов](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180914) | [Спецификация: тарифы](leafrun-saas-v1.md#9-тарифы-квоты-и-оплата) |
| [14. Аудит и эксплуатация](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180915) | [Mermaid](diagrams/v1/14-audit-operations.mmd) · [SVG](diagrams/v1/14-audit-operations.svg) |
| [15. Восстановление после сбоя](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180916) | [Mermaid](diagrams/v1/15-recovery.mmd) · [SVG](diagrams/v1/15-recovery.svg) |
| [16. Порядок разработки и приёмка](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180917) | [Этапы](roadmap-v1.md) · [Первый план](../superpowers/plans/2026-10-07-workspace-foundation.md) |

Готовая часть продукта — движок Git → Typst → PDF; подтверждённые проверки находятся в [отчёте renderer](../superpowers/verification-2026-10-06.md). Остальные возможности описаны как будущая разработка, с критериями приёмки.
