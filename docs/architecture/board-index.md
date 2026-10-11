# Leafrun design navigation

> Historical document, retained from the upstream repository. Superseded by the [current Leafrun architecture](https://github.com/leafrun/leafrun/blob/main/docs/architecture.md) and [roadmap](https://github.com/leafrun/leafrun/blob/main/docs/roadmap.md). Implementation claims refer to the dated historical revision, not this checkout.

October 9, 2026. [Open the Miro board](https://miro.com/app/board/uXjVEd6scg8=/). This historical index covers 17 sections, with 13 editable diagrams and 3 tables on the board. The [v1 specification](leafrun-saas-v1.md) and [development sequence](roadmap-v1.md) preserve the earlier SaaS design.

Section 00 describes the current local `Render(repoPath)` function, its code, contract and verification. Section 07 records readiness: the earlier HTTP API, Git cache and Compose setup remain in Git history. Sections 01–06 and 08–16 describe planned development at that time. Mermaid and SVG files preserve diagram sources; board layout and styling may change independently.

| Miro section | Read or edit |
|---|---|
| [00. Current: Render(repoPath) → PDF](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686553812306) | [Code](../../render.go) · [Mermaid](diagrams/00-local-render.mmd) · [Run and verify](../../README.md) |
| [01. Overall architecture · historical SaaS plan](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188094) | [Mermaid](diagrams/01-overview.mmd) · [SVG](diagrams/01-overview.svg) |
| [02. Request to PDF · historical SaaS plan](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188095) | [Mermaid](diagrams/02-render-flow.mmd) · [SVG](diagrams/02-render-flow.svg) |
| [03. Git and source revisions](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188096) | [Mermaid](diagrams/03-git-storage.mmd) · [SVG](diagrams/03-git-storage.svg) |
| [04. Users, roles and audit](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188097) | [Mermaid](diagrams/04-domain-model.mmd) · [SVG](diagrams/04-domain-model.svg) |
| [05. Deployment and scaling](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188098) | [Mermaid](diagrams/05-scaling.mmd) · [SVG](diagrams/05-scaling.svg) |
| [06. Discussion decisions](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686264188099) | [v0 implementation sequence](leafrun-saas-v0.md#implementation-sequence) |
| [07. Implemented and planned capabilities](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180908) | [v1 readiness](leafrun-saas-v1.md#1-product-and-readiness) |
| [08. User journey](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180909) | [Mermaid](diagrams/v1/08-user-journey.mmd) · [SVG](diagrams/v1/08-user-journey.svg) |
| [09. Saving and revision conflicts](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180910) | [Mermaid](diagrams/v1/09-editor-save.mmd) · [SVG](diagrams/v1/09-editor-save.svg) |
| [10. Bot API](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180911) | [Mermaid](diagrams/v1/10-bot-api.mmd) · [SVG](diagrams/v1/10-bot-api.svg) |
| [11. Rendering, retries and cancellation](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180912) | [Mermaid](diagrams/v1/11-job-lifecycle.mmd) · [SVG](diagrams/v1/11-job-lifecycle.svg) |
| [12. Tenant and execution isolation](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180913) | [Mermaid](diagrams/v1/12-isolation.mmd) · [SVG](diagrams/v1/12-isolation.svg) |
| [13. Plans and resource usage](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180914) | [v1 plans](leafrun-saas-v1.md#9-plans-quotas-and-payment) |
| [14. Audit and operations](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180915) | [Mermaid](diagrams/v1/14-audit-operations.mmd) · [SVG](diagrams/v1/14-audit-operations.svg) |
| [15. Recovery after failure](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180916) | [Mermaid](diagrams/v1/15-recovery.mmd) · [SVG](diagrams/v1/15-recovery.svg) |
| [16. Development order and acceptance](https://miro.com/app/board/uXjVEd6scg8=/?moveToWidget=3458764686267180917) | [Stages](roadmap-v1.md) · [First plan](../superpowers/plans/2026-10-07-workspace-foundation.md) |

The implemented product is the local `Render(repoPath)` function: `main.typ` → `document.pdf`. The upstream October 9 verification reported three real-Typst subtests, `go vet` and the example build passing. The [earlier renderer report](../superpowers/verification-2026-10-06.md) covers the version before simplification. Other capabilities here remain future work with acceptance criteria.
