# Leafrun SaaS v1 design verification

> Historical document, retained from the upstream repository. Superseded by the [current Leafrun architecture](https://github.com/leafrun/leafrun/blob/main/docs/architecture.md) and [roadmap](https://github.com/leafrun/leafrun/blob/main/docs/roadmap.md). Implementation claims refer to the dated historical revision, not this checkout.

October 7, 2026. This report checks documents, diagrams and changes to the [Miro board](https://miro.com/app/board/uXjVEd6scg8=/). It does not claim that the proposed SaaS features were implemented.

## Miro

- Live board index: 95 items, 16 frames, 12 diagrams, 3 tables, 61 text areas and 3 sticky notes.
- Added 10 sections, 7 diagrams, 3 tables and navigation using actual section links.
- Writes completed without failed or skipped items. Earlier diagrams were read back; their Mermaid sources matched the state before expansion.
- Read back 56 items in the new chapter and headings. Used `data-rendered-bounds` for 10 diagrams/tables and actual native-text coordinates and sizes for 36 text items.
- Checked 46 content blocks excluding background frames: no overlaps and no unknown dimensions. The previous user layout was treated separately; its sections were not moved.
- Tables measured 1186 × 808, 1524 × 792 and 1186 × 693 and fit their sections. New board diagrams measured 1600 × 900.

## Files

Local Mermaid 11.4.1 parsed all 12 `.mmd` files. The seven new diagrams were rendered to SVG and samples were inspected. Miro also accepted them through its converter. Settings and syntax were restricted to constructs supported by Miro's Mermaid format.

Commands ran from the repository root with bundled Node available:

```powershell
node .cache/mermaid/verify-architecture.cjs
node .cache/mermaid/check-plan-code.cjs
git diff --check
```

Local document links and the absence of unfinished-code markers in the first plan were checked. `gofmt` parsed both complete Go examples in the PostgreSQL plan. This checked example syntax, not the future database integration tests. Temporary verification scripts live in ignored `.cache`; diagram sources and results live in documentation.

## Requirements and limits

| Requirement | Location |
|---|---|
| People and bots, Typst/Markdown, web editor | v1 sections 3/4/6; Miro 08–10 |
| Hosted Gitea and connected Git providers | v1 section 5; Miro 03/08/09 |
| Revisions, queue, retries and cancellation | v1 section 7; Miro 02/11 |
| Access and execution isolation | v1 sections 4/8; Miro 04/12 |
| Free/paid plans, companies and collaboration | v1 sections 4/9; Miro 13 |
| Audit, logging and load monitoring | v1 section 10; Miro 14 |
| Scaling servers and spending limits | v1 section 11; Miro 05/14 |
| Failure, backups and recovery | v1 section 11; Miro 15 |
| Development order and acceptance | Roadmap v1; Miro 16; PostgreSQL plan |

This change included no runtime code, database migrations or paid server provisioning. The earlier engine was checked separately in the [October 6 report](../superpowers/verification-2026-10-06.md). Initial migrations and real PostgreSQL tests are prescribed by the [implementation plan](../superpowers/plans/2026-10-07-workspace-foundation.md); run them when implementing that component.

References: [specification](leafrun-saas-v1.md), [diagrams](board-index.md), [stages](roadmap-v1.md).
