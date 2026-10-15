# E05 render selection

`POST /v1/renders` accepts an authenticated render selection and an
`Idempotency-Key` header. Choose either a published immutable `releaseId`, or
a registered working `snapshotId` plus `targetId`:

```json
{
  "scope": {"workspaceId": "demo", "projectId": "roundtrip"},
  "releaseId": "release_example",
  "parameters": {},
  "fileRefs": {
    "/logo": {"resourceId": "resource_example", "filename": "logo.svg"}
  },
  "profile": "archive",
  "cachePolicy": "use"
}
```

Projects resolves the selected target and profile, applies shared Core defaults
only to missing values, validates JSON Schema and verifies each accessible
same-project file resource. Invalid parameters, file bindings, profiles and
unsupported bundle execution fail before admission. The API returns bounded
field diagnostics with JSON Pointers; source values and dependency errors stay
private. Bundle execution belongs to E07; cache policy is recorded for E15.

Generation persists the original selection and resolved execution under separate
fingerprints. An exact replay checks current caller rights, then returns the
saved job without resolving a newer revision or requiring the release to remain
published. Changed inputs under the same key conflict. A saved terminal job also
replays when it expired before pin activation or workflow dispatch.

Every input-resource pin must activate before source-pin activation records the
workflow-start intent. Privileged file reads repeat current access and exact
resource checks using the bounded render-attempt context. The renderer receives
only frozen input bytes and descriptors, verifies them through Core and creates
the prepared snapshot without reapplying defaults. It receives no service
credentials.

Release rendering requires current `generation.create` permission; working and
legacy explicit-entrypoint rendering also require `source.read`. Each file
requires `resource.read`. Current release publication and caller rights are
checked again before publishing a candidate. Dependency uncertainty keeps the
result private. Source parameters and diagnostics are redacted from callers
without source access. A successful PDF remains historical after withdrawal and
retains its original fixed 24-hour Storage expiry.

Configure `GENERATION_PROJECTS_URL` and `GENERATION_PROJECTS_TOKEN` together.
The trusted Projects credential must differ from public caller and callback
credentials. The E05 Infra stand supplies this pair and the versioned Contracts
client. The E04 `POST /v1/jobs` and local Go wrapper remain supported.
