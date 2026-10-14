# Contributing

Use English for source comments, documentation, commit messages, and pull request titles and descriptions. Unicode document fixtures are welcome where they verify rendering behavior.

## Git flow

- `main` contains released code.
- `develop` is the integration branch.
- Start `feature/<issue>-<description>` from `develop` and open a pull request into `develop`.
- Start `release/<version>` from `develop` after the feature acceptance checks pass. Merge the release into `main`, tag that merge as `v<version>`, and merge the release changes back into `develop`.
- Start urgent `hotfix/<version>` changes from `main`; merge them into both `main` and `develop` and create the next patch tag.

Use merge commits for feature and release integration so the branch history remains visible. Do not merge feature pull requests directly into `main` or rewrite shared branch history.

Run the checks in [README.md](README.md#development) before requesting review. The document core must be released as `v0.1.0` first. Then verify Generation with `GOWORK=off`, commit the dependency checksum, and request integration into `develop`.

## Initial branch setup

If the repository currently has only `main`, a maintainer first creates `develop` from the unchanged `main`:

```sh
git fetch origin
git switch -c develop origin/main
git push -u origin develop
```

Feature branches prepared before this setup use the same starting commit. Retarget any draft pull request to `develop` before merging it. A temporary comparison against `main` is for review only and must stay a draft until the integration branch exists.
