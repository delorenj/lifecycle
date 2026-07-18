# Bloodbank extraction provenance

Lifecycle began as a history-preserving extraction of the tested Bloodbank
controller embryo. This document records the immutable source snapshot, the
rewrite mapping, and the evaluator baseline before standalone repository
scaffolding was layered on top.

## Immutable source

- Repository: `git@github.com:delorenj/bloodbank.git`
- Source commit: `03415705a39d77f1e6d73c8a9c92ee177320df7e`
- Source path: `services/lifecycle-controller/`
- Source subtree tree: `36054453f7ee192d7715a1676328c15bfdf89607`
- Extraction tool: `git-filter-repo` version `a40bce548d2c`

The extraction was limited to ancestors of the immutable source commit:

```sh
git switch -c extraction-source 03415705a39d77f1e6d73c8a9c92ee177320df7e
git filter-repo --force \
  --refs refs/heads/extraction-source \
  --path services/lifecycle-controller/ \
  --path-rename services/lifecycle-controller/:
```

## Old-to-new mapping

Only one commit in the pinned ancestry modifies the selected path.

| Bloodbank commit | Extracted Lifecycle commit | Meaning |
|---|---|---|
| `a98b5c34d69717ab636c14eb9d11281a4fcfa437` | `ae31b94c31eac6d4f9e7e57cc75b2eb673cbd8d2` | `fix(lifecycle): harden controller runtime paths` |

The raw filter map records the pinned source commit as pruned:

```text
03415705a39d77f1e6d73c8a9c92ee177320df7e 0000000000000000000000000000000000000000
a98b5c34d69717ab636c14eb9d11281a4fcfa437 ae31b94c31eac6d4f9e7e57cc75b2eb673cbd8d2
```

That is expected: `03415705a39d77f1e6d73c8a9c92ee177320df7e`
changes files outside `services/lifecycle-controller/`, so retaining it would
create an empty extraction commit. The pinned snapshot is instead proven by
tree equality:

```text
$ git -C bloodbank rev-parse 03415705a39d77f1e6d73c8a9c92ee177320df7e:services/lifecycle-controller
36054453f7ee192d7715a1676328c15bfdf89607
$ git -C lifecycle rev-parse ae31b94c31eac6d4f9e7e57cc75b2eb673cbd8d2^{tree}
36054453f7ee192d7715a1676328c15bfdf89607
```

The rewritten commit preserves the source metadata:

```text
author:         Jarad DeLorenzo <jaradd@gmail.com>
author date:    2026-06-01T23:20:20-04:00
committer:      Jarad DeLorenzo <jaradd@gmail.com>
committer date: 2026-06-01T23:20:20-04:00
message:        fix(lifecycle): harden controller runtime paths
```

Path ancestry remains directly queryable in this repository:

```text
$ git log --follow --format='%H %aI %an <%ae> %s' -- src/reconciler.py
ae31b94c31eac6d4f9e7e57cc75b2eb673cbd8d2 2026-06-01T23:20:20-04:00 Jarad DeLorenzo <jaradd@gmail.com> fix(lifecycle): harden controller runtime paths
```

## Untouched evaluator baseline

The focused suite and Ruff ran on extracted commit `ae31b94c31eac6d4f9e7e57cc75b2eb673cbd8d2`
before CommonProject files were added:

```text
$ uv run --frozen --extra dev pytest -q
.....................                                                    [100%]
21 passed in 0.08s

$ uv run --frozen --extra dev ruff check .
All checks passed!
```

The next commit, `7c66618e0ccc82a17a1051bf2136663b81cd896f`,
adds only the PJangler/CommonProject repository scaffold and canonical project
identity. It does not alter the extracted implementation or evaluator tests.
