# GitHub Repository Info

Generated after the 2026-07-27 cleanup/restructure/documentation pass.

## Repository

- **URL**: https://github.com/AuroraRyunix/Helios-HCI
- **Remote (`origin`)**: `https://github.com/AuroraRyunix/Helios-HCI.git` (fetch + push)
- **Default/active branch**: `main`
- **Other remote branches**: `witness-node-provisioning-fixes`

## This release (cleanup commit)

- **Commit**: `d2861c6d6937ada82fc6de3712bf14f546d0f905`
- **Message**: "Repo cleanup: remove dead assets, archive stale changelogs, strip dead provisioner sync code, trim vendored spice-html5 scaffolding, and add full documentation set"
- **Parent**: `6cd254c` ("Update cluster documentation with Witness Node support, setup, and orchestration architecture details")
- **Pushed to**: `origin/main` (fast-forward, `6cd254c..d2861c6`)

### What changed
- **Deleted**: `index.html`, `extras.html`, `urbosa.html`, `static/test_syntax.html`, `logos/` (35 MB, 44 PNGs), `docs/master_flowchart.{png,svg}`, `docs/master_technical_mindmap.{png,svg}`, `file_mindmap.md`, and non-runtime `static/spice-html5/` packaging scaffolding (`Makefile`, `*.spec.in`, `apache.conf.sample`, `TODO`, `package.json.in`, `.npmignore`).
- **Moved**: `walkthrough.md`, `task.md`, `readme_old.md` → `docs/history/` (with a new `docs/history/README.md` index).
- **Edited**: `sync_provision.py` (dropped ~260 lines of permanently-inert pre-Quadlet injection logic), `.gitignore` (added secrets/cert/build-output patterns), `README.md` (tech stack, quick start, directory map, corrected two stale claims).
- **Added**: `TODO.md`, `docs/AGENTS.md`, `docs/architecture.md`, `docs/deployment.md`, `docs/setup-guide.md`, `docs/README.md`.
- **Net diff**: 72 files changed, 534 insertions(+), 3,041 deletions(-).

## Repository statistics (post-push)

- **Tracked files**: 217
- **Total commits on `main`**: 1,246
- **Working tree size (excl. `.git`)**: ~7.1 MB (down from ~43 MB pre-cleanup — the 35 MB `logos/` removal is the majority of the reduction)
- **`.git` pack size**: ~39.6 MB (history is retained; deleted files are still recoverable via `git log --diff-filter=D` / `git show <commit>:<path>` since nothing was force-pushed or rewritten)
- **Languages**: Python (root daemons/CLIs), Rust (`agahnim/`), vanilla HTML/CSS/JS (`static/`)
- **CI/CD**: none configured (tracked as an open item in [TODO.md](./TODO.md))
- **License**: none declared at repo root (tracked as an open item in [TODO.md](./TODO.md))
