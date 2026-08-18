# docs/ Index

This directory holds the architecture references, per-daemon documentation, and technical-debt backlog for Helios-HCI. Start with the top-level [README.md](../README.md) for the project overview, tech stack, and quick start; use this index to find anything deeper.

---

## Architecture

| Doc | What it covers |
| :--- | :--- |
| [AGENTS.md](./AGENTS.md) | Technical orientation for AI coding agents: repo layout, daemon map, boot sequence, the `provision.py`/`sync_provision.py` embedding mechanism, build/test commands, known invariants. |
| [architecture.md](./architecture.md) | Mid-length system design overview: request path, control-plane vs. data-plane split, network architecture. |
| [deployment.md](./deployment.md) | The Podman Quadlet deployment model, the update-rollout paths, ingress. |
| [setup-guide.md](./setup-guide.md) | Local dev prerequisites, what runs standalone vs. what needs a real cluster. |
| [hci_master_architecture_guide.md](./hci_master_architecture_guide.md) | The deepest existing architecture reference (1500+ lines). |
| [spark_api.md](./spark_api.md) | The typed per-domain Spark API that replaces raw shell execution: the contract, the design rules, and the migration metric. |
| [cluster_state.md](./cluster_state.md) | How desired cluster state and per-node actual state are held in ZooKeeper, the ephemeral-znode liveness model, and the direct-probe fallback. |
| [network.md](./network.md) | Full network scope/port allocation reference. |
| [master_flowchart.md](./master_flowchart.md) | System-wide Mermaid flowchart (database boundaries, mTLS calls, socket loops). |
| [master_technical_mindmap.md](./master_technical_mindmap.md) | High-level taxonomy map of all components. |
| [add_ons_design.md](./add_ons_design.md) | Forward-looking design blueprint for four scale-out add-ons (Helios Portal, Helios Files, Helios Horizon, Scale-Out Urbosa) — none implemented yet. |

## Per-Daemon Docs

Each pair is `<name>.md` (narrative overview) + `<name>_technical.md` (internals, functions, flowcharts). A few tooling scripts only have the technical variant (no separate narrative doc), and a few conceptual components only have a narrative doc (no code-level technical counterpart) — both are noted below.

| Component | Narrative | Technical |
| :--- | :--- | :--- |
| Bifrost (`bifrost.py`) | [bifrost.md](./bifrost.md) | [bifrost_technical.md](./bifrost_technical.md) |
| Catalyst (`catalyst.py`) | [catalyst.md](./catalyst.md) | [catalyst_technical.md](./catalyst_technical.md) |
| Cluster CLI (`cluster_new.py`) | [cluster.md](./cluster.md) | [cluster_technical.md](./cluster_technical.md) |
| Dagur (`dagur.py`) | [dagur.md](./dagur.md) | [dagur_technical.md](./dagur_technical.md) |
| Daruk (`daruk.py`) | [daruk.md](./daruk.md) | [daruk_technical.md](./daruk_technical.md) |
| Gatoway (`gatoway.py`) | [gatoway.md](./gatoway.md) | [gatoway_technical.md](./gatoway_technical.md) |
| Hylia (`hylia.py`) | [hylia.md](./hylia.md) | [hylia_technical.md](./hylia_technical.md) |
| Lanayru (`lanayru.py`) | [lanayru.md](./lanayru.md) | [lanayru_technical.md](./lanayru_technical.md) |
| Logos (`logos.py`) | [logos.md](./logos.md) | [logos_technical.md](./logos_technical.md) |
| Mimir (`mimir.py` / `mcli`) | [mimir.md](./mimir.md) | [mimir_technical.md](./mimir_technical.md) |
| Mipha (`mipha.py`) | [mipha.md](./mipha.md) | [mipha_technical.md](./mipha_technical.md) |
| Spark (`spark.py` / `spark_daemon_decoded.py`) | [spark.md](./spark.md) | [spark_technical.md](./spark_technical.md) |
| Spectrum (`spectrum_server.py`) | [spectrum.md](./spectrum.md) | [spectrum_technical.md](./spectrum_technical.md) |
| Urbosa (`urbosa.py`) | [urbosa.md](./urbosa.md) | [urbosa_technical.md](./urbosa_technical.md) |
| Urbosa Bootstrap (`urbosa_bootstrap.py`) | — | [urbosa_bootstrap_technical.md](./urbosa_bootstrap_technical.md) |
| Vali (`vali.py`) | [vali.md](./vali.md) | [vali_technical.md](./vali_technical.md) |
| Valcli (`valcli.py`) | — | [valcli_technical.md](./valcli_technical.md) |
| Provision (`provision.py`) | — | [provision_technical.md](./provision_technical.md) |
| Sync Provision (`sync_provision.py`) | — | [sync_provision_technical.md](./sync_provision_technical.md) |
| Check Updates (`check_updates.py`) | — | [check_updates_technical.md](./check_updates_technical.md) |
| Create Upgrade Zip (`create_upgrade_zip.py`) | — | [create_upgrade_zip_technical.md](./create_upgrade_zip_technical.md) |
| Deploy Updates (`deploy_updates.py`) | — | [deploy_updates_technical.md](./deploy_updates_technical.md) |
| Push to GitHub (`push_to_github.py`) | — | [push_to_github_technical.md](./push_to_github_technical.md) |
| Replace Run CQL (`replace_run_cql.py`) | — | [replace_run_cql_technical.md](./replace_run_cql_technical.md) |
| Test Hylia (`test_hylia.py`) | — | [test_hylia_technical.md](./test_hylia_technical.md) |
| Valkyrie (host OS) | [valkyrie.md](./valkyrie.md) | — (no daemon code; it's the physical hypervisor host itself) |
| Odin (consensus concept) | [odin.md](./odin.md) | — |
| ZooKeeper | [zookeeper.md](./zookeeper.md) | — |
| Hydra (ScyllaDB) | [hydra.md](./hydra.md) | — |
| Aether (Linstor/DRBD) | [aether.md](./aether.md) | — |
| Slate (Traefik) | [slate.md](./slate.md) | — |
| Agahnim (Rust console proxy) | [agahnim.md](./agahnim.md) | — |

## Audit / Backlog

| Doc | What it covers |
| :--- | :--- |
| [audit_findings.md](./audit_findings.md) | Actively-maintained backlog of known architectural gaps, HA/quorum edge cases, and performance bottlenecks found by code audit. |
| [../TODO.md](../TODO.md) | Roadmap and technical-debt summary derived from this document and `add_ons_design.md`, cross-checked against current code. |

## History

| Doc | What it covers |
| :--- | :--- |
| [history/README.md](./history/README.md) | Explains what's in this subdirectory and why. |
| [history/walkthrough.md](./history/walkthrough.md) | Point-in-time record of a past comprehensive bug-fix pass (quorum/HA/performance/rolling-upgrade/Quadlet migration). |
| [history/task.md](./history/task.md) | A past task brief. |
| [history/readme_old.md](./history/readme_old.md) | A superseded project README. |
