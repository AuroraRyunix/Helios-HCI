# Third-Party Licenses

Helios-HCI is distributed under the Business Source License 1.1 (see `LICENSE`).
The components below are **not** covered by that license and retain their own terms.

## Bundled and served to browsers

| Component | Path | License |
| :-- | :-- | :-- |
| noVNC | `static/novnc/` | MPL-2.0 (`static/novnc/LICENSE.txt`) |
| pako | `static/vendor/pako/` | MIT (`static/vendor/pako/LICENSE`) |

MPL-2.0 is file-level copyleft: modifications to noVNC's own files must remain
available under MPL-2.0. It does not extend to the rest of this project.

## Bundled, compiled into a WebAssembly artifact

| Component | Path | License |
| :-- | :-- | :-- |
| spice-html5 | `static/spice-html5/` | LGPL-3.0 (`COPYING.LESSER`) |

Only `src/lz_decompress.c` is used, compiled to WebAssembly during the Spectrum
image build (`deploy_updates.py`). The resulting artifact is a work derived from
LGPL-3.0 source: its source must remain available and users must be able to
replace it. The remaining ~2.4 MB of spice-html5 JavaScript is vendored but not
referenced by any served page -- removing it would reduce this obligation to the
single compiled file.

## Runtime dependencies (not redistributed)

| Package | License |
| :-- | :-- |
| paramiko | LGPL-2.1 |
| cassandra-driver | Apache-2.0 |
| tokio, tokio-tungstenite, futures-util, serde, serde_json, tokio-rustls, rustls-pemfile | MIT / Apache-2.0 |

These are installed on the host or fetched at build time rather than
redistributed in this repository.
