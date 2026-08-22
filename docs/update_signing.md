# Update Signing (`helios_sig.py`)

Detached Ed25519 signatures over the update chain, verified against a public key pinned
on every node at provision time.

## The gap this closes

The update path used to certify itself at every step:

| Step | What was checked | What it proved |
| --- | --- | --- |
| `check-updates` reads `/api/v1/releases/latest` | nothing | — |
| Spectrum downloads the package | zip digest vs. the `sha256` **from that same response** | the download was not corrupted |
| Hylia extracts the package | each file vs. the `sha256` **declared in `manifest.json` beside it** | the zip was not corrupted |

Every digest in that table is supplied by whoever supplied the thing it describes.
Anyone able to answer for `updates-helios.zerotwo.cloud`, or to write into the bucket
behind it, publishes a package and its matching digests in one motion; the rest of the
chain then confirms the substitution faithfully, all the way onto `root`'s `PATH` on
every node in the cluster.

Nothing about those digests was useless — they still catch a truncated transfer, and
they still bound the deployment path so a component cannot be swapped between download
and install. They were simply never integrity in the sense that mattered. The signature
is what makes them worth checking.

## What is signed

Two documents, both produced by `create_upgrade_zip.py` on the release workstation.

**1. The package manifest.** `manifest.sig` sits beside `manifest.json` inside
`upgrade_<version>.zip` and covers the exact bytes of `manifest.json`. Since every
component digest lives in that manifest, one signature transitively covers every file
in the package, plus the install path each one claims.

**2. The release document.** `upgrade_<version>.release.json` is what the update server
serves as the body of `/api/v1/releases/latest`:

```json
{
  "signed": "{\"components\":{...},\"download_url\":\"https://...\",\"sha256\":\"...\",\"latest_version\":\"1.2.3-stable\",...}",
  "signature": { "algorithm": "ed25519", "key_id": "a4ca42072de6a3bc", "signature": "<base64>" }
}
```

The signed half travels as **text**, not as a nested object, and `check-updates` reads
`download_url`, `sha256`, `latest_version`, `size`, `changelog` and `components` out of
the parsed signed text only. Fields sitting next to it in the response are ignored
entirely: trusting one because a signature elsewhere in the body verified would be the
original bug with a signature stapled on. Carrying the signed text verbatim also means
no JSON encoder ever sits between the bytes that were signed and the bytes that are
checked.

Note that the package digest in the release document is computed on the release
workstation, by the process holding the signing key. A digest the update *host*
computes for a file the update *host* serves asserts nothing.

## Why Ed25519 and not an HMAC

Both are buildable from what a node already has. The difference is who can forge.

Every node in every cluster must be able to *verify* a release. With a shared secret,
every node that can verify can also sign: one stolen disk image, one copied `/etc`, one
restored backup, and the attacker mints updates for the entire fleet — undetectably,
because with a MAC "verified" and "forged by a verifier" are the same event. The threat
model here is exactly "a node, or a node's storage, is compromised", so a symmetric
scheme puts the forging material in the hands of the attacker it is meant to stop.

With a keypair the nodes hold nothing worth stealing. The private half never leaves the
release workstation. This is the property the TODO entry asked for: *compromise of a
node cannot forge updates.*

There is no `cryptography` module on these hosts and the daemons are stdlib-only by
design, so the primitive comes from `openssl` as a subprocess — already a hard
dependency, already installed by `provision.py`, already trusted to hold the mTLS CA.
`pkeyutl -rawin` needs OpenSSL 1.1.1 or newer; EL8 and later ship it.

```bash
# What verification is, underneath:
openssl pkeyutl -verify -pubin -inkey /etc/hci/keys/release_ed25519.pub \
    -rawin -in manifest.json -sigfile manifest.sig
```

## The pinned key

```
/etc/hci/keys/release_ed25519.pub      root:root  0644   (directory 0755)
```

Written by `provision.py` when the node is built, before it has ever spoken to the
update server. **The pinning is the entire mechanism.** A signature checked against a key
that arrived with the payload proves only that the sender owns a key, which is precisely
as much as the old `sha256` proved; the key has to arrive once, out of band, over the
channel that installs the operating system, and must never be refreshed from the network.

It lives under `/etc/hci` because that tree is already bind-mounted read-only into the
Spectrum container (`Volume=/etc/hci:/etc/hci:ro`), so the host daemons and the console
verify against one file rather than two copies that can drift.

World-readable is correct: it is a public key, and nothing is protected by hiding it.

### Generating and holding the release keypair

```bash
mkdir -p ~/.helios
openssl genpkey -algorithm ed25519 -out ~/.helios/release_ed25519.key
chmod 600 ~/.helios/release_ed25519.key
openssl pkey -in ~/.helios/release_ed25519.key -pubout -out ~/.helios/release_ed25519.pub
```

The private half stays on the release workstation — the machine that runs
`create_upgrade_zip.py` — and on no cluster node, ever. `HELIOS_RELEASE_SIGNING_KEY`
overrides the default path. `deploy_updates.py` refuses to distribute a file containing
`PRIVATE KEY`, because pointing it at the wrong half would copy the one secret the whole
scheme depends on onto every node in the fleet.

### Rotation

Pin the new public key on every node **first**, then start signing with the new private
key. Packages signed by a key a node does not pin are refused; the refusal names both
key fingerprints, precisely so that a rotation performed in the wrong order is
distinguishable from an attack:

```
Manifest signature verification failed against the key pinned at
/etc/hci/keys/release_ed25519.pub: ... The manifest signature names key
'9f2a1c0be4d75311' but this node pins key 'a4ca42072de6a3bc'. If the release key was
rotated, the new public key has to be pinned on every node before packages signed with
it will install.
```

The `key_id` is the first 16 hex characters of the SHA-256 of the key's DER-encoded
public half. It is diagnostic only and never contributes to a trust decision — the
decision is made solely by whether the signature verifies against the pinned key.

## Failure behaviour

Everything fails closed, and every refusal says why.

| Condition | Result |
| --- | --- |
| Valid signature from the pinned key | accepted |
| Any byte of the signed payload altered | refused |
| Signature made with any other key | refused, naming both key fingerprints |
| Signature absent | refused — unless the transition override is set (below) |
| Signature malformed (bad base64, wrong length, unknown algorithm, bad `key_id`) | refused with a reason, never a traceback |
| No key pinned on this node | refused, naming the path `provision.py` should have written |
| Pinned key is not Ed25519, or unreadable | refused, identified as a provisioning fault |
| `openssl` missing or too old | refused |

An unrecognised `algorithm` in the envelope is refused rather than guessed at. Talking a
verifier into a weaker primitive is how signature checks get defeated without anyone
breaking a signature.

## The transition

Clusters that predate this have an unsigned release server and unsigned packages. There
is one escape hatch, and it is deliberately awkward:

```
HELIOS_ALLOW_UNSIGNED_UPDATES=i-accept-unsigned-updates
```

- It is **not** a boolean. `1`, `true` and `yes` do nothing. Turning off signature
  checking should not be something anyone does by reflex, by copying another tool's
  convention, or by a value inherited from an unrelated environment.
- It only ever covers a release or package that carries **no signature at all**. A
  signature that is present and wrong is an attack in any migration state, and no
  environment variable makes it acceptable.
- When it is used, `check-updates` prints a banner and writes the reason into
  `hydra.lcm_update_state.error_msg`, which Spectrum surfaces as the error on the LCM
  page. An accepted-but-unverified release that looks identical to a verified one is how
  a temporary migration setting becomes permanent.

The intended path is to re-sign: build the release with `create_upgrade_zip.py` against
a signing key, publish the emitted `upgrade_<version>.release.json` as the body of
`/api/v1/releases/latest`, and pin the public half on the nodes. Until then, a node with
no pinned key simply reports an error on the LCM page and offers no update — no cluster
stops working because a signature is missing.

## API

```python
import helios_sig

# Verification (nodes)
helios_sig.verify_signed_document(response_dict)      # -> (release fields, key_id)
helios_sig.verify_package_manifest(extract_dir)       # -> key_id
helios_sig.verify_detached_signature(payload_bytes, envelope)
helios_sig.unsigned_updates_permitted()               # the transition override

# Signing (release workstation only)
helios_sig.sign_bytes(payload_bytes)                  # -> envelope dict
helios_sig.build_signed_document(payload_obj)         # -> {"signed": ..., "signature": ...}
helios_sig.sign_manifest_file(manifest_path)          # writes manifest.sig
```

`SignatureError` means refused. `SignatureMissing` (a subclass) means nothing was
presented to verify, and is the only case the transition override may act on; a caller
that does not care about the distinction catches `SignatureError` and still fails closed.

## Tests

`test_update_signature.py` — real openssl keys, real signatures, nothing stubbed:

```bash
python -m unittest test_update_signature
```
