#!/usr/bin/env python3
"""Detached Ed25519 signatures for the update chain.

Why this exists: until now the update path certified itself. The release endpoint
returned a `download_url` and the `sha256` of whatever it pointed at in the same
JSON body, and `manifest.json` declared the digests of the files shipped beside it
in the same zip. Both checks catch a truncated download. Neither catches a hostile
one -- whoever can answer for the update host, or write to the bucket behind it,
publishes a package and the matching digests in one motion, and every node in the
fleet installs it as root.

The anchor that fixes this is a public key written to
/etc/hci/keys/release_ed25519.pub at provision time, before the node has ever
spoken to the update server. The pinning is the whole point: a signature checked
against a key that travelled with the payload proves only that the sender owns a
key, which is exactly as much as the old sha256 proved. This key arrives once, out
of band, over the same channel that installs the operating system, and is never
refreshed from the network.

Ed25519 rather than an HMAC because the verifiers are the nodes. Every node of
every cluster has to be able to check a release, so a shared secret would place
forging material on every machine we ship: one stolen disk image, one copied
/etc, one backup, and the attacker signs updates for the entire fleet -- and can
do it undetectably, because with a MAC "verified" and "forged by a verifier" are
the same event. With a keypair the nodes hold nothing worth stealing; the private
half never leaves the release workstation that runs create_upgrade_zip.py.

There is no crypto library to reach for (the daemons are stdlib-only on purpose),
so the primitive comes from openssl, which provision.py already installs and
already trusts to hold the mTLS CA. `pkeyutl -rawin` requires OpenSSL 1.1.1 or
newer; EL8 and later ship it.
"""
__build__ = "1.2.3-stable"

import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile

SIGNATURE_ALGORITHM = "ed25519"
# An Ed25519 signature is fixed width. Checking the length before openssl sees it
# turns a truncated or padded blob into a sentence instead of a provider error.
ED25519_SIGNATURE_BYTES = 64

# Pinned by provision.py. Lives under /etc/hci because that whole tree is already
# bind-mounted read-only into the Spectrum container, so the host daemons and the
# console verify against the same file without a second copy to keep in step.
PINNED_PUBLIC_KEY_PATH = "/etc/hci/keys/release_ed25519.pub"

MANIFEST_FILENAME = "manifest.json"
MANIFEST_SIGNATURE_FILENAME = "manifest.sig"

# The one transition allowance, for a cluster whose release server has not been
# re-signed yet. It is deliberately not a boolean: "1", "true" and "yes" do not
# work, because turning off signature checking should not be something anyone can
# do by reflex, by copying another tool's convention, or by a stray value inherited
# from an unrelated environment. It only ever covers a release that carries no
# signature at all -- a signature that is present and wrong is an attack, and no
# environment variable makes that acceptable.
UNSIGNED_OVERRIDE_ENV = "HELIOS_ALLOW_UNSIGNED_UPDATES"
UNSIGNED_OVERRIDE_VALUE = "i-accept-unsigned-updates"

# Release workstation only. Nothing on a cluster node reads this.
PRIVATE_KEY_ENV = "HELIOS_RELEASE_SIGNING_KEY"
DEFAULT_PRIVATE_KEY_PATH = os.path.join(os.path.expanduser("~"), ".helios", "release_ed25519.key")

# key_id is echoed into log lines, CQL statements and the console, so it is held to
# a character set that cannot do anything in any of them.
_KEY_ID_RE = re.compile(r'^[A-Za-z0-9._:-]{1,64}$')


class SignatureError(Exception):
    """Verification was refused, and the message says why.

    Every path that raises this must leave the caller with no usable release. There
    is no severity here that a caller is meant to weigh up.
    """


class SignatureMissing(SignatureError):
    """Nothing was presented to verify.

    Split out from SignatureError so the one narrow transition allowance can be
    written against 'unsigned' without ever widening to 'signed wrongly'. A caller
    that catches SignatureError catches both, which is the safe default.
    """


def _openssl(args, capture_binary=False):
    """Run openssl with an argument list, never a shell string.

    stdin is closed: a passphrase-protected signing key otherwise prompts, and a
    build or an update check that blocks forever on an invisible prompt is a worse
    failure than the error it replaces.
    """
    try:
        proc = subprocess.run(
            ["openssl"] + list(args),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        raise SignatureError(
            "openssl is not installed or not on PATH, so update signatures cannot be "
            "verified. provision.py installs it on every node; fix the node rather than "
            "disabling verification."
        )
    except OSError as exc:
        raise SignatureError(f"Could not run openssl: {exc}")

    stderr = proc.stderr.decode("utf-8", errors="replace").strip()
    if capture_binary:
        return proc.returncode, proc.stdout, stderr
    return proc.returncode, proc.stdout.decode("utf-8", errors="replace").strip(), stderr


def key_fingerprint(key_path, private=False):
    """Short stable identifier for a key: sha256 over its DER-encoded public half.

    Purely diagnostic -- it is what turns "signature verification failed" into "this
    package was signed by a key you do not pin", which is what a key rotation looks
    like and is otherwise indistinguishable from tampering. It never contributes to
    a trust decision, so it returns "" rather than raising when it cannot be read.
    """
    if not key_path or not os.path.exists(key_path):
        return ""
    if private:
        args = ["pkey", "-in", key_path, "-pubout", "-outform", "DER"]
    else:
        args = ["pkey", "-pubin", "-in", key_path, "-pubout", "-outform", "DER"]
    try:
        rc, der, _ = _openssl(args, capture_binary=True)
    except SignatureError:
        return ""
    if rc != 0 or not der:
        return ""
    return hashlib.sha256(der).hexdigest()[:16]


def require_pinned_key(public_key_path=PINNED_PUBLIC_KEY_PATH):
    """Confirm the pinned key exists and is the algorithm we think it is.

    An operator who pins an RSA key by mistake would otherwise see every release
    rejected with an opaque provider error, which reads exactly like an attack and
    invites someone to reach for the escape hatch to make it stop.
    """
    if not public_key_path or not os.path.exists(public_key_path):
        raise SignatureError(
            f"No release public key is pinned at {public_key_path}, so no update can be "
            "verified. provision.py writes this file when the node is built; a node "
            "provisioned before signing existed has to have it installed before it can "
            "accept a release."
        )
    rc, text, err = _openssl(["pkey", "-pubin", "-in", public_key_path, "-noout", "-text"])
    if rc != 0:
        raise SignatureError(
            f"The pinned release key at {public_key_path} is not a readable public key: "
            f"{err or 'openssl could not parse it'}"
        )
    if "ED25519" not in text.upper():
        raise SignatureError(
            f"The pinned release key at {public_key_path} is not an Ed25519 public key. "
            "Re-pin the correct key; this is a provisioning fault, not a bad package."
        )
    return public_key_path


def parse_signature_envelope(raw, source="signature"):
    """Structurally validate a detached-signature envelope.

    Everything here arrives from the update server or from inside a downloaded
    package, so malformed input has to come back as a refusal with a reason -- not
    as a traceback out of json, base64 or openssl. Returns (key_id, signature bytes).
    """
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = bytes(raw).decode("utf-8")
        except UnicodeDecodeError:
            raise SignatureError(f"The {source} is not valid UTF-8.")

    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            raise SignatureMissing(f"The {source} is empty.")
        try:
            raw = json.loads(text)
        except ValueError as exc:
            raise SignatureError(f"The {source} is not valid JSON: {exc}")

    if raw is None:
        raise SignatureMissing(f"No {source} was supplied.")
    if not isinstance(raw, dict):
        raise SignatureError(f"The {source} is not a JSON object.")

    algorithm = raw.get("algorithm")
    if not isinstance(algorithm, str) or algorithm.strip().lower() != SIGNATURE_ALGORITHM:
        raise SignatureError(
            f"The {source} declares algorithm {algorithm!r}; only {SIGNATURE_ALGORITHM!r} is "
            "accepted. An unrecognised algorithm is refused rather than guessed at: talking a "
            "verifier into a weaker primitive is how signature checks get defeated without "
            "ever breaking one."
        )

    encoded = raw.get("signature")
    if not isinstance(encoded, str) or not encoded.strip():
        raise SignatureError(f"The {source} carries no signature value.")
    try:
        signature = base64.b64decode(encoded.strip(), validate=True)
    except ValueError as exc:
        raise SignatureError(f"The {source} is not valid base64: {exc}")
    if len(signature) != ED25519_SIGNATURE_BYTES:
        raise SignatureError(
            f"The {source} decodes to {len(signature)} bytes; an Ed25519 signature is exactly "
            f"{ED25519_SIGNATURE_BYTES}."
        )

    key_id = raw.get("key_id") or ""
    if not isinstance(key_id, str) or (key_id and not _KEY_ID_RE.match(key_id)):
        raise SignatureError(
            f"The {source} carries a malformed key_id; it is reported to operators verbatim, "
            "so only [A-Za-z0-9._:-] is accepted."
        )
    return key_id, signature


def verify_detached_signature(payload, envelope, public_key_path=PINNED_PUBLIC_KEY_PATH, source="signature"):
    """Verify `envelope` over the exact bytes of `payload` against the pinned key.

    `payload` is bytes and never an object: the signature covers a byte string, and
    re-serialising a parsed structure to check it would put a JSON encoder between
    what was signed and what is verified. Returns the envelope's key_id.
    """
    if not isinstance(payload, (bytes, bytearray)):
        raise SignatureError(
            "A signature covers exact bytes, so the payload must be bytes rather than a "
            "re-serialisation of a parsed object."
        )
    key_id, signature = parse_signature_envelope(envelope, source=source)
    require_pinned_key(public_key_path)

    # openssl wants both halves on disk, and /tmp on a node is world-writable. A
    # private directory of our own closes the window in which anything else could
    # swap the bytes between writing them and openssl reading them back.
    work_dir = tempfile.mkdtemp(prefix="helios-sig-")
    try:
        payload_path = os.path.join(work_dir, "payload.bin")
        signature_path = os.path.join(work_dir, "payload.sig")
        with open(payload_path, "wb") as handle:
            handle.write(bytes(payload))
        with open(signature_path, "wb") as handle:
            handle.write(signature)
        rc, out, err = _openssl([
            "pkeyutl", "-verify",
            "-pubin", "-inkey", public_key_path,
            "-rawin", "-in", payload_path,
            "-sigfile", signature_path,
        ])
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

    # openssl prints "Signature Verified Successfully" and exits 0. The text check is
    # belt and braces around the exit status, not a substitute for it.
    if rc != 0 or "failure" in f"{out} {err}".lower():
        detail = err or out or f"openssl exited {rc}"
        pinned_id = key_fingerprint(public_key_path)
        if key_id and pinned_id and key_id != pinned_id:
            detail += (
                f" The {source} names key '{key_id}' but this node pins key '{pinned_id}'. "
                "If the release key was rotated, the new public key has to be pinned on every "
                "node before packages signed with it will install."
            )
        raise SignatureError(
            f"{source.capitalize()} verification failed against the key pinned at "
            f"{public_key_path}: {detail}"
        )
    return key_id


def verify_signed_document(document, public_key_path=PINNED_PUBLIC_KEY_PATH):
    """Verify a {"signed": <text>, "signature": {...}} response and return what was signed.

    The signed half travels as text rather than as a nested object, and the caller
    must read its release fields out of the returned payload rather than from the
    response around it. That is the entire fix: the old code took download_url and
    its sha256 from the same body a hostile update host would have written, so the
    digest only ever proved the download had not been corrupted in transit. Trusting
    an unsigned field because a signature elsewhere in the response verified would
    reintroduce that bug wearing a signature.

    Returns (release payload dict, key_id).
    """
    if not isinstance(document, dict):
        raise SignatureError("The release response is not a JSON object.")

    signature = document.get("signature")
    if signature is None:
        raise SignatureMissing("The release response carries no 'signature' field.")

    signed = document.get("signed")
    if isinstance(signed, (dict, list)):
        raise SignatureError(
            "The release response sent 'signed' as an object. It has to be the exact signed "
            "text, byte for byte, or there is nothing the signature can be checked against."
        )
    if not isinstance(signed, str) or not signed.strip():
        raise SignatureError("The release response is signed but carries no 'signed' payload.")

    key_id = verify_detached_signature(
        signed.encode("utf-8"), signature, public_key_path, source="release signature"
    )

    try:
        payload = json.loads(signed)
    except ValueError as exc:
        raise SignatureError(f"The signed release payload is not valid JSON: {exc}")
    if not isinstance(payload, dict):
        raise SignatureError("The signed release payload is not a JSON object.")
    return payload, key_id


def verify_package_manifest(extract_dir, public_key_path=PINNED_PUBLIC_KEY_PATH):
    """Verify manifest.sig over the manifest.json bytes in an extracted package.

    The per-component digests inside the manifest only ever bound the package to
    itself; this binds the manifest to the release key, which is what makes those
    digests worth checking. Returns the key_id that signed it.
    """
    manifest_path = os.path.join(extract_dir, MANIFEST_FILENAME)
    signature_path = os.path.join(extract_dir, MANIFEST_SIGNATURE_FILENAME)

    if not os.path.exists(manifest_path):
        raise SignatureError(
            f"{MANIFEST_FILENAME} is not present in the update package, so there is nothing "
            "to verify."
        )
    if not os.path.exists(signature_path):
        raise SignatureMissing(
            f"The update package carries no {MANIFEST_SIGNATURE_FILENAME}, so its manifest is "
            "unsigned. Rebuild it with create_upgrade_zip.py against the release signing key."
        )

    with open(manifest_path, "rb") as handle:
        manifest_bytes = handle.read()
    with open(signature_path, "rb") as handle:
        envelope = handle.read()

    return verify_detached_signature(
        manifest_bytes, envelope, public_key_path, source="manifest signature"
    )


def unsigned_updates_permitted():
    """True only when an operator set the escape hatch to its exact documented value."""
    return os.environ.get(UNSIGNED_OVERRIDE_ENV, "").strip() == UNSIGNED_OVERRIDE_VALUE


def unsigned_override_hint():
    """The sentence to append to an unsigned refusal, so the refusal is actionable."""
    return (
        f"If this cluster is mid-migration to signed releases, set "
        f"{UNSIGNED_OVERRIDE_ENV}={UNSIGNED_OVERRIDE_VALUE} in the unit environment to accept "
        "an unsigned release; a badly-signed one is never accepted."
    )


def default_private_key_path():
    return os.environ.get(PRIVATE_KEY_ENV, "").strip() or DEFAULT_PRIVATE_KEY_PATH


def public_key_path_for(private_key_path):
    """Conventional location of the public half beside a signing key."""
    root, _ = os.path.splitext(private_key_path)
    return root + ".pub"


def keygen_instructions(private_key_path=None):
    """The exact commands that produce a release keypair, quoted in build failures."""
    private_key_path = private_key_path or default_private_key_path()
    public_path = public_key_path_for(private_key_path)
    return (
        f"  mkdir -p {os.path.dirname(private_key_path) or '.'}\n"
        f"  openssl genpkey -algorithm ed25519 -out {private_key_path}\n"
        f"  chmod 600 {private_key_path}\n"
        f"  openssl pkey -in {private_key_path} -pubout -out {public_path}\n"
        f"Keep the private half on the release workstation only, and pin {os.path.basename(public_path)} "
        f"on every node at {PINNED_PUBLIC_KEY_PATH}."
    )


def sign_bytes(payload, private_key_path=None):
    """Produce a detached signature envelope over exact bytes.

    Release workstation only. Nothing on a cluster node has the private half, and
    nothing on a cluster node should ever call this.
    """
    if not isinstance(payload, (bytes, bytearray)):
        raise SignatureError("sign_bytes signs exact bytes; encode the payload first.")
    private_key_path = private_key_path or default_private_key_path()
    if not os.path.exists(private_key_path):
        raise SignatureError(
            f"No release signing key at {private_key_path}. Generate one with:\n"
            f"{keygen_instructions(private_key_path)}"
        )

    work_dir = tempfile.mkdtemp(prefix="helios-sign-")
    try:
        payload_path = os.path.join(work_dir, "payload.bin")
        signature_path = os.path.join(work_dir, "payload.sig")
        with open(payload_path, "wb") as handle:
            handle.write(bytes(payload))
        rc, out, err = _openssl([
            "pkeyutl", "-sign",
            "-inkey", private_key_path,
            "-rawin", "-in", payload_path,
            "-out", signature_path,
        ])
        if rc != 0:
            raise SignatureError(f"openssl could not sign with {private_key_path}: {err or out}")
        with open(signature_path, "rb") as handle:
            signature = handle.read()
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

    if len(signature) != ED25519_SIGNATURE_BYTES:
        raise SignatureError(
            f"{private_key_path} produced a {len(signature)}-byte signature; it is not an "
            "Ed25519 key."
        )
    return {
        "algorithm": SIGNATURE_ALGORITHM,
        "key_id": key_fingerprint(private_key_path, private=True),
        "signature": base64.b64encode(signature).decode("ascii"),
    }


def build_signed_document(payload_obj, private_key_path=None):
    """Serialise `payload_obj` once and sign that exact text.

    The text is carried alongside the signature instead of being reconstructed by
    the verifier, so no JSON encoder difference between the build host and a node
    can ever turn a good signature into a bad one.
    """
    signed_text = json.dumps(payload_obj, sort_keys=True, separators=(",", ":"))
    return {
        "signed": signed_text,
        "signature": sign_bytes(signed_text.encode("utf-8"), private_key_path),
    }


def sign_manifest_file(manifest_path, private_key_path=None):
    """Sign a manifest.json on disk and write manifest.sig beside it. Returns its path."""
    with open(manifest_path, "rb") as handle:
        manifest_bytes = handle.read()
    envelope = sign_bytes(manifest_bytes, private_key_path)
    signature_path = os.path.join(os.path.dirname(manifest_path), MANIFEST_SIGNATURE_FILENAME)
    with open(signature_path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(envelope, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return signature_path
