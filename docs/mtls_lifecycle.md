# mTLS Certificate Lifecycle

Every inter-node call in this cluster runs over mutual TLS to `spark-daemon` on port
`9099`. The certificates that make those calls possible were minted once, by
`provision.py`, at cluster creation. Nothing renewed them, nothing watched them, and
nothing verified who was on the other end beyond "signed by our CA".

This document covers what exists, how to watch it, and how to replace it without
re-provisioning a cluster that already holds data.

---

## 1. What exists on a node

| Path | Contents | Read by |
| :--- | :--- | :--- |
| `/var/lib/hci/certs_staging/` | `ca.key`, `ca.crt`, `ca.srl`, and the per-node material | **First host only.** The CA private key exists nowhere else in the cluster. |
| `/etc/hci/spark/certs/` | `ca.crt`, `node.crt`, `node.key` | `spark-daemon` (as its server identity on 9099, and as its own client identity when it dials peers), `mipha` |
| `/root/.certs/` | `ca.crt`, `client.crt`, `client.key` | Every other daemon and CLI that dials 9099: `mimir`, `dagur`, `catalyst`, `hylia`, `cluster`, `allssh`, `mcli`, `valcli` |
| `/etc/hci/spectrum/certs/` | `server.crt`, `server.key` | The web console on 8443 and Slate/Traefik. Self-signed, **not** issued by the cluster CA, separate lifecycle. |

The CA is `CN=HCI-Root-CA`. It signs exactly two kinds of leaf:

* **`client.crt`** — `CN=HCI-Client`, no SAN, **identical on every node**. It is an identity
  that says "a member of this cluster", not "this particular node".
* **`node-<ip>.crt`** — `CN=<node ip>`, `subjectAltName = IP:<node ip>`, installed as
  `/etc/hci/spark/certs/node.crt` on that one node.

All of it is RSA-2048 and, as provisioned, valid for 3650 days from cluster creation.

### The two defects this document exists for

**Nothing renews and nothing warns.** Ten years is long enough that no test cluster ever
reached the date, so the failure was invisible: on one particular day, every
`run_remote_spark` call on every node starts returning a TLS error simultaneously, the
cluster becomes unorchestratable, and the only documented recovery is re-provisioning.

**The CA and the leaves expire on the same day.** `provision.py` issues both at 3650 days
in the same shell script, so the CA has no headroom to sign a replacement leaf near the
end of its own life. A leaf renewed at year 9 would be issued with a `notAfter` past the
CA's own, look fine, and fail cluster-wide the moment the CA lapsed. `impa` refuses to
sign a leaf that outlives its issuer and tells you to rotate the CA instead.

---

## 2. Watching it

### From the health console

`mimir` surveys `/etc/hci/spark/certs` and `/root/.certs` on **every node** every 15
minutes and publishes the result to `hydra.mimir_results` under the check name
`mtls_cert_expiration` (category `security.mtls.certs`) that the console already renders.
This is deliberately not gated on the ZooKeeper leader election the rest of Mimir's
scheduling uses: the certificates are per-node, and the day they lapse is the day the
leader-only fan-out stops being able to reach anything.

* `PASS` — every certificate has more than 30 days left
* `WARN` — something expires within 30 days, or an expiry date could not be read at all
* `FAIL` — something expires within 7 days, or has already expired

A date that cannot be parsed is `WARN`, never `PASS`. `mcli health_checks` reports the
same numbers through `mtls_cert_expiry_warning`.

### From a shell

```bash
impa status                # this node
impa status --all-nodes    # every host in cluster.json, over SSH
impa status --json         # machine-readable; exit code is 0 only on PASS
```

`impa status` also answers the question expiry alone does not: **what is this certificate
addressable as?** For each node certificate it prints the routes that certificate can and
cannot be verified for.

```
  PASS   /etc/hci/spark/certs/node.crt            3647d left       Aug 14 20:55:31 2036 GMT
           CN=10.10.102.41  issuer=HCI-Root-CA  SAN=10.10.102.41
           not verifiable for: 127.0.0.1, Valkyrie-997A49, 10.10.102.45
```

That third line is the second defect, printed. See §5.

---

## 3. Renewing

`impa` runs **on the first host in `cluster.json`** — the only node that has `ca.key` —
and drives its peers over **SSH**, not over mTLS. That is deliberate: renewal has to work
in the state that makes it necessary, and once the certificates have expired the mTLS API
on 9099 is precisely what is broken. `provision.py` already seeds root SSH keys and
`known_hosts` across the fleet for live migration; this reuses that channel.

```bash
impa plan                      # print the ordered steps, change nothing
impa plan --rotate-ca
impa renew --days 825 --dry-run
impa renew --days 825 --yes
impa renew --rotate-ca --yes
impa rollback --backup 20260820-092958
```

### Blast radius

**One service restart per node, and nothing else.** Every mTLS *client* in this tree
builds its `SSLContext` inside the call that uses it — `run_remote_spark` and
`run_mtls_spark_api` call `ssl.create_default_context` per request — so a renewed
`client.crt` is picked up by the next outbound call with no restart at all.

`spark-daemon` is the one exception: its server context is built once in `main()` and
wraps the listening socket, so it must be restarted before it will present a new
`node.crt`. `impa` restarts one node at a time and verifies it before moving on, so at no
point is more than one node's 9099 listener down.

Existing TLS sessions are unaffected; they finish on the certificate they started with.

### Order of operations — leaf renewal (the common case)

The CA has not changed, so every peer already trusts the signer of the new certificate.
There is no trust-distribution step and no ordering constraint between nodes.

1. **Preflight.** Confirm `ca.key` is present and readable here, that every host in
   `cluster.json` answers over SSH, and that the CA outlives the requested leaf validity.
   Nothing is written if any of these fail.
2. **Back up.** `tar` `/etc/hci/spark/certs` and `/root/.certs` on **every** node into
   `/var/lib/hci/cert-backups/<timestamp>.tgz`, before anything is changed anywhere. A
   backup failure aborts the whole run.
3. **Mint.** One shared `client.key`/`client.crt`, and one `node-<ip>.key`/`node-<ip>.crt`
   per host, all signed by the existing CA, into a private staging directory.
4. **Per node, one node at a time:**
   1. Write `node.crt`, `node.key`, `client.crt`, `client.key` — each to a temporary name
      at its final mode, then renamed into place. A daemon restarting mid-write must never
      be able to load half a key, or load a key that was briefly world-readable.
   2. `systemctl restart spark-daemon`.
   3. Handshake to `<ip>:9099` **with hostname verification on**, from the coordinating
      node. This proves the daemon presents a certificate that chains to the CA *and* is
      issued for the address it was dialled at. A chain-only check would pass while the
      node presented some other node's certificate.
   4. Only on success, move to the next node. On failure, stop: the remaining nodes are
      untouched and step 2's archive restores this one.

### Order of operations — CA rotation

This is the case with a real ordering constraint, and it is one sentence:

> **A node must never present a certificate signed by a CA that some peer does not yet
> trust.**

Satisfying it takes three passes over the fleet. Collapsing any two of them strands the
cluster.

**Phase 1 — mint the new CA.** Key and self-signed certificate, staged locally. Nothing is
distributed yet.

**Phase 2 — trust, every node.** Write `ca.crt` = *old CA* concatenated with *new CA* into
both `/etc/hci/spark/certs/` and `/root/.certs/`, and restart `spark-daemon`. OpenSSL and
Python both accept a multi-certificate PEM as a trust store, so after this pass every node
accepts certificates from **either** signer, while everything still presents old-CA
certificates. Nothing has changed identity yet; this pass is pure trust widening and is
safe to stop after.

**Phase 3 — mint the leaves** from the new CA.

**Phase 4 — present, one node at a time.** Install the new-CA leaves and restart, exactly
as in the leaf-renewal case. This works in both directions mid-pass: a node presenting a
new-CA certificate is accepted by peers still on old-CA leaves, because phase 2 gave them
the new CA, and it accepts theirs because it still holds the old CA.

**Phase 5 — prune, every node.** Only once *every* node presents a new-CA leaf, write
`ca.crt` = *new CA only* and restart. The old CA is now untrusted and the rotation is
complete. Promote the new key and certificate over `ca.key`/`ca.crt` in the staging
directory.

Doing phase 5 before phase 4 completes rejects any node still on an old-CA leaf. Doing
phase 4 before phase 2 completes offers a signature peers do not recognise. Both leave a
partitioned cluster where some nodes can orchestrate and others cannot, and neither is
recoverable over mTLS — which is why `renewal_plan()` builds the order and
`plan_violates_ordering()` asserts it before a single byte is written. `impa renew`
refuses to run a plan that fails that assertion.

### The ingress certificate is not part of this

`/etc/hci/spectrum/certs/server.{crt,key}` is the web console's own certificate. It is
self-signed, shared by every node, generated at the same 3650 days, and **not** issued by
the cluster CA, so `impa renew` does not touch it — but it expires on the same day as
everything else and takes the console with it. `impa status` reports it so the date is
visible, and `mcli health_checks` covers it as `ingress_cert_expiration`. Replacing it is
one command plus a restart, and it must be the *same* file on every node:

```bash
openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout /etc/hci/spectrum/certs/server.key \
    -out /etc/hci/spectrum/certs/server.crt \
    -days 3650 -subj '/CN=Spectrum'
chmod 600 /etc/hci/spectrum/certs/server.key
# copy both files to every other node, then on each:
systemctl restart spectrum slate
```

### Rollback

Every run backs up both certificate directories on every node first and prints the
timestamp. `impa rollback --backup <timestamp>` untars it and restarts `spark-daemon`.
A rollback after a completed CA rotation restores the old CA *and* the old leaves
together, which is consistent — the two must always move back as a pair.

---

## 4. What `provision.py` must change

`provision.py` is the source of truth for a *new* cluster; `impa` is the source of truth
for renewing an existing one. They must mint the same shape of certificate. Today they do
not, and these are the differences. The block is the `cert_gen_sh` heredoc in Phase 6 of
`main()` — find it with `grep -n "openssl genrsa -out ca.key" provision.py`; line numbers
move.

### Required — the node SAN

`cert_gen_sh` is a Python f-string, so `VIP` (module global, set around line 500, long
before certificate generation) interpolates directly, while `$ip` is the shell loop
variable. The `Valkyrie-XXXXXX` hostname is derived from the IP and therefore has to be
recomputed inside the loop; `printf %s | md5sum | cut -c1-6 | tr a-z A-Z` reproduces
`hashlib.md5(ip.encode()).hexdigest()[:6].upper()` exactly (verified against a live node:
`10.10.102.41` gives `Valkyrie-997A49` both ways).

The node loop currently reads:

```bash
for ip in {" ".join(HOSTS)}; do
  cat <<EOF > "node-$ip.cnf"
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no
[req_distinguished_name]
CN = $ip
[v3_req]
subjectAltName = IP:$ip
EOF
```

It must become:

```bash
for ip in {" ".join(HOSTS)}; do
  h=$(printf %s "$ip" | md5sum | cut -c1-6 | tr a-z A-Z)
  cat <<EOF > "node-$ip.cnf"
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no
[req_distinguished_name]
CN = $ip
[v3_req]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = IP:$ip,IP:127.0.0.1,DNS:localhost,DNS:Valkyrie-$h{f",IP:{VIP}" if VIP else ""}
EOF
```

The signing command below the heredoc is unchanged — it already passes
`-extensions v3_req -extfile "node-$ip.cnf"`.

Note the f-string details: the whole script is already an f-string (`{" ".join(HOSTS)}`
proves it), so the VIP term is appended conditionally by an inline expression rather than
by writing an empty `IP:` component — an empty `subjectAltName` element makes
`openssl req` fail outright, which is why a cluster created without a VIP must simply
omit the term. Nothing else in the added lines contains a brace.

Every entry is a route a daemon actually dials, and every one that is missing is a route
where hostname verification has to be switched off:

* `IP:$ip` — peer-to-peer orchestration. Already present.
* `IP:127.0.0.1` and `DNS:localhost` — every daemon dials `127.0.0.1:9099` to reach its
  own host. `spark-daemon` binds `0.0.0.0`, so the clients now rewrite loopback to the
  node's own address as a workaround; adding this makes the workaround unnecessary.
* `DNS:Valkyrie-$h` — `cluster.json` records a hostname per node, and libvirt migration
  and `known_hosts` seeding both use it.
* `IP:{VIP}` — `cluster_new.py` reaches the leader through the floating VIP. Without this
  entry the VIP can only be chain-verified (see §5).

### Required — CA validity

```
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt -subj "/CN=HCI-Root-CA"
```

must become:

```
openssl req -new -x509 -days 7300 -key ca.key -out ca.crt -subj "/CN=HCI-Root-CA"
```

The CA must outlive the certificates it signs, or the last renewal before expiry silently
issues leaves the CA cannot vouch for. 7300 gives 3650-day leaves a full second term.

### Recommended — client certificate constraints

```
openssl x509 -req -days 3650 -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt
```

signs `client.crt` with no extensions at all, which means it is valid for **any** purpose,
including as a *server* certificate. Since `client.key` sits in `/root/.certs` on every
node, that is a usable path to standing up a listener that other nodes accept. Adding a
`client.cnf` alongside the existing `node-$ip.cnf` pattern:

```
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no
[req_distinguished_name]
CN = HCI-Client
[v3_req]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = clientAuth
```

and signing with `-extensions v3_req -extfile client.cnf` confines it to client use.
OpenSSL applies the `ssl_client` purpose when a server verifies its peer and `ssl_server`
when a client does, so `clientAuth` alone is correct for `client.crt` — but the node
certificate needs **both** `serverAuth` and `clientAuth`, because `spark-daemon` and
`mipha` dial peers with `node.crt`. Issuing a node certificate with only `serverAuth`
breaks every outbound call it makes.

### Do existing clusters need regeneration?

**No, not to keep working.** Everything shipped here works against certificates minted by
the current `provision.py`: node certificates already carry `IP:<node ip>`, which is what
hostname verification needs for peer-to-peer traffic, and the loopback route is handled by
rewriting the address client-side.

**Yes, to close the last gap.** Until node certificates carry the VIP, connections to the
VIP cannot be bound to a single node. Regenerating is a normal `impa renew --yes` — it
writes the full SAN above — and does not require re-provisioning. Do it at the next
convenient maintenance window; there is no urgency, and the interim behaviour is described
in §5.

---

## 5. Hostname verification: what is on, and what is not

Every mTLS client in this tree used to set `check_hostname = False`. With
`verify_mode = CERT_REQUIRED` and the cluster CA as the trust store, that verifies the
chain but not the identity: **any** certificate the cluster CA ever signed satisfies a
connection to **any** node. One compromised node could answer for the node being
orchestrated, and the shared `client.crt` — present on every node — could be used to stand
up a listener.

Python's `ssl` module matches an IP address in a certificate's SAN when the IP literal is
passed as `server_hostname`; CPython detects an IP literal, skips SNI, and uses
`X509_VERIFY_PARAM_set1_ip`. So the constraint that motivated turning verification off —
certificates addressed by IP rather than hostname — was never actually a blocker.

| Route | Before | Now |
| :--- | :--- | :--- |
| Peer node by IP (`run_remote_spark(ip, ...)`, all daemons) | chain only | **full**: `check_hostname = True`, bound to that node's IP SAN |
| `spark-daemon` maintenance forwarding to peers from `cluster.json` | chain only | **full** |
| Own host via `127.0.0.1:9099` | chain only | **full**, by rewriting loopback to this node's own address, which `spark-daemon` also listens on |
| Own host when `spectrum.env` is unreadable and the local address is unknown | chain only | chain only — the identity check is dropped rather than failing a call that cannot leave the machine anyway |
| The floating VIP (`cluster_new.py make_request`) | chain only | **partial**: the peer's IP SAN must name a host listed in `cluster.json` |

The VIP is the one route that cannot be fully bound with the certificates a current
`provision.py` produces, because the VIP is answered by whichever node holds it and no
certificate is issued for it — there is no single name to hand `check_hostname`.
`ClusterPeerSSLContext` in `cluster_new.py` closes what can be closed: it requires the
peer to present a certificate whose IP SAN is a configured cluster node. That is weaker
than binding the connection to one node, but it rejects the shared client certificate
(which carries no SAN at all) and anything signed by the CA that is not a node. Once node
certificates carry `IP:$VIP` per §4, that class can be deleted and the VIP treated like
any other address.

### The Elixir client

`spectrum_phx/lib/spectrum_phx/spark.ex` carries the same workaround, as a
`customize_hostname_check` whose `match_fun` returns `true` for every peer. **It needs no
change for anything here to work** — nothing in this document alters what `spark-daemon`
presents, only what clients require of it, and Erlang's `:ssl` is an independent client
that keeps working exactly as before.

It is still the one client that accepts any cluster-signed certificate for any node, and
closing that is a separate piece of work. It is not a straight port of the Python change:
OTP builds its reference identifier from the connection's `server_name_indication`, so an
IP literal has to be presented as an `{:ip, _}` reference identifier before
`:public_key.pkix_verify_hostname/2` will compare it against an `iPAddress` SAN, and how
that reaches `:ssl` through Req/Finch/Mint needs checking against the pinned OTP 27.1.2
rather than assumed.

---

## 6. Recommended validity, once renewal is routine

3650-day certificates are how this became invisible. Nothing exercised the renewal path
because nothing ever needed it, so the path did not exist. With `impa renew` in place,
shortening the leaves makes the mechanism something the cluster uses rather than something
that has never run:

```bash
impa renew --days 825 --yes
```

825 days is the public-CA maximum and a reasonable fleet default. Keep the CA long
(7300); rotating a CA is a three-pass operation and there is no reason to do it on a leaf
cadence.

---

## 7. Verifying a renewal by hand

```bash
# What the daemon actually presents, and whether it is valid for the address you dialled
openssl s_client -connect 10.10.102.41:9099 \
    -CAfile /root/.certs/ca.crt \
    -cert /root/.certs/client.crt -key /root/.certs/client.key \
    -verify_return_error </dev/null 2>&1 | head -20

# Certificate and key still a pair
openssl x509 -noout -modulus -in /etc/hci/spark/certs/node.crt | openssl sha256
openssl pkey -noout -modulus -in /etc/hci/spark/certs/node.key | openssl sha256

# The whole fleet at once
impa status --all-nodes
allssh mcli health_checks run --check mtls_cert_expiry_warning
```

`impa selftest` mints a throwaway CA and node/client pair in a temporary directory and
completes a real mTLS handshake against them, asserting that the SAN satisfies hostname
verification, that the node certificate still works as a client certificate, and that a
certificate for one address is refused for another. It touches nothing outside its temp
directory and is safe to run on a production node.
