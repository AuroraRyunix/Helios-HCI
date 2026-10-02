# The guest console

Two protocols, chosen per VM. VNC is the default; SPICE is opt-in.

## The decision, and why it was the gate

SPICE had been half-present in this tree for a long time. The vendored client was here and
compiled to WebAssembly on *every* rollout, [`agahnim`](agahnim.md) bridged TCP to
WebSocket protocol-agnostically, and Spectrum's WebSocket proxy was already parameterised
by `console_type` and already refused a protocol mismatch rather than downgrading. What was
missing sat at the two ends rather than in the middle: no domain was ever given a SPICE
device, and no page loaded the client.

Neither end could be built without first answering one question — **is SPICE a
cluster-wide default, or a per-VM choice?** — because the answer decides whether the
graphics type is a column or a switch.

It is **per-VM**, for three reasons:

1. **The graphics device lives in the domain XML**, so it is per-domain by construction. A
   cluster-wide switch would still have to rewrite every domain and restart every guest
   before it meant anything — a per-VM, restart-requiring change wearing a toggle's
   clothes. Presenting it as a global flag would be a lie about what the flag does.
2. **The proxy was already per-VM.** It reads the graphics type off the live domain and
   refuses a mismatch. Everything downstream of the decision was per-VM already; the
   stored column was the only part that did not exist.
3. **The client is unproven.** Its cursor encoder had never emitted a valid zlib stream —
   a deflate header with BFINAL on the wrong bit and big-endian stored-block lengths — so
   no cursor has ever rendered through it. Making that the default console for every VM in
   the cluster would bet the primary operator interface on untested code. Per-VM means it
   can be tried on one guest.

## How it works

`hydra.vms.graphics` holds `spice` or `vnc`. **Null means VNC**, so every VM written before
migration `0010-vm-graphics` keeps the console it has always had, and the migration
rewrites no rows — switching a console is a redefine, which is a restart, and a migration
cannot do that to a running cluster.

The desired state is the column. The **running domain is the runtime truth**: `vali` builds
the graphics device when it defines the domain, so a change takes effect on the VM's next
start. The console says so when you change a running VM rather than letting you click a
SPICE button at a VNC server and read the proxy's refusal as a bug.

| | VNC | SPICE |
|---|---|---|
| `<graphics type=…>` | `vnc` | `spice`, `<image compression='off'/>` |
| Video adapter | `virtio` | `virtio` — *not* QXL |
| Extra channel | — | `spicevmc` → `com.redhat.spice.0` |
| Page | `vnc_auto.html` | `spice_auto.html` |

Both pages take the same session token from the same places, exchange it at
`/api/vms/console/token` for a short-lived console ticket, and open one WebSocket at
`/api/vms/console/ws`. Slate keeps both on the Python tier by their `.html` suffix; they
are the one page-shaped thing Phoenix does not serve.

### Two choices that look wrong and are not

**The video adapter stays VirtIO.** QXL is the classic SPICE pairing and the obvious
reach, but [`vali.md`](vali.md) records that QXL is avoided on these hosts because the BIOS
ROM files it needs are absent from the EL 10.2 repositories — and a video model whose ROM
is missing is a domain that does not start. The client does not require it: `spice-html5`
decodes the SPICE *display channel*, which QEMU serves whatever adapter is behind it.
Keeping one adapter across both protocols also means switching a console does not hand the
guest different hardware to find drivers for.

**Image compression is off.** The client can decode LZ — that is what `lz_decompress.c` is
compiled to WebAssembly for — but QEMU's default is `auto_glz`, and GLZ is a different,
dictionary-backed format. Off is the setting certain to render. It costs bandwidth on a LAN
that has it, and it is the first thing to revisit once a SPICE console has been watched
working on hardware.

## Does this hypervisor support SPICE at all?

Worth asking, not assuming. Red Hat deprecated QXL and then removed SPICE from the EL
virtualisation stack, and these hosts are built from EL 10.2 repositories. A
`<graphics type='spice'>` on a QEMU without SPICE compiled in is not a degraded console:
**libvirt refuses the domain, so the VM never starts.**

`/api/v1/host/capabilities` therefore reports a `graphics` list read from
`virsh domcapabilities` — what this binary supports, rather than what the distribution
usually ships. A host it cannot read reports an empty list, and an empty list is treated as
"do not offer SPICE": wrong in the harmless direction.

## Unverified

**No SPICE console has been watched working against a real guest.** The cluster was
unreachable when this was written, so everything above is verified by `test_vm_graphics.py`
and by reading the vendored client's exports — not on hardware. What to check first, in
order:

1. `GET /api/v1/host/capabilities` on each node — does `graphics` contain `spice`? If it
   does not, nothing else here matters and the vendored client should be retired rather
   than finished.
2. Create a VM with `graphics: "spice"`, start it, and confirm the domain actually defines
   and runs.
3. Open the console and watch for the **cursor** specifically. That is the code path whose
   encoder was broken, so it is the one with no history of working.

## One thing this does not change

Neither protocol's port takes a password, and both listen on `0.0.0.0`. That is the posture
VNC has always had here, and SPICE now matches it rather than making it worse — but it
means the console port is reachable directly on the LAN, bypassing the ticket exchange
entirely. It is a pre-existing exposure with a second protocol on it now, and it belongs
behind the network boundary until that changes.
