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

## What the Phoenix console lost in the port, and what was restored

The strangler migration measured "every page is Phoenix". That is a measure of whether each
page renders, not of whether it can still *do* what the page it replaced did, and six things
could not. The rule for all of them was to find the old behaviour first (`static/settings.html`,
`static/vms.html`, `static/app.js`, `spectrum_server.py`) and port it faithfully before
improving it. Phoenix talks to Hydra, Spark and Catalyst directly, so "the same API the old UI
used" means the same *effects*, not an HTTP hop through the Python tier.

| What was reported | What had been lost | What it is now |
|---|---|---|
| **Settings is read-only, the VIP included** | The cluster name, VIP, subnet and replication factor were figures, not inputs. And saving DNS, NTP or timezone wrote a row and told no host: the Python save also rewrote `resolv.conf` and `chrony.conf`, set the timezone, rewrote `cluster.json` everywhere (restarting `bifrost` when the VIP moved), altered the keyspace and repaired when it rose, and re-scheduled the scrub. | `SpectrumPhx.Settings.save/2` validates the whole form, writes, and applies. See below. |
| **Policies is nonsense** | There was no such page: it was a *panel* on Settings holding region, scrub interval, session timeout, rate limit, password policy and DRS. None of those is a policy in the sense the rest of the system uses the word. | The panel is gone. `/policies` shows the four things the cluster actually has that are policies. See below. |
| **VM create lost most of its options** | The old wizard carried vCPU, memory with a unit, firmware, boot device, CPU model, any number of disks (size, unit, container, bus), any number of CD-ROMs and any number of NICs (network, model). The port had name, vCPU, MiB, firmware and three text boxes. | All of them, on one full-width page, plus the graphics device. See below. |
| **Disks and CD-ROM are one text box** | A comma-separated string where the old form had repeatable rows with a container picker. | Repeatable rows: *Add disk*, *Add CD-ROM*, *Add NIC*, each removable. |
| **The storage page looks bad** | One column of full-width cards, a card per vdisk and a card per store. | Panels in a grid; vdisks are table rows. See below. |
| **The dashboard uses 55-60% of the window** | The layout wrapped every page in `mx-auto max-w-7xl`, a 1280px column. | Removed. Every page is full width. |

### Saving a setting does what the old save did

`SpectrumPhx.Settings.Apply` is the part that was dropped. Only what *changed* is applied -- the
form submits every field, so re-saving an unchanged page touches no host.

| Changed | Effect |
|---|---|
| DNS resolvers, search domains | `/etc/resolv.conf` rewritten on every host |
| NTP servers | `/etc/chrony.conf` rewritten, `chronyd` restarted, on every host |
| Timezone | `timedatectl set-timezone` on every host |
| Cluster name, VIP, subnet | merged into `/etc/hci/cluster.json` on every host; `bifrost` restarted on every host when the VIP moved |
| Replication factor | `ALTER KEYSPACE hydra` (NetworkTopologyStrategy, datacenter read from `system.local`), capped at the node count; a repair is started when it rose, because the new replicas are empty until one runs |
| Scrub interval | `hydra.dagur_schedules` row for `storage_scrub` re-scheduled (disabled disables the job) |
| Password policy, session timeout, rate limit, DRS, region, MTU | the row only, as before |

What is new where the old save had none: every field is **validated first, as a whole**, and
nothing is written or applied if any fails (the Python endpoint validated nothing; a VIP of
`banana` went into `cluster.json` on every host). Values reach hosts as base64 inside the
command, never as text the shell parses, and a timezone is refused rather than sanitised into a
different one. A host that does not take a change is *named* on the page and does not hide that
the others did; it is not rolled back. A blank cluster field means "not shown", never "clear it".
Operator accounts can be created and have their password changed from the page, under the
cluster's password policy.

Two latent bugs surfaced on the way and are fixed. The settings page asked for the cluster name
with a string key where `Cluster.Config` is keyed by atoms, so it never showed one. And the
password-policy hint said `strict` where the backend tests for `enabled`, so an operator who
followed it silently kept the weak rule; it is now a select offering the values the backend
checks.

Not ported from the old Settings page, and still on no Phoenix page: SSL certificate upload,
node add / remove / safe reboot, the maintenance operations (rebalance, cleanup, keyspace
cleanup), the language selector and theme cards. Those were not part of the report and are not
claimed.

### Policies

`/policies` is read-only and says where each kind is edited. It shows:

* **Snapshot policies** (`hydra.dfs_snapshot_policies`) by scope, with a disabled narrow row
  called an *exemption* rather than "disabled", because that is what it does. Set with
  `valcli storage.snapshot-policy.set`.
* **Protection domains** (`hydra.dfs_protection_domains`): members, cadence, retention,
  consistency level, pause bound and the latest set. Managed with `valcli storage.domain*`.
* **Storage container policies** (`hydra.storage_containers`): tier, quota, fault tolerance and
  compression. Edited on Storage.
* **Security policy**: password complexity, session timeout, auth rate limit. Edited on Settings.

*Assumption, stated:* the page was undefined in the report, so this is what was inferred from
the policy objects that exist in the schema. A table that could not be read is reported as
unreadable and never drawn as "no policies", because on this page that sentence means "nothing
is being protected". There is no editing here on purpose: the first writer of each table has
rules this page does not know, and a second writer is how a policy gets set that the first
refuses to honour.

### VM create

One page, a grid of panels. Containers, images and networks are drop-downs over the real
catalogues (`SpectrumPhx.Vms.Options`); a catalogue that failed to load says so rather than
offering an empty list. `SpectrumPhx.Vms.Form` turns the rows into the strings Vali reads:
`20GB:default-pool:virtio` in `disks_list`, comma-separated image names in `iso`, and the JSON
list `["<network>:<model>"]` in `network_id` (no rows is `[]`, an isolated VM, not the default
network). `Vm.disks/1` used to split an entry on its first colon only, which made the old
console's three-part entries a container named `default-pool:virtio`; the bus is now a field.

**Graphics: SPICE is offered only when every host reports it** in
`/api/v1/host/capabilities`, for the reason in "Does this hypervisor support SPICE at all?"
above -- the VM can be placed on any node and a host without SPICE refuses the domain. The
choice is stored in `hydra.vms.graphics`.

Two things the old form showed are **deliberately not offered**, because a control that does
nothing is a lie. *Network (PXE)* as a boot device: `generate_vm_xml` only distinguishes
`cdrom` from everything else, so choosing it booted the disk. And **Secure Boot**: nothing in
`hydra.vms` or in `generate_vm_xml` expresses it (the host-side Secure Boot gate was removed with
DRBD). Adding either is a change to Vali and the schema; the form follows.

### Storage

The same data as before -- Sidon's per-node capacity, vdisk list and peers, `lsblk` and the
container catalogue -- laid out for reading: a summary panel (vdisk counts, capacity bar), then
extent stores beside containers, then **one vdisks table** (a body per vdisk so that what is
wrong with it stays inside it), then physical disks per node. Everything that refused to draw an
unknown as healthy still refuses.

### Not built: dedup

Dedup is blocked on decision **D-23** (the extent id map and what an inline dedup would cost the
drain). It is not a settings toggle and nothing here pretends otherwise. See `TODO.md`.
