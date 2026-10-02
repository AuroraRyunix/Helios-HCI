#!/usr/bin/env python3
"""Ordered, recorded schema for the `hydra` keyspace.

Until now the schema was 38 `CREATE TABLE IF NOT EXISTS` statements spread across five
daemons, each running its own `init_db` at startup. Six tables were declared by two
daemons and one by three. They happen to agree today -- that was checked, statement by
statement -- but nothing makes them agree, and nothing ever notices when they stop.

The failure that shape produces is a rolling upgrade. Node A restarts on the new build
and creates a table with a new column; node B is still on the old build and its own
`CREATE TABLE IF NOT EXISTS` is a no-op, so it never learns. Whichever daemon reaches a
fresh cluster first silently wins, and the loser spends the rest of its life reading
columns that are not there. `IF NOT EXISTS` makes that silent rather than loud.

So: one ordered list, applied once, recorded in the database, behind a lock.

## What this is not

It is not an ORM and it does not diff anything. Cassandra/Scylla DDL is not
transactional -- there is no rollback of a half-applied `ALTER` -- so migrations are
written to be individually idempotent and are recorded one at a time. A migration that
fails half way leaves the ones before it applied and recorded, and re-running resumes
from the failure rather than starting over.

## Using it

    import helios_schema
    helios_schema.ensure_schema(run_conditional_cql_query, node_id=LOCAL_IP)

`execute` is any callable taking one CQL string and returning `(rc, stdout, stderr)` --
the signature `run_cql_query` already has in every daemon here. Nothing in this module
imports a driver, opens a socket, or knows whether it is talking to Daruk or cqlsh.

It must be the *unguarded* executor. `run_cql_query` refuses a conditional statement
because it cannot report whether the condition held, and this module's own schema lock is
an IF NOT EXISTS insert -- so a daemon that passed the guarded one raised the moment
anything was actually pending, and only then. Four daemons did, and the failure was
invisible for as long as no migration was outstanding: on a cluster that had nothing to
apply, `ensure_schema` returns before it ever reaches the lock.
"""

KEYSPACE = "hydra"

# Bumped only when the *bookkeeping* tables change shape. Migrations themselves are
# added to MIGRATIONS; this is the schema of the ledger that records them.
BOOKKEEPING = [
    "CREATE TABLE IF NOT EXISTS hydra.schema_migrations ("
    " id text PRIMARY KEY, checksum text, applied_at timestamp, applied_by text );",
    # A lock row, not a lock table: one partition, taken with IF NOT EXISTS. The TTL is
    # the important part -- a daemon that dies mid-migration must not wedge every other
    # node in the cluster forever, and there is nobody to notice and clear it by hand.
    "CREATE TABLE IF NOT EXISTS hydra.schema_lock ("
    " name text PRIMARY KEY, holder text, acquired_at timestamp );",
]

LOCK_NAME = "hydra-schema"
# Long enough for the slowest migration, short enough that a crashed holder does not
# block a cluster restart. Applying the baseline on a fresh cluster is the slow case.
LOCK_TTL_SECONDS = 300

# The uniqueness constraint hydra.gatoway_networks cannot express, held in its own table
# so that a VLAN id can be a partition key and therefore the subject of an IF NOT EXISTS.
VLAN_CLAIMS_TABLE = "hydra.gatoway_vlan_claims"
NETWORKS_TABLE = "hydra.gatoway_networks"


def _json_rows(stdout):
    """The `SELECT JSON` rows in a result, whichever path produced it.

    Through Daruk each row arrives as one JSON object on its own line; through the cqlsh
    fallback the same objects are wrapped in a table with a `[json]` header, a rule and a
    row count. Both reduce to "a line that starts with { and ends with }", which is the
    test every reader of gatoway_networks in this repo already applies.

    Returns None if a line looked like JSON and did not parse. A partial set is worse than
    no set here for the same reason it is worse in Gatoway: acting on it would look like
    the missing rows did not exist.
    """
    rows = []
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{") or not line.endswith("}"):
            continue
        import json

        try:
            rows.append(json.loads(line))
        except ValueError:
            return None
    return rows


def backfill_vlan_claims(execute, report, now_ms):
    """Give every VLAN a network already carries a claim, and name the ones that clash.

    The claim table is only a constraint on writes made after it exists. A cluster
    adopting it already has networks, and if nothing claimed their VLANs the first create
    after the migration would win a claim on a VLAN that is visibly in use -- which is the
    duplicate this whole exercise exists to prevent, arrived at from the other direction.

    ## Why a duplicate cannot fail the migration

    A cluster that already has two networks on VLAN 100 is *why* this table is being
    added. Refusing to migrate would leave that cluster with no constraint at all and no
    way to get one, and it would do it at daemon start, where the failure surfaces as a
    service that will not come up rather than as an answer to a question anyone asked.
    So one of the two wins the claim, the rest are reported, and the operator is told
    what to do about them. Note what is *not* done: nothing is deleted and nothing is
    re-tagged. Both are configuration changes that take a guest's network away, and a
    migration running at daemon start is the worst possible place to make one.

    The winner is the lowest `net_id`, which is arbitrary but stable: every node runs this
    and they must not disagree about who holds VLAN 100.

    ## Re-running

    Each claim is an IF NOT EXISTS, so a second run refuses every claim it already made
    and reports nothing new. A read that fails, or a claim that fails for a reason other
    than losing its race, raises instead -- the migration is then not recorded and the
    next daemon start tries again, which is the behaviour a half-applied migration wants.
    """
    rc, stdout, stderr = _run(
        execute, "SELECT JSON net_id, name, vlan_id FROM %s;" % NETWORKS_TABLE)
    rows = _json_rows(stdout)
    if rows is None:
        raise SchemaError(
            "a row of %s did not parse, so it is not known which VLANs are in use. "
            "The claim table is created but empty; this migration is not recorded and "
            "will run again. (%s)" % (NETWORKS_TABLE, (stderr or "").strip()))

    by_vlan = {}
    for row in rows:
        vlan = row.get("vlan_id")
        # Null for every `direct` network -- verified against the live cluster, where the
        # seeded Physical-Direct row carries `"vlan_id": null`. Booleans are excluded
        # because bool is a subclass of int and a claim keyed on True is nonsense.
        if not isinstance(vlan, int) or isinstance(vlan, bool):
            continue
        by_vlan.setdefault(vlan, []).append(row)

    for vlan in sorted(by_vlan):
        holders = sorted(by_vlan[vlan], key=lambda row: str(row.get("net_id") or ""))
        winner = holders[0]
        _rc, claim_stdout, _stderr = _run(execute, claim_vlan_statement(
            vlan, winner.get("net_id"), winner.get("name"), now_ms))
        if not lwt_applied(claim_stdout):
            # Either a re-run, in which case the holder is this same network and there is
            # nothing to say, or somebody claimed it in between -- which is a network that
            # carries VLAN 100 without holding it, and is worth saying.
            winner = _claim_holder(execute, vlan) or winner

        losers = [row for row in holders
                  if str(row.get("net_id")) != str(winner.get("net_id"))]
        if not losers:
            continue
        report(
            "VLAN %d is carried by %d networks. The claim is held by %s (%s); %s hold no "
            "claim and will keep working exactly as they do now, but nothing can be "
            "created on VLAN %d until they are gone. Re-tag or delete the extras -- "
            "whichever is safe for the guests attached to them -- and the claim then "
            "matches reality."
            % (vlan, len(holders), winner.get("name"), winner.get("net_id"),
               ", ".join("%s (%s)" % (row.get("name"), row.get("net_id"))
                         for row in losers),
               vlan))


def claim_vlan_statement(vlan_id, net_id, name, now_ms):
    """The claim, as CQL text.

    Here rather than at its call sites because the migration and Daruk's typed endpoint
    have to write the same row: a claim the migration made must be releasable by a delete
    the console makes, and vice versa. The column list is explicit because
    `INSERT ... JSON ? IF NOT EXISTS` is accepted by Scylla and then executes
    unconditionally -- no `[applied]` column, and the existing row overwritten.
    """
    return (
        "INSERT INTO %s (vlan_id, net_id, name, claimed_at_ms) "
        "VALUES (%d, %s, %s, %d) IF NOT EXISTS;"
        % (VLAN_CLAIMS_TABLE, int(vlan_id), quote(net_id), quote(name), int(now_ms)))


def _claim_holder(execute, vlan_id):
    """Who holds the claim on `vlan_id`, or None.

    Read back with its own SELECT rather than parsed out of the refused claim's row: a
    refusal arrives through Daruk as the row's values joined by spaces, and a network
    named "Marketing VLAN" makes that unsplittable. The extra read costs one round trip
    on a path that only runs when a claim was refused.
    """
    try:
        _rc, stdout, _stderr = _run(execute, "SELECT JSON net_id, name FROM %s WHERE vlan_id = %d;"
                                    % (VLAN_CLAIMS_TABLE, int(vlan_id)))
    except SchemaError:
        return None
    rows = _json_rows(stdout)
    return rows[0] if rows else None


MIGRATIONS = [
    {
        "id": "0001-baseline",
        "description": (
            "Every table the five daemons created independently, deduplicated. "
            "Safe on an existing cluster: each statement is IF NOT EXISTS, so adopting "
            "a cluster that already has these tables records the migration without "
            "changing anything."
        ),
        "statements": [
        "CREATE TABLE IF NOT EXISTS hydra.catalyst_tasks ( task_id uuid PRIMARY KEY, service text, action text, status text, payload text, progress int, error_msg text, created_at timestamp, updated_at timestamp );",
        "CREATE TABLE IF NOT EXISTS hydra.cluster_settings ( key text PRIMARY KEY, value text );",
        "CREATE TABLE IF NOT EXISTS hydra.console_metrics ( vm_name text, timestamp timestamp, avg_fps float, low_fps float, latency float, PRIMARY KEY (vm_name, timestamp) ) WITH CLUSTERING ORDER BY (timestamp DESC) AND default_time_to_live = 86400;",
        "CREATE TABLE IF NOT EXISTS hydra.console_sessions ( console_token text PRIMARY KEY, host_ip text, port int, expires_at int );",
        "CREATE TABLE IF NOT EXISTS hydra.dagur_runs ( job_name text, start_time timestamp, run_id uuid, end_time timestamp, status text, exit_code int, output text, PRIMARY KEY (job_name, start_time) ) WITH CLUSTERING ORDER BY (start_time DESC);",
        "CREATE TABLE IF NOT EXISTS hydra.dagur_schedules ( job_name text PRIMARY KEY, task_type text, cron_expression text, interval_seconds int, enabled boolean, last_run_epoch bigint, command text );",
        "CREATE TABLE IF NOT EXISTS hydra.gatoway_networks ( net_id uuid PRIMARY KEY, name text, type text, vlan_id int );",
        "CREATE TABLE IF NOT EXISTS hydra.hylia_jobs ( job_id uuid PRIMARY KEY, state text, target_nodes list<text>, current_node text, build_number text, manifest_json text, changelog_md text );",
        "CREATE TABLE IF NOT EXISTS hydra.hylia_logs ( job_id uuid, timestamp timestamp, log_line text, PRIMARY KEY (job_id, timestamp) ) WITH CLUSTERING ORDER BY (timestamp ASC);",
        "CREATE TABLE IF NOT EXISTS hydra.lanayru_clusters ( cluster_id uuid PRIMARY KEY, name text, control_nodes int, overlay_segment_id uuid, status text, created_at timestamp );",
        "CREATE TABLE IF NOT EXISTS hydra.lanayru_k8s_state ( cluster_id uuid, name text, value blob, version int, is_dir boolean, ttl int, PRIMARY KEY (cluster_id, name) );",
        "CREATE TABLE IF NOT EXISTS hydra.lcm_inventory ( key text PRIMARY KEY, inventory_json text, last_updated timestamp );",
        "CREATE TABLE IF NOT EXISTS hydra.lcm_update_state ( key text PRIMARY KEY, latest_version text, release_date text, download_url text, sha256 text, size bigint, changelog text, current_version text, update_available boolean, last_checked timestamp, error_msg text );",
        "CREATE TABLE IF NOT EXISTS hydra.logos_metrics ( node_ip text, timestamp timestamp, cpu_pct float, mem_pct float, mem_total_kb bigint, cpu_cores int, disk_iops float, disk_bandwidth_kbps float, net_rx_kbps float, net_tx_kbps float, PRIMARY KEY (node_ip, timestamp) ) WITH CLUSTERING ORDER BY (timestamp DESC) AND default_time_to_live = 86400;",
        "CREATE TABLE IF NOT EXISTS hydra.mimir_results ( category text, check_name text, node_ip text, status text, output text, execution_id uuid, timestamp timestamp, PRIMARY KEY (category, check_name, node_ip) );",
        "CREATE TABLE IF NOT EXISTS hydra.mimir_schedules ( schedule_name text PRIMARY KEY, category text, enabled boolean, last_run_epoch bigint );",
        "CREATE TABLE IF NOT EXISTS hydra.nodes ( hostname text PRIMARY KEY, ip text, status text, maintenance_mode boolean );",
        "CREATE TABLE IF NOT EXISTS hydra.sessions ( session_token text PRIMARY KEY, username text, created_at timestamp );",
        "CREATE TABLE IF NOT EXISTS hydra.storage_containers ( name text PRIMARY KEY, tier text, quota_bytes bigint, path text, ftt int );",
        "CREATE TABLE IF NOT EXISTS hydra.urbosa_firewall_rules ( rule_id uuid PRIMARY KEY, description text, source_ip text, dest_ip text, protocol text, port int, action text, priority int );",
        "CREATE TABLE IF NOT EXISTS hydra.urbosa_segments ( segment_id uuid PRIMARY KEY, name text, vni int, t1_link_id uuid, subnet_cidr text, gateway_ip text, dhcp_enabled boolean, dhcp_start text, dhcp_end text );",
        "CREATE TABLE IF NOT EXISTS hydra.urbosa_t0_routers ( router_id uuid PRIMARY KEY, name text, uplink_interface text, uplink_ip text, gateway_ip text, nat_rules text );",
        "CREATE TABLE IF NOT EXISTS hydra.urbosa_t1_routers ( router_id uuid PRIMARY KEY, name text, t0_link_id uuid, dhcp_enabled boolean );",
        "CREATE TABLE IF NOT EXISTS hydra.urbosa_tunnel_metrics ( node_ip text, interface_name text, timestamp timestamp, rx_kbps float, tx_kbps float, rx_packets float, tx_packets float, PRIMARY KEY ((node_ip, interface_name), timestamp) ) WITH CLUSTERING ORDER BY (timestamp DESC) AND default_time_to_live = 86400;",
        "CREATE TABLE IF NOT EXISTS hydra.users ( username text PRIMARY KEY, password_hash text );",
        "CREATE TABLE IF NOT EXISTS hydra.valhalla_images ( name text PRIMARY KEY, filename text, size_bytes bigint, type text, path text, created_at timestamp );",
        "CREATE TABLE IF NOT EXISTS hydra.vali_drs_history ( event_time timestamp, vm_name text, source_host text, target_host text, reason text, PRIMARY KEY (event_time, vm_name) );",
        "CREATE TABLE IF NOT EXISTS hydra.vali_drs_status ( cluster_name text PRIMARY KEY, current_deviation double, status_str text, last_drs_run bigint, drs_enabled boolean );",
        "CREATE TABLE IF NOT EXISTS hydra.vali_tasks ( task_id uuid PRIMARY KEY, vm_name text, action text, status text, target_host text, created_at bigint, updated_at bigint, error_msg text );",
        "CREATE TABLE IF NOT EXISTS hydra.vm_nvram ( vm_name text PRIMARY KEY, nvram_data text );",
        "CREATE TABLE IF NOT EXISTS hydra.vms ( name text PRIMARY KEY, vcpu int, memory int, disk_path text, disk_size int, state text, host_ip text, disks_list text, firmware text, iso text, boot_device text, network_id text, cpu_model text, audio_enabled boolean, status text );",
        ],
    },
    {
        "id": "0002-cluster-locks",
        "description": (
            "One row per cluster-wide mutual exclusion, taken with IF NOT EXISTS. "
            "Added for the maintenance lock: 'only one host in maintenance at a time' "
            "was a scan of every hydra.nodes row followed by a write, so two hosts "
            "entering concurrently both read 'nobody' and both proceeded. A lightweight "
            "transaction cannot span partitions, so the exclusion has to live in a "
            "single row that every contender conditions on."
        ),
        "statements": [
            # `holder_token` identifies one *acquisition*, not one node. Releasing on
            # holder alone lets a stale release from a node's earlier, expired
            # acquisition drop the lock that same node holds now -- the flaw daruk.md
            # records against the migration lock.
            #
            # `acquired_at_ms` is bigint rather than timestamp on purpose. A refused
            # IF NOT EXISTS returns the whole existing row, and Daruk's make_serializable
            # passes a driver datetime through untouched, so json.dumps would raise on
            # exactly the response that tells a caller who holds the lock.
            "CREATE TABLE IF NOT EXISTS hydra.cluster_locks ( name text PRIMARY KEY, holder text, holder_token text, reason text, acquired_at_ms bigint );",
        ],
    },
    {
        "id": "0003-bound-task-history",
        "description": (
            "A retention window on hydra.catalyst_tasks, which grows without bound. "
            "Its primary key is task_id alone, so it has no time-ordered clustering key "
            "and 'the most recent N tasks' is not answerable server-side -- every read "
            "is a full scan of the whole table, and both consoles do exactly that on "
            "every page load. This does not make the query indexed; it bounds what the "
            "scan has to walk, which is the part that gets worse forever. Thirty days "
            "of task history is a log, not a system of record."
        ),
        "statements": [
            # Not ALTER ... ADD, which errors when the column exists and would break
            # every restart after the first. Setting a table property is idempotent:
            # applying the same value twice is accepted, so a re-run is a no-op.
            #
            # Existing rows are unaffected -- a default TTL applies to writes made after
            # it is set, so history already recorded stays until something rewrites it.
            # That is the conservative direction: nothing already stored disappears
            # because this migration ran.
            #
            # The real fix is a companion table keyed by time bucket that Catalyst writes
            # alongside, so recent tasks are one partition read. That is a dual-write and
            # belongs with the work that migrates the readers.
            "ALTER TABLE hydra.catalyst_tasks WITH default_time_to_live = 2592000;",
        ],
    },
    {
        "id": "0004-urbosa-transit-pool",
        "description": (
            "Transit /30 allocations for Urbosa Tier-1 routers. The addressing was "
            "derived from md5(router_id) with no collision check and no persistence, so "
            "two routers whose hashes met were handed the same /30 and the Tier-0 "
            "delivered one tenant's return traffic into the other's namespace. Keyed on "
            "the slot, so INSERT ... IF NOT EXISTS resolves two claimants racing for one "
            "subnet."
        ),
        "statements": [
            # text and bigint rather than uuid and timestamp. That was originally to work
            # around Daruk returning 400 on any lightweight transaction whose refused row
            # carried a type json.dumps could not encode -- which is exactly the response
            # that says who won the slot. make_serializable now handles both, so this is
            # no longer load-bearing; it is kept because changing a live table's column
            # types is not something a migration can do, and epoch milliseconds are what
            # the rest of this schema already stores.
            "CREATE TABLE IF NOT EXISTS hydra.urbosa_transit_pool ( subnet_index int PRIMARY KEY, router_id text, node_id text, allocated_at_ms bigint );",
        ],
    },
    {
        "id": "0005-dfs-extent-store",
        "description": (
            "The extent-based DFS map: which extent group holds each 1 MiB extent of "
            "each vdisk, and where those groups physically live. Guest bytes never "
            "appear here -- the map says where data is, never what it is. Serves "
            "invariant I-3 (no silent corruption of the map) by making the block map "
            "single-writer-per-partition: partitioned by vdisk, clustered by extent "
            "index, so the owner's lookups and drain commits are single-partition "
            "operations and last-write-wins is not a euphemism for data loss. "
            "dfs_vdisks carries the (owner, epoch) CAS pair that makes ownership "
            "transfer safe and the drain_seq counter that makes the drain exactly-once."
        ),
        "statements": [
            # text ids rather than uuid, epoch-ms bigints rather than timestamp: the
            # convention cluster_locks and urbosa_transit_pool set, and the shape Daruk's
            # serializer and the Rust client both handle without type negotiation.
            "CREATE TABLE IF NOT EXISTS hydra.dfs_vdisks ( vdisk_id text PRIMARY KEY, container text, size_bytes bigint, class text, owner text, epoch bigint, drain_seq bigint, extent_bytes int, egroup_bytes int, created_at_ms bigint, parent_vdisk text );",
            # One partition per vdisk, clustered by extent index. A 1 TiB vdisk is ~1M
            # rows in one partition: large but bounded, read once at open, maintained
            # incrementally thereafter.
            "CREATE TABLE IF NOT EXISTS hydra.dfs_block_map ( vdisk_id text, extent_index bigint, egroup_id text, egroup_offset int, length int, epoch bigint, PRIMARY KEY ((vdisk_id), extent_index) );",
            # No refcount column, now or ever: reclamation is Purah's mark-sweep. A
            # refcount here would be a distributed counter with a crash window between
            # every data operation and its count operation.
            "CREATE TABLE IF NOT EXISTS hydra.dfs_egroups ( egroup_id text PRIMARY KEY, state text, node text, path text, size int, seal_hash text, vdisk_hint text, created_at_ms bigint );",
        ],
    },
    {
        "id": "0006-dfs-replication",
        "description": (
            "Replica placement. Serves invariant I-1 (durability across the failures "
            "the container's ftt claims to tolerate) and I-4 (single writer), neither of "
            "which can be satisfied at all until the map records where a vdisk's copies "
            "actually are. Two columns and one table. "
            "dfs_vdisks.replicas is the journal replica set: the nodes an append must "
            "reach before the write is acknowledged, which is what makes the takeover "
            "proof three lines -- fencing one replica stops the old owner because it "
            "needed all of them, and reading one replica sees every acknowledged write. "
            "dfs_egroup_replicas is a table rather than a list column because Purah "
            "queries it by node when a host is lost, and a collection column cannot be "
            "indexed that way without a secondary index nobody wants on the hot path."
        ),
        "statements": [
            # Nodes that must acknowledge a journal append. Ordered, and the owner is not
            # necessarily a member: a vdisk whose owner holds no local replica is the
            # normal state immediately after a failover, and it stays correct -- just
            # slower -- until Purah rebuilds locality.
            "ALTER TABLE hydra.dfs_vdisks ADD replicas list<text>;",
            # Redundancy factor at creation, copied from the container's ftt so a later
            # change to the container does not silently re-interpret the write-all set of
            # a vdisk that already exists.
            "ALTER TABLE hydra.dfs_vdisks ADD rf int;",
            # Which nodes hold a copy of an extent group. Keyed by egroup with the node as
            # the clustering column, so a group's replicas are one partition read; the
            # by-node direction Purah needs after a host is lost is a full scan, which is
            # what a curator pass already is.
            "CREATE TABLE IF NOT EXISTS hydra.dfs_egroup_replicas ( egroup_id text, node text, path text, state text, PRIMARY KEY ((egroup_id), node) );",
        ],
    },
    {
        "id": "0007-dfs-snapshots",
        "statements": [
            # The identity an extent's footer was stamped with when it was written.
            #
            # A footer carries the vdisk hash and the extent index, so a correct checksum
            # proves the bytes are undamaged and the identity proves they are the *right*
            # bytes. Reads verified that against the reading vdisk's own hash, which is
            # the same thing right up until extents are legitimately shared -- and a
            # snapshot shares every one of them. The first snapshot taken returned EIO on
            # every read, which is the guard doing its job on a case that had not existed
            # when it was written.
            #
            # Stored per row because one vdisk's map can hold both: a clone inherits its
            # parent's extents and stamps its own on whatever it rewrites.
            #
            # No backfill. A row without this column was written by the vdisk that owns
            # it, so the reader falls back to its own hash, which is the right answer for
            # every row that predates snapshots.
            "ALTER TABLE hydra.dfs_block_map ADD vdisk_hash bigint;",
        ],
    },
    {
        "id": "0008-container-compression",
        "statements": [
            # Compression is a property of the container, not of a vdisk or of the
            # cluster. A container is already the unit an operator reasons about for tier
            # and quota, and it is the level where the trade-off is actually decided: a
            # container of golden images wants it on, one holding a database's data files
            # usually does not.
            #
            # Read at seal time, not at attach: an extent group is compressed once, when
            # it is sealed, and never rewritten. Turning the flag on therefore applies to
            # what gets sealed next and leaves existing extent groups exactly as they are,
            # which is what makes the change safe to make on a live container -- nothing
            # is rewritten underneath a running guest.
            #
            # Null means off. Every container that predates this column keeps behaving the
            # way it did, and the footer of an uncompressed extent already says so.
            "ALTER TABLE hydra.storage_containers ADD compression text;",

            # Where an uploaded image is placed. Images used to land wherever the default
            # container was, which meant an operator with a container carved out for
            # templates could not actually put templates in it.
            "ALTER TABLE hydra.valhalla_images ADD container text;",
        ],
    },
    {
        "id": "0009-vlan-claims",
        "description": (
            "One row per VLAN id in use, so that 'this VLAN is taken' is a fact the "
            "database can enforce. hydra.gatoway_networks is keyed by net_id, which "
            "makes a VLAN id an ordinary column and a duplicate perfectly legal: both "
            "consoles read the existing networks and refuse a clash, which catches the "
            "mistake an operator actually makes and cannot serialise against a "
            "concurrent create -- two creates a millisecond apart both read 'VLAN 100 "
            "is free' and both write it. A lightweight transaction cannot span "
            "partitions, so the exclusion has to live in a row whose key *is* the VLAN "
            "id, taken with IF NOT EXISTS. Two networks on one VLAN put two guests' "
            "traffic in the same broadcast domain without either operator being told."
        ),
        "statements": [
            # text net_id and an epoch-ms bigint: the convention cluster_locks and
            # urbosa_transit_pool set, and the shape Daruk's serializer returns without
            # type negotiation. `net_id` is the claim's holder token -- a release is
            # conditional on it, so a late cleanup from a create that failed cannot drop
            # the claim a later create legitimately holds.
            #
            # `name` is carried so a refusal can say *which* network holds the VLAN
            # without a second read. It is a copy and may go stale if the network is
            # renamed; it is a label in an error message, never a key.
            "CREATE TABLE IF NOT EXISTS hydra.gatoway_vlan_claims "
            "( vlan_id int PRIMARY KEY, net_id text, name text, claimed_at_ms bigint );",
        ],
        # Runs after the statements, inside the same lock. See the function for why a
        # cluster that already has duplicates is reported rather than refused.
        "backfill": backfill_vlan_claims,
    },
    {
        "id": "0010-vm-graphics",
        "description": (
            "Which console protocol a VM is given, per VM. The graphics device lives in "
            "the domain XML, so it is per-domain by construction -- a cluster-wide "
            "switch would still have to rewrite every domain and restart every guest to "
            "take effect, which is a per-VM change wearing a toggle's clothes. The "
            "WebSocket proxy was already per-VM: it reads the graphics type off the live "
            "domain and refuses a protocol mismatch. This column is the desired state "
            "the domain is built from; the running domain stays the runtime truth."
        ),
        "statements": [
            # Null means VNC. Every VM that predates this column keeps the console it has
            # always had, and no existing domain is rewritten by the migration -- the
            # graphics device is only chosen when a domain is next defined, so switching
            # a VM needs a redefine, which is a restart.
            "ALTER TABLE hydra.vms ADD graphics text;",
        ],
    },
    {
        "id": "0011-catalyst-task-tree",
        "description": (
            "What a task framework has to record about a task, which Catalyst did not. "
            "Three gaps, and the table is where all three start. A task has no parent, "
            "so a rolling upgrade -- which genuinely is a tree, one job per node, each "
            "with steps -- is reported as a flat list of unrelated rows, and the "
            "relationship mipha.py does keep lives inside the JSON payload where no "
            "query can reach it. A task has no component, so 'what has Hylia been "
            "doing' is answerable only by guessing from the queue name of whoever "
            "happened to execute it. And a task has no ordering other than its "
            "millisecond timestamp, so two tasks created in the same millisecond have no "
            "defined order at all and 'the next task after this one' is not a question "
            "the table can answer. Ergon, which this follows, carries a parent uuid, a "
            "component, and a per-component monotonic sequence id on every task."
        ),
        "statements": [
            # ADD IF NOT EXISTS, where 0008 and 0010 used a bare ADD. The difference is
            # that those were one statement each: a migration with several ALTERs that
            # fails on its fourth leaves the first three applied and unrecorded, and a
            # bare ADD raises on the re-run, so the migration could never complete. The
            # ledger makes a second run unusual, not impossible, which is exactly the case
            # idempotence is for.
            #
            # Every column is nullable and nothing rewrites an existing row. A task
            # recorded before this migration keeps reading correctly: no parent, no
            # component, no sequence -- which is the truth about it, and is what the
            # console already renders for a task whose payload names no parent.
            "ALTER TABLE hydra.catalyst_tasks ADD IF NOT EXISTS parent_task_id uuid;",
            # The component that *owns* the task, which is not `service`. `service` is the
            # queue a task is dispatched through -- the executor -- and the two differ
            # exactly where the hierarchy matters: a Hylia upgrade step runs on the
            # `dagur` queue, and recording it as a Dagur task loses who asked for it.
            "ALTER TABLE hydra.catalyst_tasks ADD IF NOT EXISTS component text;",
            # bigint, and per component. Not a cluster-wide counter: a single sequence
            # would have to be claimed by every submitter in the cluster for every task,
            # which is a contended compare-and-swap on the submission path. Per component
            # the contention is between that component's own submitters, and the number
            # means what an operator reads it as -- the n-th thing this component did.
            "ALTER TABLE hydra.catalyst_tasks ADD IF NOT EXISTS sequence_id bigint;",
            # The *kind* of task, as distinct from the verb the executor was handed.
            # `action` is what the worker switches on and must keep its exact spelling;
            # `task_type` is what the task is, and a parent whose children are all
            # `action = 'execute'` has nothing else to say what each one did.
            "ALTER TABLE hydra.catalyst_tasks ADD IF NOT EXISTS task_type text;",
            # When it finished, which `updated_at` cannot answer: that column moves on
            # every progress report, so the duration of a completed task was unknowable
            # from the row even though both ends of it had been written.
            "ALTER TABLE hydra.catalyst_tasks ADD IF NOT EXISTS completed_at timestamp;",
            # The sequence allocator. One row per component, holding the next number to
            # hand out, claimed with a compare-and-swap -- see claim_task_sequence. A
            # blind increment is a lost update, and a lost update here is two tasks
            # carrying the same sequence id, which is the one thing the column exists to
            # make impossible.
            "CREATE TABLE IF NOT EXISTS hydra.catalyst_task_sequence "
            "( component text PRIMARY KEY, next_sequence_id bigint );",
        ],
    },
    {
        "id": "0012-egroup-access-data",
        "description": (
            "How often each extent group is read and written, and when it was last "
            "touched. Nothing about where data is -- this table is *about* extent "
            "groups rather than part of them, which is what makes it addable at all: "
            "a sealed extent group is immutable and its footer format is fixed, so "
            "access data has nowhere to live except beside it. "
            "Preserves I-3 and I-7 by construction: it holds no egroup_id that is a "
            "reference, so Purah's mark phase does not read it and nothing here can "
            "keep a dead extent group alive or hide a live one. A row whose extent "
            "group has been reclaimed is a stale label, not a dangling pointer, and "
            "the sweep deletes it when it deletes the group. "
            "Prerequisite for the tiering in docs/dfs/multi_disk.md, which cannot "
            "decide what to spill down without somewhere to record what is hot."
        ),
        "statements": [
            # Keyed ((egroup_id), node), one row per observer, for three reasons.
            #
            # First, it makes every row single-writer. The counters are flushed as
            # absolute values rather than increments -- there is no read-modify-write
            # and no counter column -- so two nodes sharing a row would each clobber
            # the other's totals on every flush, and the heat of a group being read on
            # two nodes would read as the heat seen by whichever flushed last.
            #
            # Second, one extent group's accesses are a single-partition read, which is
            # what the ranking pass does per candidate.
            #
            # Third, it is the shape dfs_egroup_replicas already set for "facts about
            # an extent group, per node", and consistency beats novelty.
            #
            # `since_ms` is what makes the counters readable. They are not lifetime
            # totals: they count from when the observing daemon opened the window,
            # which is its start or the first access it saw after a flush of zero. Heat
            # is therefore a *rate* over (updated_at_ms - since_ms), which is what
            # tiering wants anyway, and a restart resets the window visibly instead of
            # silently losing counts.
            #
            # bigint epoch-ms and text ids: the convention every other dfs_* table
            # uses, and the shape Daruk's serializer and the Rust client both handle
            # without type negotiation.
            "CREATE TABLE IF NOT EXISTS hydra.dfs_egroup_access "
            "( egroup_id text, node text, reads bigint, writes bigint, "
            "bytes_read bigint, bytes_written bigint, last_read_ms bigint, "
            "last_write_ms bigint, since_ms bigint, updated_at_ms bigint, "
            "PRIMARY KEY ((egroup_id), node) );",
        ],
    },
]


# -- Catalyst task rows -------------------------------------------------------------------
#
# Written from four places: catalyst.py's submit and update handlers, spectrum_server.py's
# log_catalyst_task, and mipha.py's multi-step workflows. They were four independent column
# lists, which is how `parent_task_id` came to live inside the JSON payload in one of them
# and nowhere else. The statements are built here for the same reason claim_vlan_statement
# is: a row one writer creates has to be a row the others can read and complete.

CATALYST_TASKS_TABLE = "hydra.catalyst_tasks"
TASK_SEQUENCE_TABLE = "hydra.catalyst_task_sequence"

# What a task may be. `processing` is the one that matters at recovery: it means a worker
# had picked the task up, so its effect on the cluster is unknown.
TASK_PENDING = "pending"
TASK_PROCESSING = "processing"
TASK_COMPLETED = "completed"
TASK_FAILED = "failed"
TASK_TERMINAL = (TASK_COMPLETED, TASK_FAILED)
TASK_OPEN = (TASK_PENDING, TASK_PROCESSING)

# The columns a reader needs. Here so the console, the CLI and the recovery pass do not
# each maintain their own list and discover a new column by noticing it is missing.
TASK_COLUMNS = (
    "task_id", "parent_task_id", "component", "sequence_id", "service", "action",
    "task_type", "status", "payload", "progress", "error_msg", "created_at", "updated_at",
    "completed_at",
)


def _uuid_or_null(value):
    """A uuid literal, unquoted, or NULL.

    CQL uuids are written bare -- quoting one is a type error, not a string -- so a missing
    parent has to become the keyword and not an empty literal. Anything that is not a
    plausible uuid becomes NULL rather than being interpolated: this builds statement text,
    and a parent id arrives from a request body.
    """
    text = str(value or "").strip()
    if not text:
        return "NULL"
    stripped = text.replace("-", "")
    if len(stripped) != 32:
        return "NULL"
    try:
        int(stripped, 16)
    except ValueError:
        return "NULL"
    return text


def _int_or_null(value):
    if value is None:
        return "NULL"
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return "NULL"


def _text_or_null(value):
    if value is None or value == "":
        return "NULL"
    return quote(value)


def task_insert_statement(task_id, service, action, payload_json, now_ms,
                          component=None, task_type=None, sequence_id=None,
                          parent_task_id=None, status=TASK_PENDING, progress=0):
    """The row a submission writes.

    `component` defaults to the service and `task_type` to the action, so a caller that
    knows nothing new still produces a row with every column populated with something true
    rather than with a null that a reader has to interpret.
    """
    return (
        "INSERT INTO %s (task_id, parent_task_id, component, sequence_id, service, action, "
        "task_type, status, payload, progress, created_at, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %d, %d);"
        % (CATALYST_TASKS_TABLE, _uuid_or_null(task_id), _uuid_or_null(parent_task_id),
           _text_or_null(component or service), _int_or_null(sequence_id),
           _text_or_null(service), _text_or_null(action),
           _text_or_null(task_type or action), _text_or_null(status),
           quote(payload_json), int(progress or 0), int(now_ms), int(now_ms)))


def task_update_statement(task_id, status, progress, now_ms, error_msg=None):
    """A progress report or a verdict.

    `completed_at` is written only for a terminal status, and `updated_at` on every call.
    Collapsing them is what made a completed task's duration unknowable: `updated_at`
    moves on every progress report, so by the time a task finished its start was the only
    timestamp still meaning what it said.
    """
    sets = ["status = %s" % _text_or_null(status) if status else "status = NULL",
            "progress = %d" % int(progress or 0),
            "updated_at = %d" % int(now_ms)]
    if error_msg:
        sets.append("error_msg = %s" % quote(error_msg))
    if status in TASK_TERMINAL:
        sets.append("completed_at = %d" % int(now_ms))
    return ("UPDATE %s SET %s WHERE task_id = %s;"
            % (CATALYST_TASKS_TABLE, ", ".join(sets), _uuid_or_null(task_id)))


def task_read_statement(limit=None):
    """Every column of every task. A full scan, because `task_id` is the whole primary key
    and there is no clustering order to read in -- the retention window in 0003 bounds what
    the scan walks, and that is all it can do."""
    tail = " LIMIT %d" % int(limit) if limit else ""
    return "SELECT JSON %s FROM %s%s;" % (", ".join(TASK_COLUMNS), CATALYST_TASKS_TABLE, tail)


def claim_task_sequence(lwt, component, expected=None, attempts=6):
    """The next sequence id for `component`, or None if one could not be claimed.

    `lwt` is the caller's own `run_lwt`, returning `(ok, applied, current, error)`. The
    claim is a compare-and-swap on the counter row: propose one more than the value this
    caller believes the row holds, and on a refusal take the value that beat it out of the
    refusal itself rather than spending a read on it. A refused claim is not an error -- it
    means another submitter got this number -- so it is retried, and only a genuine failure
    or an exhausted retry budget gives up.

    `expected` is a hint, not a requirement: a caller that allocated for this component
    before passes what it got, which makes the steady state one round trip. Passing None
    (or passing it wrong) costs one refusal and is otherwise harmless, because the refusal
    is what carries the right answer back.

    None is a legitimate answer and must not be fatal. A submission whose sequence could
    not be claimed is still a task that has to be queued: the number is how an operator
    orders a component's history, not how the cluster executes it. Refusing the task
    instead would make the task framework fail whenever the counter is contended.

    Note on the first claim: `IF next_sequence_id = NULL` matches a row that does not exist
    yet, which is the same property /v1/schedule/claim-job relies on for a schedule whose
    clock has never been written. So no row has to be seeded, and two nodes reaching a
    fresh component at once still produce two different numbers.
    """
    try:
        expected = int(expected) if expected is not None else None
    except (TypeError, ValueError):
        expected = None
    for _attempt in range(max(1, int(attempts))):
        proposed = (expected or 0) + 1
        ok, applied, current, _error = lwt("/v1/catalyst/claim-sequence", {
            "component": component,
            "next_sequence_id": proposed,
            "expected_sequence_id": expected,
        })
        if not ok:
            return None
        if applied:
            return proposed
        expected = (current or {}).get("next_sequence_id")
        if expected is not None:
            try:
                expected = int(expected)
            except (TypeError, ValueError):
                return None
    return None


def checksum(migration):
    """A stable digest of what a migration actually does.

    Recorded when the migration is applied and compared on every later run. It catches
    the mistake that is otherwise invisible: editing a migration that has already
    shipped. The cluster that ran the old text and the cluster that ran the new one now
    have different schemas and both believe they are up to date.

    Only `statements` is hashed, plus the *name* of a backfill step when there is one.
    The description is prose and may be improved freely.

    Hashing a backfill by name and not by body is deliberate, and it is a constraint on
    how one may be written rather than an oversight: a backfill derives rows that already
    exist from rows that already exist, so re-running it must be a no-op and changing what
    it derives is a new migration, not an edit to this one. Nothing here would notice such
    an edit, which is why the rule is stated instead of enforced.

    Migrations with no backfill hash exactly as they did before the key existed, so the
    checksums recorded on every live cluster still match.
    """
    import hashlib

    digest = hashlib.sha256()
    for statement in migration["statements"]:
        digest.update(" ".join(statement.split()).encode("utf-8"))
        digest.update(b"\n")
    backfill = migration.get("backfill")
    if backfill is not None:
        digest.update(("backfill:" + backfill.__name__).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def pending(applied):
    """Migrations not yet recorded, in declaration order.

    `applied` maps id -> checksum, as read from `hydra.schema_migrations`. A migration
    whose recorded checksum differs from its current text is *not* returned as pending
    -- re-running it would not fix anything -- it raises, because the divergence needs a
    human.
    """
    out = []
    for migration in MIGRATIONS:
        recorded = applied.get(migration["id"])
        if recorded is None:
            out.append(migration)
            continue
        current = checksum(migration)
        if recorded != current:
            raise SchemaDivergence(migration["id"], recorded, current)
    return out


class SchemaDivergence(Exception):
    """A migration was edited after it had already been applied somewhere."""

    def __init__(self, migration_id, recorded, current):
        self.migration_id = migration_id
        self.recorded = recorded
        self.current = current
        super().__init__(
            "Migration %s was applied with checksum %s but its text now hashes to %s. "
            "Editing an applied migration leaves clusters with different schemas that "
            "both believe they are current. Add a new migration instead, and if this "
            "edit was deliberate, correct hydra.schema_migrations by hand."
            % (migration_id, recorded[:12], current[:12]))


def parse_applied(stdout):
    """Read `id | checksum` rows out of cqlsh's table output.

    Deliberately tolerant: cqlsh prints a header, a rule, blank lines and a row count,
    and none of that is worth being strict about. A line is a row only if it splits on
    '|' into two non-empty halves and the first is not the header.
    """
    applied = {}
    for line in (stdout or "").splitlines():
        stripped = line.strip()
        if not stripped or set(stripped) <= set("-+ "):
            continue
        # cqlsh's row-count footer, "(2 rows)". It has no pipe and splits into exactly
        # two fields, so the space-joined branch below would otherwise read it as a
        # migration named "(2" -- which then never matches anything and quietly makes
        # every real migration look unapplied.
        if stripped.startswith("("):
            continue

        if "|" in stripped:
            left, _, right = stripped.partition("|")
            key, value = left.strip(), right.strip()
        else:
            # Not cqlsh. Through Daruk's /query the daemons get row values joined by a
            # space and no header, so the same two columns arrive as:
            #
            #     0001-baseline 1f4c...
            #
            # Reading only the piped form made every migration look unapplied, and the
            # runner would try to reapply the whole list on every single start.
            parts = stripped.split()
            if len(parts) != 2:
                continue
            key, value = parts

        if not key or not value or key == "id":
            continue
        applied[key] = value
    return applied


def lwt_applied(stdout):
    """Whether a lightweight transaction actually took effect.

    cqlsh renders an LWT result as an `[applied]` column holding True or False. A
    rejected LWT is not an error and exits zero, so the return code says nothing --
    reading this is the only way to tell "I took the lock" from "someone else holds it".

    `[applied]` is the first column and the row carries the conditioned columns beside
    it, which is what makes this fiddlier than it looks. Against a real Scylla:

        [applied] | name         | acquired_at                     | holder
       -----------+--------------+---------------------------------+--------
            False | hydra-schema | 2026-08-20 10:00:14.701000+0000 | 10.0.0.1

    and on success the other columns come back null:

        [applied] | name | acquired_at | holder
       -----------+------+-------------+--------
             True | null |        null |   null

    An earlier version compared the whole stripped line to "True", which matched the
    single-column case and nothing else -- so every successful lock acquisition read as a
    lost race, and the caller returned while still holding the lock it had just taken.
    Only the first field is looked at now.
    """
    text = stdout or ""

    if "[applied]" not in text:
        # Not cqlsh. The daemons' own `run_cql_query` proxies to Daruk, which returns
        # decoded row *values* joined by spaces and no column names at all, so the same
        # rejection arrives as the bare string:
        #
        #     False 10.10.102.41
        #
        # There is no marker to look for, only position -- and `[applied]` is always the
        # first value. Reading only the cqlsh form meant every successful acquisition
        # through a daemon was read as a lost race, and the runner returned holding the
        # lock it had just taken. This is the same shape that makes `run_cql_query`
        # unable to express a conditional write at all.
        #
        # Safe because this is only ever called on a statement this module issued with a
        # condition attached; it is not a general result parser.
        first = text.strip().split(None, 1)[0] if text.strip() else ""
        if first in ("True", "true"):
            return True
        if first in ("False", "false"):
            return False
        # Neither shape. Treat as not-applied rather than assuming success: the
        # consequence of guessing wrong is two daemons migrating at once.
        return False

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or "[applied]" in stripped:
            continue
        if set(stripped) <= set("-+ "):
            continue
        first = stripped.split("|", 1)[0].strip()
        if first in ("True", "true"):
            return True
        if first in ("False", "false"):
            return False
    return False


def quote(value):
    """Single-quote a CQL string literal, doubling any embedded quote.

    The values reaching here are node identifiers and migration ids from this file, not
    user input -- but this module builds CQL text, and a helper that is correct is
    cheaper than a comment explaining why it does not need to be.
    """
    return "'" + str(value).replace("'", "''") + "'"


def ensure_schema(execute, node_id="unknown", now_ms=None, report=None):
    """Apply every pending migration, once, cluster-wide.

    Returns the list of migration ids applied by *this* call, which is empty on every
    node that lost the race and on every restart after the first.

    `execute` must be able to run a *conditional* statement and hand back its stdout: the
    schema lock is an IF NOT EXISTS insert and a conditional delete, and a backfill step
    reads the `[applied]` verdict of its own claims. Every daemon's `run_cql_query`
    refuses conditional statements -- correctly, because it cannot report whether the
    condition held -- so the executor passed in here is the unguarded one.

    `report` is called with one line of prose whenever a migration finds something an
    operator has to decide about, which a migration must never decide by itself. It
    defaults to stderr because that is where a daemon's start-up output already goes.

    Raises `SchemaDivergence` if an applied migration's text has changed, and
    `SchemaError` if the database could not be reached or a statement failed.
    """
    if now_ms is None:
        import time

        now_ms = int(time.time() * 1000)

    if report is None:
        def report(message):
            import sys

            sys.stderr.write(str(message).rstrip("\n") + "\n")

    for statement in BOOKKEEPING:
        _run(execute, statement)

    rc, stdout, _stderr = _run(
        execute, "SELECT id, checksum FROM hydra.schema_migrations;")
    outstanding = pending(parse_applied(stdout))
    if not outstanding:
        return []

    if not _acquire_lock(execute, node_id, now_ms):
        # Another node is migrating. That is the system working: it will finish, and
        # this node's next start will find nothing pending. Blocking here would turn a
        # peer's crash into this node's hang.
        return []

    applied_here = []
    try:
        for migration in outstanding:
            for statement in migration["statements"]:
                _run(execute, statement)
            backfill = migration.get("backfill")
            if backfill is not None:
                # Before the migration is recorded, so a backfill that raises leaves the
                # migration pending and the next start repeats it. The statements above
                # are all IF NOT EXISTS or idempotent property changes, which is what
                # makes repeating them free.
                backfill(execute, report, now_ms)
            _run(execute,
                 "INSERT INTO hydra.schema_migrations (id, checksum, applied_at, applied_by) "
                 "VALUES (%s, %s, %d, %s);"
                 % (quote(migration["id"]), quote(checksum(migration)), now_ms,
                    quote(node_id)))
            applied_here.append(migration["id"])
    finally:
        _release_lock(execute, node_id)

    return applied_here


def _acquire_lock(execute, node_id, now_ms):
    statement = (
        "INSERT INTO hydra.schema_lock (name, holder, acquired_at) "
        "VALUES (%s, %s, %d) IF NOT EXISTS USING TTL %d;"
        % (quote(LOCK_NAME), quote(node_id), now_ms, LOCK_TTL_SECONDS))
    _rc, stdout, _stderr = _run(execute, statement)
    return lwt_applied(stdout)


def _release_lock(execute, node_id):
    # Conditional on still being the holder. An unconditional delete would let a node
    # whose TTL had already expired -- and whose lock another node has since taken --
    # release someone else's lock and allow two migrators at once.
    statement = ("DELETE FROM hydra.schema_lock WHERE name = %s IF holder = %s;"
                 % (quote(LOCK_NAME), quote(node_id)))
    try:
        _run(execute, statement)
    except SchemaError:
        # The lock expires on its own. Failing to release it is not worth turning a
        # successful migration into an error.
        pass


class SchemaError(Exception):
    """A statement failed, or the database could not be reached."""


def _run(execute, statement):
    result = execute(statement)
    try:
        rc, stdout, stderr = result
    except (TypeError, ValueError):
        raise SchemaError(
            "execute() must return (rc, stdout, stderr); got %r" % (result,))
    if rc != 0:
        raise SchemaError("%s\n  statement: %s" % ((stderr or stdout or "").strip(),
                                                   statement[:200]))
    return rc, stdout, stderr
