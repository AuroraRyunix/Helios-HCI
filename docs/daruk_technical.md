# Daruk (ScyllaDB Query Proxy) - Technical Documentation

This document details the internal technical structure, functions, flowcharts, and mindmaps of the Daruk CQL query proxy.

## Technical Mindmap

```mermaid
mindmap
  root((Daruk Query Proxy))
    Consensus Database Connection
      python cassandra-driver Cluster
      LOCAL_IP dynamic resolution (UDP socket)
      default_consistency_level ConsistencyLevel.QUORUM
    Lightweight HTTP server
      127.0.0.1:9043 binding
      HTTPServer & BaseHTTPRequestHandler
    Serialization & Output
      make_serializable helper
      Maps set/list/tuple/dict types to JSON
      Structured success / error JSON payloads
    Query routing
      POST /query handler
      session.execute(raw_cql_string)
    Typed compare-and-swap
      LWT_OPS operation table (CQL owned by the proxy)
      _bind_lwt parameter validation and binding
      _lwt_statement prepared-statement cache at SERIAL
      _split_lwt_result applied / current
```

## Function & Logic Breakdown

### `get_local_ip()`
- Instantiates a UDP socket (`socket.AF_INET`, `socket.SOCK_DGRAM`) and attempts a connection to `10.255.255.255`.
- Returns the local bound interface IP address.
- Fallback: `127.0.0.1`.

### Database Driver Initialization
- `cluster = Cluster([LOCAL_IP])`
- `session = cluster.connect()`
- `session.default_consistency_level = ConsistencyLevel.QUORUM`: Enforces cluster-wide data write and read quorum consistency across nodes for all proxied queries.

### `make_serializable(obj)`
- Recursively traverses Cassandra driver query returns.
- Converts non-serializable datatypes (sets, row tuple subclasses, custom iterable models) into JSON-safe dictionaries and lists.

### `CQLProxyHandler` (HTTP Handler)
- **`POST /query`**:
  1. Decodes post payload as UTF-8 (contains the raw CQL statement string).
  2. Runs statement via the persistent Cassandra driver `session.execute(post_data)`.
  3. Iterates over rows, calling `_asdict()` or extracting `_fields` to serialize data.
  4. Encodes and returns success JSON payload containing data list: `{"status": "success", "rows": [...]}` on port 200.
  5. Catches exceptions and returns error payload: `{"status": "error", "error": "details"}` on port 400.
- All other endpoints or HTTP verbs return a `404 Not Found` response.

### `run()`
- Binds standard `HTTPServer` on `127.0.0.1:9043` to start serving query proxies.

---

## Compare-and-Swap Internals

### `LWT_OPS`
The whole surface of the typed endpoints: a dict keyed by URL path, each entry holding

| Key | Meaning |
| --- | --- |
| `cql` | the statement, owned by the proxy — never assembled from a request |
| `binds` | the ordered names whose values fill the statement's `?` placeholders |
| `params` | the caller-supplied parameters, each with a `type` and either `required` or a `default` |
| `fixed` | values the proxy supplies and a caller cannot name (the migration lock string) |

`binds` may repeat a name — `migrate-lock` binds the lock value twice, once as the value to
write and once as the value to compare against.

### `_bind_lwt(op_path, op, params)`
Validates the request body and returns the bound tuple, or raises `ValueError`.

- **Unknown keys are rejected, not ignored.** A misspelt `expected_host_ip` would otherwise
  fall back to its default and turn the compare-and-swap into an unconditional claim.
- Missing required parameters are rejected. `release` deliberately has no default expected
  owner: a release that matches any owner is the blind write these endpoints replace.
- `bool` is refused where an `int` is due. `isinstance(True, int)` is `True` in Python, so
  an unguarded check would register a VM with one vcpu because a caller sent `true`.
- `null` is accepted only for parameters marked `nullable`, because `IF col = null` is a
  real condition that matches a column which was never written.

### `_lwt_statement(cql)`
Prepares once and caches by statement text. Each prepared statement is pinned to
`ConsistencyLevel.QUORUM` and `serial_consistency_level = ConsistencyLevel.SERIAL`. Without
SERIAL the compare and the swap are not one Paxos round and the endpoint is back to being
the blind write it replaced. LWT results are never retried at a weaker consistency level,
degradable failure or not: a QUORUM failure on a conditional write means we do not know who
owns the row, and guessing is how both sides of a partition come to own the same VM.

### `_split_lwt_result(rows)`
Returns `(applied, current)`.

- Reads the verdict from **column 0 positionally**, guarded by `column_names[0] ==
  "[applied]"`. `ResultSet.was_applied` is deliberately not used: it is only readable
  *before* the result set is iterated and raises `RuntimeError` afterwards, so a handler
  that also wants the conflicting values has to get the order right or crash.
- Raises when there is no `[applied]` column at all, rather than assuming success — see the
  `INSERT ... JSON` note below.

---

## Observed Scylla Behaviour

Verified against the live cluster (Scylla 5.4, `release_version` 3.0.8, python
`cassandra-driver` 3.26.3). These are the behaviours the endpoints are built on.

| Case | Result |
| --- | --- |
| Rejected LWT | `[applied] = false` plus the conditioned columns **as they stand now** |
| Applied LWT | `[applied] = true` plus the same columns holding their **pre-image** |
| `IF status != 'migrating'` against a null `status` | **applies** — a null satisfies `!=` |
| `IF col = ''` against an absent row | does not apply (null is not `''`) |
| `IF col = null` against an absent row | **applies, and creates a partial row** |
| `INSERT ... IF NOT EXISTS`, refused | returns the whole existing row |
| Statement with no `IF` | no rows and `column_names is None` |

The driver reports the column as `[applied]` in `ResultSet.column_names` but sanitises it to
`applied` in each row's `_fields`/`_asdict()`, because `[applied]` is not a valid Python
identifier. Code that matches on the literal `"[applied]"` inside a row dict finds nothing.

> [!WARNING]
> **`INSERT INTO <table> JSON ? IF NOT EXISTS` is accepted by Scylla and then executed
> unconditionally.** It returns no `[applied]` column, no rows, and it overwrites the
> existing row. Appending `IF NOT EXISTS` to a JSON insert therefore *looks* like a fix and
> does nothing. `/v1/vm/create` lists its columns explicitly for this reason, and
> `_split_lwt_result` raises rather than reporting a compare-and-swap that never happened.
