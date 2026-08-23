"""The cluster's one CQL query layer.

Every Helios daemon needs to run a statement against Hydra, and until this module existed
each of them carried its own copy of how: fifteen definitions of `run_cql_query` across
fifteen files, most of them byte-identical, none of them importable. That is not merely
repetition. It had three consequences worth stating, because they are why this file exists
rather than being a tidy-up:

**There was nowhere to parameterise.** Every copy accepted `*args, **kwargs` and ignored
them, so no call site could pass a value separately from the statement. That is why the
CQL-injection work had to be done a call site at a time, and why `cql_escape` had to be
remembered by each author rather than being the only way through.

**A fix reached one copy.** Catalyst, Dagur and Mipha grew a guard that refuses
conditional statements; the other twelve files never got it. Four of those twelve contain
lightweight transactions.

**The copies could drift silently.** Nothing compared them, and a behaviour change in one
was invisible from the others.

## The conditional-statement guard

Daruk's `/query` endpoint renders a *rejected* lightweight transaction as its row of
values joined by spaces --

    False 10.10.102.41

-- and returns rc=0, which is indistinguishable from a successful write. A caller using
this path for a compare-and-swap therefore treats every lost race as a win. `run_cql_query`
refuses such statements outright; a caller that genuinely reads the `[applied]` verdict
itself uses `run_conditional_cql_query`, and one that wants the verdict interpreted for it
uses a Daruk typed `/v1/...` endpoint.

DDL is deliberately not caught: `CREATE TABLE IF NOT EXISTS` is not a compare-and-swap and
its result carries nothing a caller needs.

## The cqlsh fallback

When Daruk cannot be reached the statement is piped through `podman exec ... cqlsh`. This
is a fallback for a metadata layer that is down, not a second supported path: cqlsh
executes `;`-separated statements, so anything reaching it must already be trusted text.
That is the strongest argument for parameterising above it rather than escaping into it.
"""

import base64
import json
import re
import socket
import subprocess
import urllib.request

DARUK_URL = "http://127.0.0.1:9043"
DARUK_QUERY_URL = DARUK_URL + "/query"
HYDRA_DB_CONTAINER = "systemd-hydra-db"

# Daruk answers a query in well under this; the fallback below is for when it does not
# answer at all rather than for when it is slow.
QUERY_TIMEOUT_S = 10


class ConditionalStatementError(RuntimeError):
    """A compare-and-swap was handed to the query path, which cannot report one."""


def cql_escape(value):
    """Escape a value for embedding inside a single-quoted CQL string literal.

    Kept because a great deal of existing code builds statements as text. New code should
    prefer a Daruk typed endpoint, which binds values rather than escaping them: escaping
    is only correct if every author remembers to do it, and binding is correct because the
    author cannot express the unsafe thing.
    """
    if value is None:
        return ""
    return str(value).replace("'", "''")


def cql_int(value, default=0):
    """Coerce a numeric field to an integer literal.

    A non-numeric value would otherwise be interpolated unquoted, which is an injection
    with extra steps.
    """
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        try:
            return int(float(str(value).strip()))
        except (TypeError, ValueError, OverflowError):
            return default


def _cql_outside_string_literals(cql_query):
    """The statement with every single-quoted literal blanked out.

    Job output, task error messages and operator-supplied commands all end up inside CQL
    literals here, and any of them can contain the word "if". Searching the raw text for
    the keyword would refuse an ordinary INSERT because a job Dagur ran happened to print
    "check if the volume is mounted". A doubled quote ('') is an escaped quote inside a
    literal, not the end of one.
    """
    out = []
    index = 0
    length = len(cql_query)
    while index < length:
        char = cql_query[index]
        if char != "'":
            out.append(char)
            index += 1
            continue
        index += 1
        while index < length:
            if cql_query[index] == "'":
                if index + 1 < length and cql_query[index + 1] == "'":
                    index += 2
                    continue
                index += 1
                break
            index += 1
        out.append("''")
    return "".join(out)


# A mutating statement whose text carries an IF clause. DDL is excluded on purpose:
# "CREATE TABLE IF NOT EXISTS" is not a compare-and-swap and its result carries nothing a
# caller needs.
_CONDITIONAL_CQL = re.compile(r"\s*(?:insert|update|delete|begin)\b.*\bif\b", re.I | re.S)


def is_conditional_cql(cql_query):
    """True when the statement is a lightweight transaction rather than a plain write."""
    return bool(_CONDITIONAL_CQL.match(_cql_outside_string_literals(cql_query or "")))


def run_cql_query(cql_query, *args, **kwargs):
    """Run a statement whose only interesting outcome is "did it execute".

    Conditional statements are refused rather than run, for the reason in this module's
    docstring: a rejected lightweight transaction is indistinguishable from a successful
    write on this path. The refusal is here rather than in a review comment because the
    bug comes back the moment somebody appends "IF ..." to an existing call and the tests
    still pass.

    Returns `(returncode, stdout, stderr)`.
    """
    if is_conditional_cql(cql_query):
        raise ConditionalStatementError(
            "a conditional statement cannot be run through run_cql_query(): its result "
            "cannot say whether the condition held. Use a Daruk /v1/... endpoint via "
            "run_lwt(), or run_conditional_cql_query() if the caller reads the [applied] "
            "verdict itself. Statement: %s" % " ".join(cql_query.split())[:200])
    return run_conditional_cql_query(cql_query)


def run_conditional_cql_query(cql_query, *args, **kwargs):
    """`run_cql_query` without the conditional-statement guard.

    The only legitimate caller is one that reads the `[applied]` verdict out of stdout
    itself, or one whose statement is idempotent seeding where a lost race means "somebody
    else already did it" and that is the wanted outcome.

    `helios_schema` is the first kind: its schema lock is taken with IF NOT EXISTS and
    released with IF holder = ?, and it parses the verdict positionally, including Daruk's
    space-joined shape. That lock cannot move to a typed endpoint because it runs *before*
    the schema exists -- Daruk would need an operation table entry for a table nothing has
    created yet.
    """
    try:
        request = urllib.request.Request(
            DARUK_QUERY_URL,
            data=cql_query.encode("utf-8"),
            headers={"Content-Type": "text/plain"},
        )
        with urllib.request.urlopen(request, timeout=QUERY_TIMEOUT_S) as response:
            body = json.loads(response.read().decode("utf-8"))
        if body.get("status") == "success":
            return 0, "\n".join(_render_rows(body.get("rows", []))), ""
        return 1, "", body.get("error", "Database query execution error")
    except Exception:
        return _cqlsh_fallback(cql_query)


def _render_rows(rows):
    """Daruk's rows as the lines every caller was written to parse.

    A `SELECT JSON` comes back as one `json` field per row; anything else is the row's
    values joined by spaces, which is also how a rejected lightweight transaction renders
    -- the shape the guard above exists because of.
    """
    lines = []
    for row in rows:
        if isinstance(row, dict):
            if "json" in row:
                lines.append(row["json"])
            else:
                lines.append(" ".join(str(value) for value in row.values()))
        else:
            lines.append(str(row))
    return lines


def _local_ip():
    """This host's address on the route out, for addressing cqlsh.

    A UDP connect to an unroutable address picks the interface the kernel would use
    without sending anything.
    """
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("10.255.255.255", 1))
            return probe.getsockname()[0]
        finally:
            probe.close()
    except Exception:
        return "127.0.0.1"


def _cqlsh_fallback(cql_query):
    """Run the statement through cqlsh, for when Daruk is not answering.

    Base64 through the shell rather than the statement itself: the text contains quotes,
    newlines and semicolons, and none of them should be interpreted twice.
    """
    encoded = base64.b64encode(cql_query.encode("utf-8")).decode("utf-8")
    command = "echo %s | base64 -d | podman exec -i %s cqlsh %s" % (
        encoded,
        HYDRA_DB_CONTAINER,
        _local_ip(),
    )
    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return (
        process.returncode,
        stdout.decode("utf-8", errors="ignore").strip(),
        stderr.decode("utf-8", errors="ignore").strip(),
    )


def parse_replication_factor(text):
    """The number of replicas the hydra keyspace declares, or None if it cannot be read.

    The replication map arrives as a stringified dict whichever way it is fetched -- Daruk
    flattens result rows into space-joined `str(value)`, and the cqlsh fallback prints the
    same shape -- so this reads the pairs out of the text rather than expecting a real
    mapping. The separator is `[:,]` because the driver's own `OrderedMapSerializedKey`
    reprs its pairs as tuples rather than with colons, and the difference is invisible
    until the gate quietly reports "replication factor unknown" and refuses every
    maintenance request.

    `SimpleStrategy` gives `replication_factor` directly. `NetworkTopologyStrategy` spreads
    the factor across datacenters and QUORUM is computed from their sum, so the
    per-datacenter values are added. `LocalStrategy` and `EverywhereStrategy` have no
    replication factor at all and give None.

    Here rather than in three files because Spectrum had its own version that read only
    `replication_factor` -- correct for SimpleStrategy and silently "unknown" under
    NetworkTopologyStrategy, which is the strategy any rack-aware cluster uses.
    """
    if not text:
        return None
    lowered = text.lower()
    if "localstrategy" in lowered or "everywherestrategy" in lowered:
        return None
    pairs = re.findall(r"['\"]([^'\"]+)['\"]\s*[:,]\s*['\"]?(\d+)['\"]?", text)
    factors = {key: int(value) for key, value in pairs if key != "class"}
    if "replication_factor" in factors:
        return factors["replication_factor"]
    if not factors:
        return None
    return sum(factors.values())
