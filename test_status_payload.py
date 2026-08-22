#!/usr/bin/env python3
"""The contract between the status payload and the console that renders it.

`/api/status` is not a private structure: the dashboard reaches into it field by field.
When Sidon replaced DRBD, the per-pool dict was rewritten around what the extent store
knows -- node, egroups, journal bytes -- and quietly stopped carrying the four fields the
console renders a pool by. The dashboard then threw on `pool.name.startsWith(...)`.

What made that expensive was where the throw surfaced. `fetchStatus` attached its
`.catch` after the `.then` that renders, and a `.catch` in that position also catches
whatever the `.then` throws, so a missing field was reported as *"Lost connection to the
Helios management service. Reconnecting..."*. The management service was answering every
poll with 200 for the whole investigation, on two addresses, while the banner said it was
unreachable.

Both halves are guarded here, because either one alone would let it happen again:

  * the payload has to carry the fields the console reads, and
  * a render that throws must not be reported as a connection failure.

Run with:  python -m unittest test_status_payload
"""

import ast
import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SPECTRUM = os.path.join(HERE, "spectrum_server.py")
APP_JS = os.path.join(HERE, "static", "app.js")

# Read only inside the "Physical Disk" arm, which describes a bare device. Spectrum emits
# no such pool today; the arm is dead code for a Sidon cluster and its fields are not part
# of the contract.
PHYSICAL_DISK_ONLY = {"size"}


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def pool_payload_keys():
    """The literal keys of the dict Spectrum appends to `storage.pools`."""
    tree = ast.parse(read(SPECTRUM), filename=SPECTRUM)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "append"):
            continue
        if not (isinstance(func.value, ast.Name) and func.value.id == "pools"):
            continue
        if not (node.args and isinstance(node.args[0], ast.Dict)):
            continue
        return {k.value for k in node.args[0].keys
                if isinstance(k, ast.Constant) and isinstance(k.value, str)}
    raise AssertionError("spectrum_server.py no longer builds pools with pools.append({...})")


def pool_render_block():
    """The console's `data.storage.pools.forEach` body, split into its two arms."""
    source = read(APP_JS)
    start = source.index("data.storage.pools.forEach(pool => {")
    end = source.index("// Dynamic Host Interfaces binding", start)
    block = source[start:end]
    split = block.index('.startsWith("Physical Disk")')
    physical_end = block.index("} else {", split)
    return block[:split], block[split:physical_end], block[physical_end:]


class PoolPayloadContract(unittest.TestCase):
    def test_console_reads_only_fields_the_payload_carries(self):
        keys = pool_payload_keys()
        shared, physical, card = pool_render_block()
        read_fields = set()
        for chunk in (shared, card):
            read_fields |= set(re.findall(r"pool\.([a-zA-Z_][a-zA-Z0-9_]*)", chunk))
        missing = sorted(read_fields - keys - PHYSICAL_DISK_ONLY)
        self.assertEqual(
            missing, [],
            "the dashboard renders pool fields that /api/status does not send, so each "
            "one reaches the page as 'undefined' or throws: %s" % missing)

    def test_the_fields_the_render_would_throw_on_are_present(self):
        """`name` is called with `.startsWith` and `type` with `.toLowerCase`. Absent, they
        do not render badly -- they abort the whole status render."""
        keys = pool_payload_keys()
        for field in ("name", "type"):
            self.assertIn(
                field, keys,
                f"a pool without '{field}' aborts the dashboard render, because the "
                f"console calls a string method on it")

    def test_physical_disk_arm_is_still_the_only_reader_of_its_own_fields(self):
        """If the card arm starts reading these, they belong in the contract above."""
        _, physical, card = pool_render_block()
        for field in PHYSICAL_DISK_ONLY:
            self.assertIn("pool.%s" % field, physical)
            self.assertNotIn("pool.%s" % field, card)


class RenderFailureIsNotConnectionFailure(unittest.TestCase):
    def test_the_render_is_fenced_off_from_the_fetch_catch(self):
        """The specific defect: `.catch` after `.then` catches the render too."""
        source = read(APP_JS)
        start = source.index("function fetchStatus()")
        body = source[start:source.index("function fetchDrsStatus()", start)]

        self.assertIn(
            "catch (renderErr)", body,
            "fetchStatus no longer fences its render, so a console bug will be reported "
            "as 'Lost connection to the Helios management service' again")

        # The render calls must sit inside the fence, not after it.
        fence = body.index("catch (renderErr)")
        for call in ("updateSharedHeader(data)", "updatePageSpecificContent(data)"):
            self.assertLess(
                body.index(call), fence,
                f"{call} runs outside the render fence, so it can still reach the "
                f"connection-error path")

    def test_only_a_failed_fetch_shows_the_connection_banner(self):
        """`showConnectionError` must stay reachable from exactly one place: the outer
        `.catch`, which is the one that means the service did not answer."""
        source = read(APP_JS)
        self.assertEqual(
            source.count("showConnectionError()"), 2,
            "showConnectionError is defined once and called once; a new call site needs "
            "to prove it means the service is unreachable")


class TheFixHasToReachTheBrowser(unittest.TestCase):
    """`app.js` is served with a `?v=` cache-buster that browsers key their cache on.

    A fix deployed without bumping it reaches the node and stops there: every console
    already open keeps running the cached copy. That happened while diagnosing this bug --
    the corrected file was on the node, served by Spectrum, and the browser went on
    throwing the old exception from cache.

    The version also has to be the *same* everywhere. Two pages sat on `1.1.1` while the
    rest were on `1.1.2`, which means they were caching a separate copy of the same file
    and could be a fix behind the others without anything looking wrong.
    """

    def test_every_page_requests_the_same_app_js_version(self):
        import glob
        versions = {}
        for path in sorted(glob.glob(os.path.join(HERE, "static", "*.html"))):
            found = re.findall(r"app\.js\?v=([0-9.]+)", read(path))
            if found:
                versions[os.path.basename(path)] = set(found)

        self.assertTrue(versions, "no page references app.js with a cache-busting version")
        distinct = set()
        for found in versions.values():
            distinct |= found
        self.assertEqual(
            len(distinct), 1,
            "pages disagree about which app.js version to request, so some of them cache "
            "a different copy of it: %s" % {k: sorted(v) for k, v in versions.items()})


if __name__ == "__main__":
    unittest.main()
