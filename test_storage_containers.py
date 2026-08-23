#!/usr/bin/env python3
"""Storage containers: the policy object, and the compression setting it carries.

A container was a row nobody could create. `/api/storage/containers/create` refused with
"there is nothing to create at the storage layer" -- true of *allocation* and wrong about
policy, which is exactly the thing that has to be written down. An operator had whatever
the installer happened to make and no way to add a second.

Compression hangs off the same object, because the container is where the trade-off is
actually decided: a container of golden images wants it on, one holding a database's data
files usually does not.

Run with:  python -m unittest test_storage_containers
"""

import ast
import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SPECTRUM = os.path.join(HERE, "spectrum_server.py")
SCHEMA = os.path.join(HERE, "helios_schema.py")
VALCLI = os.path.join(HERE, "valcli.py")
EXTENT_RS = os.path.join(HERE, "sidon", "src", "extent.rs")
VDISK_RS = os.path.join(HERE, "sidon", "src", "vdisk.rs")


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def load_from(path, names, scope=None):
    """Compile named functions/assignments out of a module that does work at import."""
    tree = ast.parse(read(path), filename=path)
    body = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            body.append(node)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id in names:
                    body.append(node)
    got = set()
    for n in body:
        got.add(n.name if isinstance(n, ast.FunctionDef) else n.targets[0].id)
    missing = names - got
    assert not missing, "%s no longer defines %s" % (os.path.basename(path), missing)
    ns = dict(scope or {})
    exec(compile(ast.Module(body=body, type_ignores=[]), path, "exec"), ns)
    return ns


class CompressionIsAnAllowList(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ns = load_from(
            SPECTRUM,
            {"normalise_compression", "CONTAINER_COMPRESSION_MODES"},
            {"re": re})
        cls.normalise = staticmethod(ns["normalise_compression"])
        cls.modes = ns["CONTAINER_COMPRESSION_MODES"]

    def test_the_shapes_a_ui_actually_sends_all_resolve(self):
        for value, expected in [
            (True, "lz4"), (False, "none"), (None, "none"),
            ("lz4", "lz4"), ("none", "none"),
            ("on", "lz4"), ("off", "none"),
            ("true", "lz4"), ("false", "none"),
            ("yes", "lz4"), ("no", "none"),
            ("LZ4", "lz4"), ("  lz4  ", "lz4"), ("", "none"),
        ]:
            self.assertEqual(
                self.normalise(value), expected,
                f"{value!r} did not resolve to {expected!r}")

    def test_anything_else_is_refused_rather_than_defaulted(self):
        """A typo that silently means "off" is worse than one that is rejected: the
        operator believes compression is on and nothing ever says otherwise."""
        for value in ("zstd", "gzip", "1", "enabled", 7, [], {}):
            self.assertIsNone(
                self.normalise(value),
                f"{value!r} was quietly accepted as a compression setting")

    def test_every_resolved_value_is_one_sidon_will_recognise(self):
        for value in (True, False, None, "on", "off", "lz4", "none"):
            self.assertIn(self.normalise(value), self.modes)


class ContainerNamesAreValidatedNotQuoted(unittest.TestCase):
    """The name is interpolated into CQL. It is validated on the same terms as a VM name."""

    @classmethod
    def setUpClass(cls):
        ns = load_from(SPECTRUM, {"is_valid_container_name", "_CONTAINER_NAME_RE"}, {"re": re})
        cls.valid = staticmethod(ns["is_valid_container_name"])

    def test_ordinary_names_are_accepted(self):
        for name in ("default-pool", "templates", "iso_store", "a", "tier1.fast", "A1"):
            self.assertTrue(self.valid(name), name)

    def test_anything_that_could_reach_cql_is_refused(self):
        for name in (
            "a'; DROP TABLE hydra.vms; --",
            "has space", "-leading", ".leading", "_leading",
            "", None, 7, "x" * 64, "quote'inside", 'double"quote', "semi;colon",
        ):
            self.assertFalse(self.valid(name), repr(name))


class TheSchemaCarriesBothColumns(unittest.TestCase):
    def setUp(self):
        self.source = read(SCHEMA)

    def test_compression_is_added_to_containers(self):
        self.assertIn("ALTER TABLE hydra.storage_containers ADD compression text;", self.source)

    def test_images_record_where_they_were_put(self):
        self.assertIn("ALTER TABLE hydra.valhalla_images ADD container text;", self.source)

    def test_the_migration_has_its_own_id(self):
        self.assertIn('"id": "0008-container-compression"', self.source)

    def test_no_migration_id_is_used_twice(self):
        ids = re.findall(r'"id": "(\d{4}-[a-z0-9-]+)"', self.source)
        self.assertEqual(sorted(ids), sorted(set(ids)), "a migration id is duplicated")


class CreateAndDeleteAreNoLongerStubs(unittest.TestCase):
    def setUp(self):
        self.source = read(SPECTRUM)

    def test_the_refusals_are_gone(self):
        for stub in (
            "There is nothing to create at the storage layer.",
            "so there is nothing at the storage layer to delete.",
        ):
            self.assertNotIn(
                stub, self.source,
                "container management still refuses outright")

    def test_create_writes_the_row_with_its_compression(self):
        self.assertIn(
            "INSERT INTO hydra.storage_containers (name, tier, quota_bytes, path, ftt, compression)",
            self.source)

    def test_delete_refuses_while_vdisks_reference_the_container(self):
        """Deleting the row is trivial; the damage is every vdisk left naming a policy
        that no longer exists."""
        self.assertIn("def container_in_use(", self.source)
        self.assertIn("still holds", self.source)

    def test_update_touches_only_what_was_sent(self):
        """A form that edits the quota must not silently reset compression."""
        body = self.source[self.source.index('"/api/storage/containers/update"'):]
        body = body[:body.index("elif self.path ==", 10)]
        for field in ("tier", "ftt", "compression"):
            self.assertIn(f'if "{field}" in payload:', body,
                          f"{field} is written unconditionally on update")


class UploadedImagesLandWhereTheOperatorSaid(unittest.TestCase):
    def setUp(self):
        self.source = read(SPECTRUM)

    def test_the_upload_takes_a_container(self):
        self.assertIn('query.get("container", [""])[0]', self.source)
        self.assertIn('self.headers.get("X-Container", "")', self.source)

    def test_the_container_is_validated_and_must_exist(self):
        body = self.source[self.source.index('/api/images/upload'):]
        body = body[:body.index("target_container") + 4000]
        self.assertIn("is_valid_container_name(target_container)", body)
        self.assertIn("No storage container named", body)

    def test_the_image_vdisk_is_created_in_it(self):
        self.assertIn("container=target_container", self.source)

    def test_the_row_records_it(self):
        self.assertIn('"container": target_container,', self.source)


class SidonCompressesAtSealTime(unittest.TestCase):
    def test_the_footer_carries_codec_and_original_length(self):
        source = read(EXTENT_RS)
        self.assertIn("pub const COMP_NONE: u8 = 0;", source)
        self.assertIn("pub const COMP_LZ4: u8 = 1;", source)
        self.assertIn("pub fn decode_extent(", source)
        self.assertIn("pub fn encode_extent(", source)

    def test_compression_is_off_by_the_absence_of_a_setting(self):
        """COMP_NONE is zero so every footer written before compression existed already
        reads correctly, with no backfill and no version check."""
        source = read(EXTENT_RS)
        self.assertIn("COMP_NONE: u8 = 0", source)

    def test_the_container_decides_and_a_missing_row_means_off(self):
        source = read(VDISK_RS)
        self.assertIn("fn container_compresses(", source)
        self.assertIn("SELECT compression FROM hydra.storage_containers", source)
        # Fails soft: a preference that cannot be read must not fail an attach.
        self.assertIn("Err(_) => return false", source)

    def test_the_block_map_records_the_stored_length(self):
        """A read seeks by this. Recording the logical length would read the wrong bytes
        for every compressed extent."""
        source = read(VDISK_RS)
        self.assertIn("length: stored_len,", source)
        self.assertNotIn("length: ext_len as u32,", source)


class ValcliSurfacesTheReason(unittest.TestCase):
    def test_an_http_error_body_is_not_thrown_away(self):
        """Spectrum refuses with a sentence saying why. Reporting the status line instead
        turns "still holds 3 vdisks" into "HTTP Error 409: Conflict"."""
        source = read(VALCLI)
        self.assertIn("except urllib.error.HTTPError as e:", source)
        self.assertIn("json.loads(e.read().decode('utf-8'))", source)
        self.assertIn("import urllib.error", source)

    def test_the_container_commands_exist(self):
        source = read(VALCLI)
        for cmd in ("storage.container.create", "storage.container.update",
                    "storage.container.delete"):
            self.assertIn(f'elif cmd == "{cmd}":', source)
            self.assertIn(cmd, source)

    def test_the_listing_shows_compression(self):
        source = read(VALCLI)
        self.assertIn('"Compression"', source)
        self.assertIn("SELECT JSON name, tier, quota_bytes, path, ftt, compression", source)


if __name__ == "__main__":
    unittest.main()
