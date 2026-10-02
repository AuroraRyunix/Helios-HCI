#!/usr/bin/env python3
"""A VM's console protocol is a property of the VM, and three layers have to agree on it.

SPICE had been half-present for a long time: the vendored client was in the tree and
compiled to WebAssembly on every rollout, `agahnim` bridged the protocol, and Spectrum's
WebSocket proxy was already parameterised by `console_type` and already refused a
mismatch. What was missing was at the two ends -- no domain was ever given a SPICE
device, and no page loaded the client -- and the gate on fixing either was a decision:
whether SPICE is a cluster-wide default or a per-VM choice.

It is per-VM, because the graphics device lives in the domain XML and is therefore
per-domain by construction. A cluster-wide switch would still have to rewrite every
domain and restart every guest before it meant anything, which is a per-VM change wearing
a toggle's clothes -- and the proxy was already per-VM, so the stored column was the only
part that did not exist.

What is asserted here is the agreement, because the parts are in four languages and the
failure mode when they drift is quiet: a console the UI calls SPICE and the daemon builds
as VNC opens on a protocol-mismatch refusal, which reads as a broken console rather than
as a disagreement about what the VM is.

Run with:  python -m unittest test_vm_graphics
"""

import importlib.util
import io
import os
import re
import sys
import unittest
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))

# Every file that decides what counts as a SPICE console.
NORMALISERS = (
    "spectrum_server.py",
    "vali.py",
    os.path.join("spectrum_phx", "lib", "spectrum_phx", "vms", "vm.ex"),
)


def read(name):
    with io.open(os.path.join(HERE, name), encoding="utf-8") as handle:
        return handle.read()


def load(name):
    spec = importlib.util.spec_from_file_location(
        "under_test_" + name.replace(".py", ""), os.path.join(HERE, name))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TheTwoBuildersAgree(unittest.TestCase):
    """`generate_vm_xml` exists twice, in spectrum_server and in vali.

    The duplication predates this change and is not what is being fixed. What matters is
    that a VM defined by the console and the *same VM* redefined by vali after a migration
    comes back with the same console -- otherwise migrating a guest silently changes the
    protocol its console speaks.
    """

    def setUp(self):
        self.spectrum = load("spectrum_server.py")
        self.vali = load("vali.py")

    def build(self, graphics):
        return (
            self.spectrum.generate_vm_xml(
                "vm", "uuid-1", 2048, 2, "bios", [], "", graphics=graphics),
            self.vali.generate_vm_xml(
                "vm", 2048, 2, "bios", [], "", graphics=graphics),
        )

    @staticmethod
    def console_devices(xml):
        root = ET.fromstring(xml)
        graphics = [
            (e.get("type"), e.get("listen"), [c.tag for c in e])
            for e in root.iter("graphics")
        ]
        channels = sorted(c.get("type") for c in root.iter("channel"))
        video = [m.get("type") for m in root.iter("model") if m.get("primary") == "yes"]
        return graphics, channels, video

    def test_both_builders_produce_the_same_console(self):
        for graphics in ("vnc", "spice"):
            a, b = (self.console_devices(x) for x in self.build(graphics))
            self.assertEqual(a, b, "the two builders disagree for graphics=%s" % graphics)

    def test_the_domain_is_well_formed_for_every_input(self):
        """A graphics value reaches this from a request body. An unparseable domain is a
        define that fails at start time, long after the create answered 201."""
        for graphics in ("vnc", "spice", "SPICE", " spice ", None, "", "qxl", "../etc"):
            for xml in self.build(graphics):
                ET.fromstring(xml)

    def test_exactly_one_graphics_device(self):
        """Not both at once.

        docs/vali.md claimed for a long time that "Both VNC and SPICE graphic displays are
        enabled concurrently", which was never true of the code. It is not made true here
        either: two graphics servers on every guest doubles the listening surface on a port
        that takes no password, to spare a restart when a console is switched.
        """
        for graphics in ("vnc", "spice"):
            for xml in self.build(graphics):
                self.assertEqual(len(ET.fromstring(xml).findall(".//graphics")), 1)

    def test_spice_keeps_the_virtio_adapter_and_adds_an_agent_channel(self):
        """Not QXL, which is the obvious pairing and the wrong one here.

        docs/vali.md records that QXL is avoided on these hosts because the BIOS ROM files
        it needs are absent from the EL 10.2 repositories, and a video model whose ROM is
        missing is a domain that does not start. The client does not need it: spice-html5
        decodes the SPICE display channel, which QEMU serves whatever adapter is behind it.
        """
        for xml in self.build("spice"):
            graphics, channels, video = self.console_devices(xml)
            self.assertEqual(graphics[0][0], "spice")
            self.assertEqual(video, ["virtio"])
            self.assertIn("spicevmc", channels)

    def test_the_video_adapter_never_changes_with_the_console(self):
        """The adapter is the part guests have drivers for. Switching a console should not
        hand the guest different hardware."""
        vnc_video = self.console_devices(self.build("vnc")[0])[2]
        spice_video = self.console_devices(self.build("spice")[0])[2]
        self.assertEqual(vnc_video, spice_video)

    def test_vnc_is_untouched_by_any_of_this(self):
        """Every VM in the cluster is VNC today. The shape of the device they get must not
        move, because it is the one that is known to work."""
        for xml in self.build("vnc"):
            graphics, channels, video = self.console_devices(xml)
            self.assertEqual(graphics[0][0], "vnc")
            self.assertEqual(video, ["virtio"])
            self.assertNotIn("spicevmc", channels)

    def test_an_unknown_protocol_becomes_vnc_rather_than_a_broken_domain(self):
        for graphics in ("qxl", "rdp", "", None, "spice-but-not-really", 17):
            for xml in self.build(graphics):
                self.assertEqual(
                    ET.fromstring(xml).find(".//graphics").get("type"), "vnc",
                    "graphics=%r produced something other than a VNC fallback" % (graphics,))


class EveryLayerNarrowsItTheSameWay(unittest.TestCase):
    def test_the_python_copies_agree(self):
        spectrum, vali = load("spectrum_server.py"), load("vali.py")
        for value in ("spice", "SPICE", " spice ", "Spice", "vnc", "VNC", None, "",
                      "qxl", "sp ice", 0, 17, True):
            self.assertEqual(
                spectrum.normalise_graphics(value), vali.normalise_graphics(value),
                "the two copies disagree about %r" % (value,))

    def test_only_the_exact_word_counts(self):
        normalise = load("spectrum_server.py").normalise_graphics
        self.assertEqual(normalise("spice"), "spice")
        self.assertEqual(normalise("  SPICE  "), "spice")
        for not_spice in ("vnc", None, "", "spicy", "spice2", "qxl", "s p i c e"):
            self.assertEqual(normalise(not_spice), "vnc", repr(not_spice))

    def test_the_elixir_copy_applies_the_same_rule(self):
        """Read rather than executed -- there is no Elixir in this suite. The property is
        that it trims, downcases, compares against exactly "spice", and that every other
        clause answers "vnc"."""
        source = read(NORMALISERS[2])
        match = re.search(
            r"defp normalise_graphics\(value\) when is_binary\(value\) do(.*?)\n  end",
            source, re.S)
        self.assertTrue(match, "vm.ex has no binary clause for normalise_graphics")
        body = match.group(1)
        for required in ("String.downcase", "String.trim", '"spice"', '"vnc"'):
            self.assertIn(required, body)
        self.assertIn('defp normalise_graphics(_other), do: "vnc"', source,
                      "a non-binary graphics value does not fall back to VNC")

    def test_nobody_rolls_their_own_comparison(self):
        """The inline form this replaced. Four copies of a string comparison is how the
        layers drift apart, and it is the comparison itself that has to be shared."""
        for name in ("spectrum_server.py", "vali.py"):
            source = read(name)
            inline = re.findall(
                r'str\(\s*graphics[^)]*\)\s*\.strip\(\)\.lower\(\)\s*==\s*"spice"', source)
            self.assertEqual(
                inline, [],
                "%s compares a graphics value by hand instead of calling "
                "normalise_graphics" % name)


class TheColumnIsCarriedEndToEnd(unittest.TestCase):
    """The column is read and written by four places, and a reader that does not select it
    sees None -- which normalises to VNC and therefore looks like a working default rather
    than a missing field."""

    def test_the_migration_adds_it(self):
        schema = read("helios_schema.py")
        self.assertIn('"id": "0010-vm-graphics"', schema)
        self.assertIn("ALTER TABLE hydra.vms ADD graphics text;", schema)

    def test_the_migration_does_not_rewrite_existing_domains(self):
        """Switching a VM's console is a redefine, which is a restart. A migration that
        wrote a value into every row would be claiming to have done something it cannot
        do without stopping every guest in the cluster."""
        schema = read("helios_schema.py")
        block = schema[schema.index('"id": "0010-vm-graphics"'):]
        block = block[: block.index("]", block.index('"statements"'))]
        self.assertNotIn("UPDATE hydra.vms", block)
        self.assertNotIn("backfill", block)

    def test_daruk_inserts_it(self):
        daruk = read("daruk.py")
        columns = re.search(r"_VM_COLUMNS = \((.*?)\)", daruk, re.S).group(1)
        self.assertIn('"graphics"', columns,
                      "a create through Daruk would leave the column null")
        self.assertIn('"graphics": {"type": "text", "default": "vnc"}', daruk)

    def test_the_api_records_what_it_was_asked_for(self):
        server = read("spectrum_server.py")
        self.assertIn('"graphics": graphics', server, "the create does not store it")
        self.assertIn("graphics = '{graphics}'", server, "the update does not store it")

    def test_the_vm_record_the_console_reads_carries_it(self):
        self.assertEqual(
            read("spectrum_server.py").count(
                '"graphics": normalise_graphics(vm.get("graphics"))'),
            2, "a VM record reaches the console without its console protocol")

    def test_the_daemon_that_defines_the_domain_passes_it(self):
        self.assertIn('graphics=vm_data.get("graphics")', read("vali.py"),
                      "vali defines the domain without the stored graphics, so a SPICE VM "
                      "would come back as VNC on its next start")

    def test_the_phoenix_context_selects_it(self):
        vms = read(os.path.join("spectrum_phx", "lib", "spectrum_phx", "vms.ex"))
        columns = re.search(r'@columns "(.*?)"', vms).group(1)
        self.assertIn("graphics", columns.split(", "),
                      "the console's own VM query does not read the column")


class TheConsoleOpensTheRightClient(unittest.TestCase):
    def test_there_is_a_page_that_loads_the_vendored_client(self):
        """Gap two of the three recorded against this console: the client was in the tree
        and nothing served it."""
        page = read(os.path.join("static", "spice_auto.html"))
        self.assertIn("./spice-html5/src/main.js", page)
        self.assertIn("SpiceMainConn", page)

    def test_the_page_asks_for_a_spice_ticket(self):
        page = read(os.path.join("static", "spice_auto.html"))
        self.assertIn("type=spice", page,
                      "the page would be handed a VNC ticket and fail on the handshake")

    def test_the_page_uses_the_same_session_token_as_the_vnc_one(self):
        """Not a second auth path. The console pages are reached with the token the rest of
        the console already stored."""
        page = read(os.path.join("static", "spice_auto.html"))
        self.assertIn("helios_session_token", page)
        self.assertIn("/api/vms/console/token", page)
        self.assertIn("/api/vms/console/ws", page)

    def test_only_apis_the_vendored_client_exports_are_called(self):
        """Every one of these is re-exported from main.js. A name that only looks right
        fails at run time in a page no test opens."""
        page = read(os.path.join("static", "spice_auto.html"))
        exports = re.search(
            r"export \{(.*?)\}", read(os.path.join("static", "spice-html5", "src", "main.js")),
            re.S).group(1)
        exported = {name.strip().rstrip(",") for name in exports.split("\n") if name.strip()}
        for used in re.findall(r"SpiceHtml5\.(\w+)", page):
            self.assertIn(used, exported,
                          "spice_auto.html calls SpiceHtml5.%s, which main.js does not "
                          "export" % used)

    def test_there_is_one_console_button_and_it_follows_the_vm(self):
        """There were two, and the second -- labelled for the SPICE client -- opened the
        same VNC page as the first. A choice the domain cannot honour is not a choice."""
        app = read(os.path.join("static", "app.js"))
        self.assertNotIn("webgl-btn", app, "the duplicate console button is still there")
        self.assertEqual(app.count("console-btn"), 2,
                         "expected exactly one console button and one handler for it")
        self.assertIn("spice_auto.html", app, "no path in the console opens the SPICE page")

    def test_the_phoenix_vm_page_offers_a_console_at_all(self):
        """It did not. Every page is served by Phoenix, and the only console buttons in the
        tree were the two in the legacy app.js -- so an operator on the current console had
        no way to open a guest console."""
        show = read(os.path.join("spectrum_phx", "lib", "spectrum_phx_web", "live", "vms",
                                 "show_live.ex"))
        self.assertIn('defp console_page(%{graphics: "spice"}), do: "spice_auto.html"', show)
        self.assertIn('defp console_page(_vm), do: "vnc_auto.html"', show)
        self.assertIn('id="console"', show)

    def test_the_console_pages_still_reach_the_python_tier(self):
        """Slate routes pages to Phoenix and keeps the `.html` suffix on the Python tier.
        A new console page that the Phoenix rule captured would 404."""
        routing = read("test_console_routing.py")
        self.assertIn("/spice_auto.html", routing,
                      "the routing test does not pin the new console page to the tier that "
                      "serves it")


class TheDocumentationSaysWhatTheCodeDoes(unittest.TestCase):
    def test_vali_no_longer_claims_both_protocols_at_once(self):
        """docs/vali.md said "Both VNC and SPICE graphic displays are enabled
        concurrently". No builder has ever emitted two graphics devices."""
        vali_doc = read(os.path.join("docs", "vali.md"))
        self.assertNotIn("enabled concurrently", vali_doc)

    def test_the_decision_is_written_down(self):
        """Per-VM rather than cluster-wide was the gate on this whole piece of work. The
        reasoning outlives the memory of it."""
        doc = read(os.path.join("docs", "console.md"))
        for required in ("per-VM", "spice_auto.html", "restart"):
            self.assertIn(required, doc)

    def test_the_readme_links_it(self):
        self.assertIn("docs/console.md", read("README.md"))


if __name__ == "__main__":
    unittest.main()
