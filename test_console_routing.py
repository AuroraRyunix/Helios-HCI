#!/usr/bin/env python3
"""Which tier of the console serves a given path, and the two files that must agree on it.

The console is being rebuilt in Phoenix a page at a time. While that is in progress two
applications serve it: the rebuilt pages come from `spectrum-phx` on 8444, everything else
from the Python `spectrum` on 8443, and Slate decides per path.

That makes `slate_config/dynamic.yml` and the navigation table in
`SpectrumPhxWeb.Layouts` two halves of one statement. When they disagree the failure is
not a build error, it is a link in the navigation bar that 404s -- or worse, a page that
loads from the tier that was supposed to have been replaced, which looks like it works.

The routing rule is expressed as "Phoenix owns these paths, everything else is still
Python" on purpose. A catch-all pointing at the new tier would send it every API endpoint
and unported page as well, and each would fail the moment it was missed.

Run with:  python -m unittest test_console_routing
"""

import io
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
DYNAMIC = os.path.join(HERE, "slate_config", "dynamic.yml")
LAYOUTS = os.path.join(
    HERE, "spectrum_phx", "lib", "spectrum_phx_web", "components", "layouts.ex")
QUADLET = os.path.join(HERE, "spectrum_phx", "quadlet", "spectrum-phx.container")

PHOENIX_SERVICE = "spectrum-phx"
PYTHON_SERVICE = "spectrum-backend"


def read(path):
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def routers():
    import yaml

    return yaml.safe_load(read(DYNAMIC))["http"]["routers"]


def services():
    import yaml

    return yaml.safe_load(read(DYNAMIC))["http"]["services"]


class Rule(object):
    """Just enough of a Traefik matcher to answer "does this rule match this path?"."""

    def __init__(self, text):
        self.exact = set()
        self.prefixes = []
        for kind, values in re.findall(r"\b(PathPrefix|Path)\(([^)]*)\)", text):
            found = re.findall(r"`([^`]+)`", values)
            if kind == "Path":
                self.exact.update(found)
            else:
                self.prefixes.extend(found)

    def matches(self, path):
        if path in self.exact:
            return True
        return any(path.startswith(prefix) for prefix in self.prefixes)


def phoenix_rule():
    return Rule(routers()["phoenix-ui"]["rule"])


def nav_items():
    """The navigation table, as (id, path, tier)."""
    source = read(LAYOUTS)
    table = source[source.index("@nav ["):]
    table = table[: table.index("]")]
    return [
        (entry[0], entry[2], entry[3])
        for entry in re.findall(
            r"\{:(\w+),\s*\"([^\"]*)\",\s*\"([^\"]*)\",\s*:(\w+)\}", table)
    ]


class TheTwoHalvesAgree(unittest.TestCase):
    def setUp(self):
        self.rule = phoenix_rule()
        self.nav = nav_items()

    def test_the_navigation_table_was_actually_read(self):
        """Every assertion below is vacuously true against an empty table."""
        self.assertTrue(self.nav, "no navigation entries parsed out of layouts.ex")
        tiers = {tier for _, _, tier in self.nav}
        self.assertEqual(tiers, {"live", "legacy"},
                         "the table no longer spans both tiers; this file may be obsolete")

    def test_every_rebuilt_page_is_routed_to_phoenix(self):
        for name, path, tier in self.nav:
            if tier != "live":
                continue
            self.assertTrue(
                self.rule.matches(path),
                "%s (%s) is served by Phoenix, but Slate sends it to the Python tier"
                % (name, path))

    def test_every_page_not_yet_rebuilt_still_goes_to_python(self):
        for name, path, tier in self.nav:
            if tier != "legacy":
                continue
            self.assertFalse(
                self.rule.matches(path),
                "%s (%s) has not been rebuilt, but Slate sends it to Phoenix, which has "
                "no route for it" % (name, path))


class ThePythonTierKeepsEverythingElse(unittest.TestCase):
    def setUp(self):
        self.rule = phoenix_rule()

    def test_the_catch_all_still_points_at_the_python_tier(self):
        """It serves the whole HTTP API and every page not yet rebuilt. Pointing the
        catch-all at Phoenix would 404 all of it the moment something was missed."""
        self.assertEqual(routers()["webui"]["service"], PYTHON_SERVICE)
        self.assertIn("PathPrefix(`/`)", routers()["webui"]["rule"])

    def test_the_api_is_not_captured_by_the_phoenix_rule(self):
        for path in ("/api/login", "/api/v1/vms", "/api/vms/console/ws"):
            self.assertFalse(self.rule.matches(path), "%s would not reach the API" % path)

    def test_the_python_tiers_own_pages_and_assets_are_not_captured(self):
        """The Python tier serves its pages with a `.html` suffix, so a prefix match on a
        page name would swallow them -- `/vms.html` is not `/vms`."""
        for path in ("/vms.html", "/images.html", "/health.html", "/storage.html",
                     "/index.html", "/app.js", "/styles.css", "/vnc_auto.html"):
            self.assertFalse(
                self.rule.matches(path),
                "%s is served by the Python tier but routed to Phoenix" % path)


class ThePartsThatAreNotPages(unittest.TestCase):
    def setUp(self):
        self.rule = phoenix_rule()

    def test_the_liveview_socket_reaches_phoenix(self):
        """Without it every dashboard renders once and then dies, which reads as a
        rendering bug rather than a routing one."""
        self.assertTrue(self.rule.matches("/live/websocket"))
        self.assertTrue(self.rule.matches("/live/longpoll"))

    def test_phoenix_serves_its_own_static_assets(self):
        for path in ("/assets/css/app.css", "/assets/js/app.js", "/images/logo.svg",
                     "/favicon.ico"):
            self.assertTrue(self.rule.matches(path), "%s would 404" % path)

    def test_a_named_guest_reaches_phoenix(self):
        for path in ("/vms/new", "/vms/some-guest"):
            self.assertTrue(self.rule.matches(path))

    def test_the_console_websocket_is_routed_to_agahnim_ahead_of_everything(self):
        console, phoenix, python = (routers()["console-ws"], routers()["phoenix-ui"],
                                    routers()["webui"])
        self.assertEqual(console["service"], "agahnim")
        self.assertGreater(console["priority"], phoenix["priority"])
        self.assertGreater(phoenix["priority"], python["priority"])

    def test_the_priorities_are_stated_rather_than_inferred(self):
        """Traefik orders routers by rule length when they are not stated, so adding a
        path to one rule could reorder the rules."""
        for name, router in routers().items():
            self.assertIn("priority", router, "%s has no explicit priority" % name)


class PhoenixIsReachableOnlyThroughSlate(unittest.TestCase):
    def test_slate_dials_it_over_plain_http_on_loopback(self):
        url = services()[PHOENIX_SERVICE]["loadBalancer"]["servers"][0]["url"]
        self.assertEqual(url, "http://127.0.0.1:8444")

    def test_it_is_bound_to_loopback(self):
        """It does not terminate TLS. Under Network=host the default bind published the
        console as plain HTTP on every interface, storage network included, where a
        session cookie crosses the wire in clear."""
        self.assertIn("Environment=PHX_BIND_IP=127.0.0.1", read(QUADLET))

    def test_no_insecure_transport_is_needed_for_it(self):
        """That flag exists for the Python tier, which terminates its own TLS."""
        self.assertNotIn(
            "serversTransport",
            services()[PHOENIX_SERVICE]["loadBalancer"],
            "Phoenix is being dialled over TLS it does not serve")


class TheVipOnlyLandsWhereTheConsoleWorks(unittest.TestCase):
    """Bifrost holds the VIP only if this node can actually serve what Slate proxies.

    That used to be one backend. Since the split it is two, and the half on 8444 includes
    "/" -- so a node with 8443 up and 8444 down would take the VIP and answer 502 on the
    page every operator lands on.
    """

    def setUp(self):
        self.bifrost = read(os.path.join(HERE, "bifrost.py"))

    def backend_port(self, service):
        url = services()[service]["loadBalancer"]["servers"][0]["url"]
        return int(url.rsplit(":", 1)[1])

    def declared(self, name):
        match = re.search(r"^%s = (\d+)$" % name, self.bifrost, re.M)
        self.assertTrue(match, "bifrost.py no longer declares %s" % name)
        return int(match.group(1))

    def test_it_guards_on_both_console_backends(self):
        self.assertIn("def is_local_spectrum_listening(", self.bifrost)
        self.assertIn("def is_local_spectrum_phx_listening(", self.bifrost)
        self.assertIn("is_local_spectrum_phx_listening()", self.bifrost.split("def is_local_stack_healthy")[1])

    def test_the_guarded_ports_are_the_ports_slate_dials(self):
        """Two files naming the same port independently is how a guard ends up watching
        something nothing serves."""
        self.assertEqual(self.declared("SPECTRUM_PORT"), self.backend_port(PYTHON_SERVICE))
        self.assertEqual(self.declared("SPECTRUM_PHX_PORT"), self.backend_port(PHOENIX_SERVICE))


if __name__ == "__main__":
    unittest.main()
