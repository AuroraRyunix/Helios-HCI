defmodule SpectrumPhxWeb.NavigationTest do
  use SpectrumPhxWeb.ConnCase, async: true

  alias SpectrumPhxWeb.Layouts

  # The navigation table in Layouts is a plain list of paths, so nothing in the compiler
  # notices when a page is renamed or removed. These walk it against the real router.
  #
  # The table spans two tiers while the rebuild is in progress: `:live` entries are served
  # here, `:legacy` ones by the Python console, with Slate routing each path to the tier
  # that owns it. The two are asserted differently on purpose -- requiring a `:legacy`
  # path to be routable here is what would silently pass if somebody moved an entry to
  # `:live` without building the page.

  describe "the navigation table matches the router" do
    test "every entry this application owns is reachable when signed in", %{conn: conn} do
      for {id, label, path, :live} <- Layouts.live_nav_items() do
        response = conn |> log_in() |> get(path)

        assert html_response(response, 200),
               "#{id} (#{label}) points at #{path}, which the router does not serve"
      end
    end

    test "every entry this application owns is behind authentication", %{conn: _conn} do
      for {_id, _label, path, :live} <- Layouts.live_nav_items() do
        assert redirected_to(get(build_conn(), path)) == "/login",
               "#{path} is in the navigation but reachable while signed out"
      end
    end

    # Slate decides which tier a path reaches, and it sends these to the Python console.
    # A route defined here for the same path would shadow that for anyone already inside
    # the application, so the two tiers would disagree about what the page is.
    test "an entry the other tier serves is not claimed by this router", %{conn: conn} do
      for {_id, _label, path, :legacy} <- Layouts.legacy_nav_items() do
        response = conn |> log_in() |> get(path)

        assert response.status == 404,
               "#{path} is served by this application as well as the Python console"
      end
    end

    test "every entry appears in the rendered header", %{conn: conn} do
      html = conn |> log_in() |> get("/") |> html_response(200)

      for {_id, label, path, _tier} <- Layouts.nav_items() do
        assert html =~ ~s(href="#{path}"), "#{path} is missing from the header"
        assert html =~ label
      end
    end

    # `navigate` asks this application's router for the page, and it has no route for one
    # the Python console serves. It has to be a plain href so the browser actually leaves.
    test "a page on the other tier is an ordinary link, not live navigation", %{conn: conn} do
      html = conn |> log_in() |> get("/") |> html_response(200)

      for {_id, _label, path, :legacy} <- Layouts.legacy_nav_items() do
        link =
          html
          |> String.split("<a")
          |> Enum.find(&String.contains?(&1, ~s(href="#{path}")))

        refute link =~ "data-phx-link", "#{path} is on the other tier but navigates as if it were local"
      end
    end
  end

  describe "the header" do
    test "names the signed-in operator and offers a way out", %{conn: conn} do
      html = conn |> log_in("helios") |> get("/") |> html_response(200)

      assert html =~ ~s(data-role="current-user")
      assert html =~ "helios"
      assert html =~ ~s(href="/logout")
      assert html =~ "Sign out"
    end

    test "marks the current page, and only the current page", %{conn: conn} do
      html = conn |> log_in() |> get("/hosts") |> html_response(200)

      # aria-current is what a screen reader announces; the styling follows it.
      assert length(String.split(html, ~s(aria-current="page"))) == 2

      hosts_link =
        html
        |> String.split("<a")
        |> Enum.find(&String.contains?(&1, ~s(href="/hosts")))

      assert hosts_link =~ ~s(aria-current="page")
    end

    test "carries no scaffolding links", %{conn: conn} do
      html = conn |> log_in() |> get("/") |> html_response(200)

      refute html =~ "phoenixframework.org"
      refute html =~ "Get Started"
    end
  end
end
