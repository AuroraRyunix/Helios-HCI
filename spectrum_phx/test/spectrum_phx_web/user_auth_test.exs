defmodule SpectrumPhxWeb.UserAuthTest do
  use SpectrumPhxWeb.ConnCase, async: true

  describe "unauthenticated access" do
    test "every dashboard redirects to the login page", %{conn: conn} do
      # The whole point of the live_session: no dashboard is reachable without a session,
      # and that is enforced in one place rather than per view.
      for path <- ["/", "/hosts", "/vms", "/vms/new", "/vms/some-vm"] do
        conn = get(build_conn(), path)
        assert redirected_to(conn) == "/login", "#{path} should redirect to /login"
      end

      _ = conn
    end

    test "the login page itself is reachable", %{conn: _conn} do
      conn = get(build_conn(), "/login")
      assert html_response(conn, 200) =~ "Sign in"
    end

    test "logout is reachable while signed out, so a stale cookie can be cleared" do
      conn = get(build_conn(), "/logout")
      assert redirected_to(conn) == "/login"
    end
  end

  describe "authenticated access" do
    test "a signed-in session reaches the dashboards", %{conn: conn} do
      for path <- ["/", "/hosts", "/vms"] do
        response = conn |> log_in() |> get(path)
        assert html_response(response, 200)
      end
    end

    test "an already-authenticated visitor is sent away from the login page", %{conn: conn} do
      response = conn |> log_in() |> get("/login")
      assert redirected_to(response) == "/"
    end
  end

  describe "session token handling" do
    test "a forged token does not authenticate", %{conn: conn} do
      response =
        conn
        |> Phoenix.ConnTest.init_test_session(%{})
        |> Plug.Conn.put_session(SpectrumPhxWeb.UserAuth.session_key(), "not-a-real-token")
        |> get("/")

      assert redirected_to(response) == "/login"
    end

    test "an injection-shaped token does not authenticate", %{conn: conn} do
      response =
        conn
        |> Phoenix.ConnTest.init_test_session(%{})
        |> Plug.Conn.put_session(
          SpectrumPhxWeb.UserAuth.session_key(),
          "' OR 1=1; DROP TABLE hydra.sessions--"
        )
        |> get("/")

      assert redirected_to(response) == "/login"
    end
  end

  describe "the session the Python tier also reads" do
    # While the rebuild is in progress the navigation bar spans two tiers, and Slate routes
    # each path to whichever serves it. Both already write the same `hydra.sessions` rows;
    # what they did not share was the cookie, so signing in on one left the operator signed
    # out on the other -- which for a bar spanning both means half the links bounce to a
    # login page.

    test "signing in writes the token where the other tier looks for it", %{conn: conn} do
      response =
        conn
        |> Phoenix.ConnTest.init_test_session(%{})
        |> SpectrumPhxWeb.UserAuth.log_in_user("helios")

      cookie = response.resp_cookies["session_id"]

      assert cookie, "no session_id cookie, so the Python tier sees an anonymous visitor"
      assert cookie.value == get_session(response, SpectrumPhxWeb.UserAuth.session_key())
      assert cookie.http_only, "the session token is readable from JavaScript"
      assert cookie.secure, "the session token may be sent over plain HTTP"
    end

    test "signing out clears it", %{conn: conn} do
      response = conn |> log_in() |> get("/logout")

      cookie = response.resp_cookies["session_id"]

      assert cookie, "the shared cookie is left in place, so the other tier stays signed in"
      assert cookie.max_age == 0 or cookie.value in [nil, ""]
    end

    test "a visitor carrying only the shared cookie is signed in", %{conn: conn} do
      response =
        conn
        |> Phoenix.ConnTest.init_test_session(%{})
        |> put_req_cookie("session_id", test_token())
        |> get("/")

      assert html_response(response, 200)
    end

    test "the token is adopted into this application's session", %{conn: conn} do
      # LiveView mounts are handed the session, not the request's cookies. Leaving the
      # token in the cookie alone authenticates the page that renders the socket and then
      # refuses the socket itself, which shows up as a dashboard that redirects to the
      # login page a moment after it appears.
      response =
        conn
        |> Phoenix.ConnTest.init_test_session(%{})
        |> put_req_cookie("session_id", test_token())
        |> get("/")

      assert get_session(response, SpectrumPhxWeb.UserAuth.session_key()) == test_token()
    end

    test "a rubbish shared cookie is not a session", %{conn: conn} do
      response =
        conn
        |> Phoenix.ConnTest.init_test_session(%{})
        |> put_req_cookie("session_id", "' OR 1=1--")
        |> get("/")

      assert redirected_to(response) == "/login"
    end

    test "this application's own session wins over the cookie", %{conn: conn} do
      # They are normally the same token. If they ever disagree, the signed one is the one
      # this tier issued and the bare one is attacker-supplied.
      response =
        conn
        |> log_in("helios")
        |> put_req_cookie("session_id", test_token("someone-else"))
        |> get("/")

      assert html_response(response, 200) =~ "helios"
    end
  end

  describe "login form" do
    test "rejects a submission with missing fields" do
      conn = post(build_conn(), "/login", %{"username" => "helios"})
      assert redirected_to(conn) == "/login"
    end
  end
end
