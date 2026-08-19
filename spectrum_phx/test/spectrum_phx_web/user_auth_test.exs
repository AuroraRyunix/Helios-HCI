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

  describe "login form" do
    test "rejects a submission with missing fields" do
      conn = post(build_conn(), "/login", %{"username" => "helios"})
      assert redirected_to(conn) == "/login"
    end
  end
end
