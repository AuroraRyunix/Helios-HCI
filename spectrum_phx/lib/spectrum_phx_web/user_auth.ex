defmodule SpectrumPhxWeb.UserAuth do
  @moduledoc """
  Session handling for both plug pipelines and LiveView mounts.

  Sessions live in `hydra.sessions` and are referenced by an opaque token held in a
  signed, HTTP-only cookie. The token is validated against its generated format before it
  is ever used in a query.
  """
  use SpectrumPhxWeb, :verified_routes

  import Plug.Conn
  import Phoenix.Controller

  alias SpectrumPhx.Accounts

  # Resolved at runtime so tests can supply a stub instead of a live ScyllaDB. Production
  # never overrides it.
  defp accounts, do: Application.get_env(:spectrum_phx, :accounts_module, Accounts)

  @session_key "helios_session_token"

  # The cookie the Python console reads the same token out of.
  #
  # While the rebuild is in progress the two tiers serve different pages of one console,
  # and Slate routes each path to whichever owns it. They already share the session store:
  # `hydra.sessions` rows are identical, and both sides generate the token as 64 lowercase
  # hex characters. What they did not share was the cookie -- this application keeps the
  # token inside its own signed session cookie, and the Python tier looks for a bare
  # `session_id`. Signing in on one tier therefore left the operator signed out on the
  # other, which for a navigation bar that spans both means being bounced to a login page
  # by half the links.
  #
  # So the token is written to both, and read from both. Unsigned, because the other tier
  # expects the raw token; that costs nothing, since the token is opaque, server-side, and
  # revoked by deleting its row.
  #
  # This goes away with the last `:legacy` entry in the navigation table.
  @shared_cookie "session_id"
  @shared_cookie_opts [http_only: true, secure: true, same_site: "Lax", path: "/"]

  @doc "Attach a freshly created session to the connection."
  def log_in_user(conn, username) do
    case accounts().create_session(username) do
      {:ok, token} ->
        conn
        |> renew_session()
        |> put_session(@session_key, token)
        |> put_resp_cookie(@shared_cookie, token, @shared_cookie_opts)
        |> redirect(to: ~p"/")

      {:error, _reason} ->
        conn
        |> put_flash(
          :error,
          "Credentials accepted, but the session could not be stored. Is ScyllaDB reachable?"
        )
        |> redirect(to: ~p"/login")
    end
  end

  @doc "Drop the session server-side and in the cookie."
  def log_out_user(conn) do
    conn
    |> session_token()
    |> accounts().delete_session()

    conn
    |> renew_session()
    |> delete_resp_cookie(@shared_cookie, @shared_cookie_opts)
    |> redirect(to: ~p"/login")
  end

  @doc """
  Resolve the session token on every request.

  A token found only in the shared cookie is adopted into this application's session.
  That is what makes an operator who signed in on the Python tier arrive here already
  signed in: LiveView mounts are handed the session, not the request's cookies, so
  leaving the token in the cookie alone would authenticate the page that renders the
  socket and then refuse the socket itself.
  """
  def fetch_current_user(conn, _opts) do
    token = session_token(conn)

    case token && accounts().user_from_token(token) do
      {:ok, username} ->
        conn
        |> put_session(@session_key, token)
        |> assign(:current_username, username)

      _ ->
        assign(conn, :current_username, nil)
    end
  end

  # This application's session first, then the cookie the other tier writes.
  defp session_token(conn) do
    case get_session(conn, @session_key) do
      nil -> conn |> fetch_cookies() |> Map.fetch!(:cookies) |> Map.get(@shared_cookie)
      token -> token
    end
  end

  @doc "Halt anything that is not signed in."
  def require_authenticated_user(conn, _opts) do
    if conn.assigns[:current_username] do
      conn
    else
      conn
      |> put_flash(:error, "Please sign in to continue.")
      |> redirect(to: ~p"/login")
      |> halt()
    end
  end

  @doc "Send an already-authenticated visitor away from the login page."
  def redirect_if_authenticated(conn, _opts) do
    if conn.assigns[:current_username] do
      conn |> redirect(to: ~p"/") |> halt()
    else
      conn
    end
  end

  @doc """
  LiveView mount hook.

  Every dashboard runs under this, so an expired or forged token cannot reach a mount.
  The check happens once, before the socket connects, rather than inside each view.
  """
  def on_mount(:ensure_authenticated, _params, session, socket) do
    case session[@session_key] && accounts().user_from_token(session[@session_key]) do
      {:ok, username} ->
        {:cont, Phoenix.Component.assign(socket, :current_username, username)}

      _ ->
        {:halt,
         socket
         |> Phoenix.LiveView.put_flash(:error, "Please sign in to continue.")
         |> Phoenix.LiveView.redirect(to: ~p"/login")}
    end
  end

  @doc "The session key the token is stored under."
  def session_key, do: @session_key

  # Rotating the session on login and logout stops a fixated cookie surviving either
  # transition.
  defp renew_session(conn) do
    conn |> configure_session(renew: true) |> clear_session()
  end
end
