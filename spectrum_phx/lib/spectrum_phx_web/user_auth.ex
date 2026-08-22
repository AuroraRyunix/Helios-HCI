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

  @doc "Attach a freshly created session to the connection."
  def log_in_user(conn, username) do
    case Accounts.create_session(username) do
      {:ok, token} ->
        conn
        |> renew_session()
        |> put_session(@session_key, token)
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
    conn |> get_session(@session_key) |> Accounts.delete_session()

    conn
    |> renew_session()
    |> redirect(to: ~p"/login")
  end

  @doc "Resolve the session token on every request."
  def fetch_current_user(conn, _opts) do
    token = get_session(conn, @session_key)

    case token && accounts().user_from_token(token) do
      {:ok, username} -> assign(conn, :current_username, username)
      _ -> assign(conn, :current_username, nil)
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
