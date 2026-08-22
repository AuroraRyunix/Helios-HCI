defmodule SpectrumPhxWeb.SessionController do
  @moduledoc """
  Login and logout.

  These are plain controller actions rather than LiveView events because setting and
  clearing a session cookie requires a real HTTP response; a LiveView socket cannot do it.
  The form is rendered by `SpectrumPhxWeb.LoginLive` and posts here.
  """
  use SpectrumPhxWeb, :controller

  alias SpectrumPhx.Accounts
  alias SpectrumPhxWeb.UserAuth

  def create(conn, %{"username" => username, "password" => password}) do
    case Accounts.authenticate(username, password) do
      {:ok, authenticated} ->
        UserAuth.log_in_user(conn, authenticated)

      {:error, :database_unavailable} ->
        conn
        |> put_flash(:error, "Cannot verify credentials: the database is unreachable.")
        |> redirect(to: ~p"/login")

      {:error, _} ->
        # Deliberately does not distinguish an unknown user from a wrong password.
        conn
        |> put_flash(:error, "Invalid username or password.")
        |> redirect(to: ~p"/login")
    end
  end

  def create(conn, _params) do
    conn
    |> put_flash(:error, "Username and password are required.")
    |> redirect(to: ~p"/login")
  end

  def delete(conn, _params) do
    UserAuth.log_out_user(conn)
  end
end
