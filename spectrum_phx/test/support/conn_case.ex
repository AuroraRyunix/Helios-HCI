defmodule SpectrumPhxWeb.ConnCase do
  @moduledoc """
  This module defines the test case to be used by
  tests that require setting up a connection.

  Such tests rely on `Phoenix.ConnTest` and also
  import other functionality to make it easier
  to build common data structures and query the data layer.

  Finally, if the test case interacts with the database,
  we enable the SQL sandbox, so changes done to the database
  are reverted at the end of every test. If you are using
  PostgreSQL, you can even run database tests asynchronously
  by setting `use SpectrumPhxWeb.ConnCase, async: true`, although
  this option is not recommended for other databases.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      # The default endpoint for testing
      @endpoint SpectrumPhxWeb.Endpoint

      use SpectrumPhxWeb, :verified_routes

      # Import conveniences for testing with connections
      import Plug.Conn
      import Phoenix.ConnTest
      import SpectrumPhxWeb.ConnCase
    end
  end

  setup _tags do
    {:ok, conn: Phoenix.ConnTest.build_conn()}
  end

  @doc """
  Put an authenticated session on the connection.

  Every dashboard sits behind authentication, so a test that mounts one has to be signed
  in. The token is resolved by the stub configured in `config/test.exs`, so this needs no
  database.
  """
  def log_in(conn, username \\ "helios") do
    conn
    |> Phoenix.ConnTest.init_test_session(%{})
    |> Plug.Conn.put_session(SpectrumPhxWeb.UserAuth.session_key(), test_token(username))
  end

  @doc "The token the test stub accepts."
  def test_token(username \\ "helios"), do: "test-session-" <> username
end
