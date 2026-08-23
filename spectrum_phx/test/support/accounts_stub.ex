defmodule SpectrumPhxWeb.AccountsStub do
  @moduledoc """
  Resolves the fixed tokens `SpectrumPhxWeb.ConnCase.log_in/2` issues, so LiveView tests
  can mount authenticated pages without a live ScyllaDB. Test environment only.
  """

  @prefix "test-session-"

  def user_from_token(@prefix <> username) when byte_size(username) > 0, do: {:ok, username}
  def user_from_token(_), do: {:error, :not_found}

  @doc "Issue the same fixed token `ConnCase.log_in/2` uses, without a database."
  def create_session(username) when is_binary(username), do: {:ok, @prefix <> username}

  @doc "Accepts anything, including the nil of a request that carried no session."
  def delete_session(_token), do: :ok
end
