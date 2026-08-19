defmodule SpectrumPhxWeb.AccountsStub do
  @moduledoc """
  Resolves the fixed tokens `SpectrumPhxWeb.ConnCase.log_in/2` issues, so LiveView tests
  can mount authenticated pages without a live ScyllaDB. Test environment only.
  """

  @prefix "test-session-"

  def user_from_token(@prefix <> username) when byte_size(username) > 0, do: {:ok, username}
  def user_from_token(_), do: {:error, :not_found}
end
