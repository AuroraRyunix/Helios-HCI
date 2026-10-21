defmodule SpectrumPhx.Auth do
  @moduledoc """
  Authentication and session verification facade delegating to `SpectrumPhx.Accounts`.
  """

  alias SpectrumPhx.Accounts

  defdelegate authenticate(username, password, opts \\ []), to: Accounts
  defdelegate check_rate_limit(key, limit \\ nil), to: Accounts
  defdelegate reset_rate_limit(), to: Accounts
  defdelegate configured_rate_limit(), to: Accounts
  defdelegate configured_session_timeout(), to: Accounts
  defdelegate verify_password(password, encoded), to: Accounts
  defdelegate hash_password(password), to: Accounts
  defdelegate create_session(username), to: Accounts
  defdelegate user_from_token(token, opts \\ []), to: Accounts
  defdelegate session_expired?(created_at, timeout_minutes \\ 60, now_ms \\ System.system_time(:millisecond)),
    to: Accounts
  defdelegate delete_session(token), to: Accounts
  defdelegate delete_user_sessions(username, opts \\ []), to: Accounts
  defdelegate valid_token_format?(token), to: Accounts
  defdelegate user_count(), to: Accounts
end
