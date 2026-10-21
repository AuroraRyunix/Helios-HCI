defmodule SpectrumPhx.Accounts do
  @moduledoc """
  Users and sessions, backed by `hydra.users` and `hydra.sessions`.

  The password hash format is fixed by the existing Python tier so both tiers authenticate
  the same accounts during migration: `pbkdf2_sha256$<iterations>$<salt>$<base64>`, where
  the digest is PBKDF2-HMAC-SHA256 over the UTF-8 password with the salt used verbatim as
  bytes. Do not change it in isolation -- a change here locks users out of one tier.
  """
  require Logger
  alias SpectrumPhx.Hydra

  @iterations 100_000
  @digest_length 32
  @token_bytes 32
  @default_session_timeout_minutes 60
  @default_rate_limit 20
  @rate_table :spectrum_auth_rate_limit

  @doc """
  Authenticate a username and password.

  Returns `{:ok, username}`, `{:error, :rate_limited}`, or `{:error, reason}`. A missing
  user still runs a dummy verification, so response time does not reveal whether the
  account exists.
  """
  def authenticate(username, password, opts \\ [])

  def authenticate(username, password, opts)
      when is_binary(username) and is_binary(password) and is_list(opts) do
    rate_key = Keyword.get(opts, :rate_key, username)
    limit = Keyword.get(opts, :rate_limit) || configured_rate_limit()

    case check_rate_limit(rate_key, limit) do
      {:error, :rate_limited} ->
        {:error, :rate_limited}

      :ok ->
        case fetch_hash(username) do
          {:ok, encoded} ->
            if verify_password(password, encoded) do
              {:ok, username}
            else
              {:error, :invalid_credentials}
            end

          {:error, :not_found} ->
            verify_password(password, dummy_hash())
            {:error, :invalid_credentials}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  def authenticate(_, _, _), do: {:error, :invalid_credentials}

  @doc "Check and increment the per-minute login attempt counter in ETS."
  def check_rate_limit(key, limit \\ nil) do
    ensure_rate_table()
    max_attempts = limit || configured_rate_limit()
    minute_bucket = div(System.system_time(:second), 60)
    bucket_key = {to_string(key), minute_bucket}

    count = :ets.update_counter(@rate_table, bucket_key, {2, 1}, {bucket_key, 0})

    if count > max_attempts do
      {:error, :rate_limited}
    else
      :ok
    end
  end

  @doc "Clear all rate-limit counters in ETS."
  def reset_rate_limit do
    ensure_rate_table()
    :ets.delete_all_objects(@rate_table)
    :ok
  end

  defp ensure_rate_table do
    case :ets.whereis(@rate_table) do
      :undefined ->
        try do
          :ets.new(@rate_table, [:named_table, :public, :set, {:write_concurrency, true}])
        rescue
          ArgumentError -> @rate_table
        end

      ref ->
        ref
    end
  end

  @doc "Read `rate_limit` from `hydra.cluster_settings`, defaulting to 20 attempts/minute."
  def configured_rate_limit do
    case safe_query("SELECT value FROM hydra.cluster_settings WHERE key = 'rate_limit'", []) do
      {:ok, [%{"value" => raw} | _]} when is_binary(raw) ->
        case Integer.parse(String.trim(raw)) do
          {n, ""} when n > 0 -> n
          _ -> @default_rate_limit
        end

      _ ->
        @default_rate_limit
    end
  end

  @doc "Read `session_timeout` (in minutes) from `hydra.cluster_settings`, defaulting to 60."
  def configured_session_timeout do
    case safe_query("SELECT value FROM hydra.cluster_settings WHERE key = 'session_timeout'", []) do
      {:ok, [%{"value" => raw} | _]} when is_binary(raw) ->
        case Integer.parse(String.trim(raw)) do
          {n, ""} when n > 0 -> n
          _ -> @default_session_timeout_minutes
        end

      _ ->
        @default_session_timeout_minutes
    end
  end

  defp fetch_hash(username) do
    case Hydra.query("SELECT password_hash FROM hydra.users WHERE username = ?", [username]) do
      {:ok, [%{"password_hash" => hash} | _]} when is_binary(hash) ->
        {:ok, hash}

      {:ok, _} ->
        {:error, :not_found}

      {:error, reason} ->
        Logger.warning("User lookup failed: #{inspect(reason)}")
        {:error, :database_unavailable}
    end
  end

  @doc "Verify a password against an encoded hash, comparing the digest in constant time."
  def verify_password(password, encoded) when is_binary(password) and is_binary(encoded) do
    case String.split(encoded, "$") do
      ["pbkdf2_sha256", iterations, salt, expected] ->
        case Integer.parse(iterations) do
          {rounds, ""} when rounds > 0 ->
            Plug.Crypto.secure_compare(derive(password, salt, rounds), expected)

          _ ->
            false
        end

      _ ->
        false
    end
  end

  def verify_password(_, _), do: false

  @doc "Produce an encoded hash in the format the Python tier also reads."
  def hash_password(password) do
    salt = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    "pbkdf2_sha256$#{@iterations}$#{salt}$#{derive(password, salt, @iterations)}"
  end

  defp derive(password, salt, rounds) do
    :sha256
    |> :crypto.pbkdf2_hmac(password, salt, rounds, @digest_length)
    |> Base.encode64()
  end

  # A correctly shaped hash, so the not-found path does the same work as the found path.
  defp dummy_hash do
    "pbkdf2_sha256$#{@iterations}$0000000000000000$" <>
      Base.encode64(:binary.copy(<<0>>, @digest_length))
  end

  # -- sessions --------------------------------------------------------------

  @doc "Create a session and return its token."
  def create_session(username) do
    token = Base.encode16(:crypto.strong_rand_bytes(@token_bytes), case: :lower)
    now = System.system_time(:millisecond)

    case Hydra.query(
           "INSERT INTO hydra.sessions (session_token, username, created_at) VALUES (?, ?, ?)",
           [token, username, now]
         ) do
      {:ok, _} -> {:ok, token}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Resolve a session token to a username, enforcing `session_timeout` from
  `hydra.cluster_settings` (default 60 minutes) and deleting expired sessions.

  The token is checked against the generated format *before* it reaches a query. The
  Python tier concatenated it straight into CQL, reachable pre-authentication from a
  header, a cookie or a query parameter.
  """
  def user_from_token(token, opts \\ [])

  def user_from_token(token, opts) when is_binary(token) and is_list(opts) do
    if valid_token_format?(token) do
      timeout_minutes = Keyword.get(opts, :timeout_minutes) || configured_session_timeout()
      now_ms = Keyword.get(opts, :now_ms) || System.system_time(:millisecond)

      result =
        case Keyword.get(opts, :session_row) do
          %{} = row -> {:ok, [row]}
          nil -> Hydra.query("SELECT username, created_at FROM hydra.sessions WHERE session_token = ?", [token])
        end

      case result do
        {:ok, [%{"username" => username} = row | _]} when is_binary(username) ->
          if session_expired?(Map.get(row, "created_at"), timeout_minutes, now_ms) do
            _ = delete_session(token)
            {:error, :session_expired}
          else
            {:ok, username}
          end

        {:ok, _} ->
          {:error, :not_found}

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, :malformed_token}
    end
  end

  def user_from_token(_, _), do: {:error, :malformed_token}

  @doc "Whether a session's `created_at` timestamp has exceeded `timeout_minutes`."
  def session_expired?(created_at, timeout_minutes \\ @default_session_timeout_minutes, now_ms \\ System.system_time(:millisecond))

  def session_expired?(nil, _timeout_minutes, _now_ms), do: false

  def session_expired?(%DateTime{} = dt, timeout_minutes, now_ms) do
    session_expired?(DateTime.to_unix(dt, :millisecond), timeout_minutes, now_ms)
  end

  def session_expired?(created_ms, timeout_minutes, now_ms)
      when is_integer(created_ms) and is_integer(timeout_minutes) and timeout_minutes > 0 do
    now_ms - created_ms > timeout_minutes * 60_000
  end

  def session_expired?(_, _, _), do: false

  @doc "Delete a session. Always returns :ok -- logout must not fail on a database blip."
  def delete_session(token) when is_binary(token) do
    if valid_token_format?(token) do
      safe_query("DELETE FROM hydra.sessions WHERE session_token = ?", [token])
    end

    :ok
  end

  def delete_session(_), do: :ok

  @doc """
  Evict all active sessions belonging to `username` from `hydra.sessions`.
  Called when an operator's password is changed or their account is deleted.
  """
  def delete_user_sessions(username, opts \\ []) when is_binary(username) do
    rows =
      case Keyword.get(opts, :sessions) do
        list when is_list(list) ->
          list

        nil ->
          case safe_query("SELECT session_token, username FROM hydra.sessions", []) do
            {:ok, list} when is_list(list) -> list
            _ -> []
          end
      end

    sink = Keyword.get(opts, :sink)

    for row <- rows,
        Map.get(row, "username") == username or Map.get(row, :username) == username do
      token = Map.get(row, "session_token") || Map.get(row, :session_token)

      if is_binary(token) do
        if is_pid(sink), do: send(sink, {:evicted_session, username, token})
        delete_session(token)
      end
    end

    :ok
  end

  @doc "True when the token is exactly what create_session/1 generates."
  def valid_token_format?(token) when is_binary(token) do
    byte_size(token) == @token_bytes * 2 and String.match?(token, ~r/\A[0-9a-f]+\z/)
  end

  def valid_token_format?(_), do: false

  @doc "Number of accounts, used to show a first-run hint on the login page."
  def user_count do
    case Hydra.query("SELECT username FROM hydra.users", [], consistency: :one) do
      {:ok, rows} -> {:ok, length(rows)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp safe_query(statement, params) do
    Hydra.query(statement, params)
  rescue
    exception -> {:error, Exception.message(exception)}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end
end
