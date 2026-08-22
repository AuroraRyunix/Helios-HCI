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

  @doc """
  Authenticate a username and password.

  Returns `{:ok, username}` or `{:error, reason}`. A missing user still runs a dummy
  verification, so response time does not reveal whether the account exists.
  """
  def authenticate(username, password) when is_binary(username) and is_binary(password) do
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

  def authenticate(_, _), do: {:error, :invalid_credentials}

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
  Resolve a session token to a username.

  The token is checked against the generated format *before* it reaches a query. The
  Python tier concatenated it straight into CQL, reachable pre-authentication from a
  header, a cookie or a query parameter.
  """
  def user_from_token(token) when is_binary(token) do
    if valid_token_format?(token) do
      case Hydra.query("SELECT username FROM hydra.sessions WHERE session_token = ?", [token]) do
        {:ok, [%{"username" => username} | _]} when is_binary(username) -> {:ok, username}
        {:ok, _} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, :malformed_token}
    end
  end

  def user_from_token(_), do: {:error, :malformed_token}

  @doc "Delete a session. Always returns :ok -- logout must not fail on a database blip."
  def delete_session(token) when is_binary(token) do
    if valid_token_format?(token) do
      Hydra.query("DELETE FROM hydra.sessions WHERE session_token = ?", [token])
    end

    :ok
  end

  def delete_session(_), do: :ok

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
end
