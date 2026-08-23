defmodule SpectrumPhx.Storage.Containers do
  @moduledoc """
  Storage containers: the policy object a vdisk inherits its behaviour from.

  A container is not an allocation. Nothing is carved out when one is created and nothing
  is freed when one is deleted — it names a tier, a quota, a fault-tolerance level and a
  compression setting, and every vdisk that references it inherits them. The Python tier
  refused to create one on exactly that reasoning ("there is nothing to create at the
  storage layer"), which is true of allocation and wrong about policy: policy is precisely
  the thing that has to be written down, and without a way to write it an operator has
  whatever the installer happened to make and no way to add a second.

  ## Compression belongs here and not elsewhere

  Per-vdisk would ask the operator the question once per disk, and the answer almost never
  differs within a container. Per-cluster would be the wrong unit in the other direction:
  a container of golden images is written once and read forever and wants compression on;
  one holding a database's data files usually does not.

  Sidon reads the setting when it opens a vdisk, so a change applies to what gets sealed
  *next*. Nothing is rewritten, no existing extent group is touched, and every extent's
  footer records what it actually is — so a container that is flipped between settings
  stays readable in both directions, and the change is safe to make while guests are
  running.

  ## Writes are parameterised

  Every statement here is prepared with bound parameters. The container name reaches CQL
  and is also compared against vdisk rows; validating *and* binding it means neither the
  shape check nor the driver is load-bearing on its own.

  ## Deleting is refused while anything references it

  Removing the row is trivial. The damage is that every vdisk naming the container keeps
  naming it, and the next thing to read those rows decides for itself what the tier, quota
  and compression meant. So a delete names the vdisks that are in the way instead.
  """

  alias SpectrumPhx.Hydra

  @doc """
  Where container reads come from: `:hydra` (the default) or `{:static, rows}`.

  The same seam `Images` and `Storage` use. Under `{:static, rows}` the writes are no-ops
  that still run every check, so a test drives the validation and the refusals without a
  cluster -- which is the half of this module that can be wrong in a way nobody notices.
  """
  @spec source() :: :hydra | {:static, list()}
  def source, do: Application.get_env(:spectrum_phx, :containers_source, :hydra)

  @list_cql """
  SELECT name, tier, quota_bytes, path, ftt, compression FROM hydra.storage_containers
  """

  @get_cql """
  SELECT name, tier, quota_bytes, path, ftt, compression
  FROM hydra.storage_containers WHERE name = ?
  """

  @insert_cql """
  INSERT INTO hydra.storage_containers (name, tier, quota_bytes, path, ftt, compression)
  VALUES (?, ?, ?, ?, ?, ?)
  """

  @delete_cql "DELETE FROM hydra.storage_containers WHERE name = ?"

  @users_cql "SELECT vdisk_id, container FROM hydra.dfs_vdisks"

  @doc """
  What a container may ask Sidon to do with its extents.

  An allow-list rather than free text: this value reaches the storage daemon, which
  refuses a codec it does not know — and a typo that silently means "off" is worse than
  one that is rejected, because the operator believes compression is on and nothing ever
  says otherwise.
  """
  def compression_modes, do: ~w(none lz4)

  @doc "Storage tiers a container may name."
  def tiers, do: ~w(SSD HDD NVME)

  @name_re ~r/^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$/

  @doc "True only for names that are safe to bind into CQL and compare against vdisk rows."
  def valid_name?(name) when is_binary(name), do: Regex.match?(@name_re, name)
  def valid_name?(_), do: false

  @doc """
  Resolve a compression setting to its stored form, or `:error`.

  Accepts the shapes a form actually sends — a checkbox's `true`/`"on"`, an absent field —
  rather than making every caller normalise them the same way and one of them forget.
  """
  def normalise_compression(nil), do: {:ok, "none"}
  def normalise_compression(false), do: {:ok, "none"}
  def normalise_compression(true), do: {:ok, "lz4"}

  def normalise_compression(value) when is_binary(value) do
    case value |> String.trim() |> String.downcase() do
      v when v in ["", "off", "false", "no"] -> {:ok, "none"}
      v when v in ["on", "true", "yes"] -> {:ok, "lz4"}
      v when v in ["none", "lz4"] -> {:ok, v}
      _ -> :error
    end
  end

  def normalise_compression(_), do: :error

  @doc """
  Every container, newest read available.

  A container written before compression existed has `nil` in that column, which means the
  same thing the column means when it is set to `"none"` — so it is resolved here rather
  than leaving every caller to decide what a null means.
  """
  def list do
    case source() do
      {:static, rows} ->
        {:ok, rows |> Enum.map(&normalise_row/1) |> Enum.sort_by(& &1.name)}

      :hydra ->
        case Hydra.query(@list_cql, []) do
          {:ok, rows} -> {:ok, Enum.map(rows, &normalise_row/1) |> Enum.sort_by(& &1.name)}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc "One container by name, or `{:error, :not_found}`."
  def get(name) do
    with true <- valid_name?(name),
         {:ok, [row | _]} <- rows_for(name) do
      {:ok, normalise_row(row)}
    else
      false -> {:error, :invalid_name}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp rows_for(name) do
    case source() do
      {:static, rows} -> {:ok, Enum.filter(rows, fn r -> r["name"] == name end)}
      :hydra -> Hydra.query(@get_cql, [{"text", name}])
    end
  end

  @doc """
  Create a container.

  `path` is set to the name. It is what the Python tier records and what the console
  displays; a container is not a directory, so this is a label rather than a location.
  """
  def create(attrs) do
    name = attrs[:name] || attrs["name"]
    tier = (attrs[:tier] || attrs["tier"] || "SSD") |> to_string() |> String.upcase()
    quota = to_int(attrs[:quota_bytes] || attrs["quota_bytes"] || 0)
    ftt = to_int(attrs[:ftt] || attrs["ftt"] || 0)

    with :ok <- check_name(name),
         :ok <- check_tier(tier),
         {:ok, compression} <- check_compression(attrs[:compression] || attrs["compression"]),
         :ok <- check_non_negative(quota, "A quota cannot be negative. Use 0 for unlimited."),
         :ok <- check_non_negative(ftt, "Fault tolerance cannot be negative."),
         :ok <- check_absent(name) do
      params = [
        {"text", name},
        {"text", tier},
        {"bigint", quota},
        {"text", name},
        {"int", ftt},
        {"text", compression}
      ]

      case write(@insert_cql, params) do
        {:ok, _} ->
          {:ok,
           %{
             name: name,
             tier: tier,
             quota_bytes: quota,
             ftt: ftt,
             compression: compression,
             path: name
           }}

        {:error, reason} ->
          {:error, "Could not create container: " <> inspect(reason)}
      end
    end
  end

  @doc """
  Change a container's policy. Only the keys present are written.

  A form that edits the quota must not silently reset compression to whatever its own
  default happened to be, so an absent key is "leave it alone" rather than "set it to the
  default".
  """
  def update(name, attrs) do
    with :ok <- check_name(name),
         {:ok, _current} <- get(name),
         {:ok, sets, params} <- build_update(attrs) do
      case sets do
        [] ->
          {:error, "Nothing to change."}

        _ ->
          statement =
            "UPDATE hydra.storage_containers SET " <>
              Enum.join(sets, ", ") <> " WHERE name = ?"

          case write(statement, params ++ [{"text", name}]) do
            {:ok, _} -> {:ok, :updated}
            {:error, reason} -> {:error, "Could not update container: " <> inspect(reason)}
          end
      end
    end
  end

  @doc """
  Delete a container, or say what is still in it.

  Returns `{:error, {:in_use, vdisk_ids}}` rather than a sentence, so the caller decides
  how to say it and a test can assert on the list rather than on prose.
  """
  def delete(name) do
    with :ok <- check_name(name),
         {:ok, users} <- users_of(name) do
      case users do
        [] ->
          case write(@delete_cql, [{"text", name}]) do
            {:ok, _} -> {:ok, :deleted}
            {:error, reason} -> {:error, "Could not delete container: " <> inspect(reason)}
          end

        _ ->
          {:error, {:in_use, users}}
      end
    end
  end

  @doc """
  The vdisks that name this container.

  A vdisk with no container recorded belongs to `default`, which is what Sidon assumes
  when the column is absent — so the two agree about what an unset value means.
  """
  def users_of(name) do
    case vdisk_rows() do
      {:ok, rows} ->
        {:ok,
         rows
         |> Enum.filter(fn row -> (row["container"] || "default") == name end)
         |> Enum.map(& &1["vdisk_id"])
         |> Enum.reject(&is_nil/1)
         |> Enum.sort()}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # -- internals ------------------------------------------------------------------

  # Under `{:static, _}` a write runs every check and then does nothing, so the refusals
  # are what the test observes rather than the row that would have been written.
  defp write(statement, params) do
    case source() do
      {:static, _rows} -> {:ok, []}
      :hydra -> Hydra.query(statement, params)
    end
  end

  # Vdisk rows are read from the same seam, keyed separately so a test can say "this
  # container has vdisks in it" without inventing a container row to match.
  defp vdisk_rows do
    case Application.get_env(:spectrum_phx, :containers_vdisk_source, :hydra) do
      {:static, rows} -> {:ok, rows}
      :hydra -> Hydra.query(@users_cql, [])
    end
  end

  defp normalise_row(row) do
    %{
      name: row["name"],
      tier: row["tier"] || "SSD",
      quota_bytes: row["quota_bytes"] || 0,
      path: row["path"] || row["name"],
      ftt: row["ftt"] || 0,
      compression: row["compression"] || "none"
    }
  end

  defp build_update(attrs) do
    Enum.reduce_while([:tier, :quota_bytes, :ftt, :compression], {:ok, [], []}, fn key,
                                                                                  {:ok, sets,
                                                                                   params} ->
      case fetch(attrs, key) do
        :missing ->
          {:cont, {:ok, sets, params}}

        {:ok, value} ->
          case update_field(key, value) do
            {:ok, fragment, param} -> {:cont, {:ok, sets ++ [fragment], params ++ [param]}}
            {:error, message} -> {:halt, {:error, message}}
          end
      end
    end)
  end

  defp update_field(:tier, value) do
    tier = value |> to_string() |> String.upcase()

    case check_tier(tier) do
      :ok -> {:ok, "tier = ?", {"text", tier}}
      error -> error
    end
  end

  defp update_field(:quota_bytes, value) do
    quota = to_int(value)

    case check_non_negative(quota, "A quota cannot be negative. Use 0 for unlimited.") do
      :ok -> {:ok, "quota_bytes = ?", {"bigint", quota}}
      error -> error
    end
  end

  defp update_field(:ftt, value) do
    ftt = to_int(value)

    case check_non_negative(ftt, "Fault tolerance cannot be negative.") do
      :ok -> {:ok, "ftt = ?", {"int", ftt}}
      error -> error
    end
  end

  defp update_field(:compression, value) do
    case check_compression(value) do
      {:ok, compression} -> {:ok, "compression = ?", {"text", compression}}
      error -> error
    end
  end

  defp fetch(attrs, key) do
    cond do
      Map.has_key?(attrs, key) -> {:ok, Map.get(attrs, key)}
      Map.has_key?(attrs, to_string(key)) -> {:ok, Map.get(attrs, to_string(key))}
      true -> :missing
    end
  end

  defp check_name(name) do
    if valid_name?(name) do
      :ok
    else
      {:error,
       "Invalid container name. A name must be 1-63 characters, start with a letter or " <>
         "digit, and contain only letters, digits, '.', '-' and '_'."}
    end
  end

  defp check_tier(tier) do
    if tier in tiers() do
      :ok
    else
      {:error, "Storage tier must be one of " <> Enum.join(tiers(), ", ") <> "."}
    end
  end

  defp check_compression(value) do
    case normalise_compression(value) do
      {:ok, mode} ->
        {:ok, mode}

      :error ->
        {:error, "Compression must be one of " <> Enum.join(compression_modes(), ", ") <> "."}
    end
  end

  defp check_non_negative(value, message) do
    if is_integer(value) and value >= 0, do: :ok, else: {:error, message}
  end

  defp check_absent(name) do
    case get(name) do
      {:error, :not_found} -> :ok
      {:ok, _} -> {:error, "A storage container named '#{name}' already exists."}
      {:error, reason} -> {:error, "Could not check for an existing container: #{inspect(reason)}"}
    end
  end

  defp to_int(value) when is_integer(value), do: value

  defp to_int(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {n, _} -> n
      :error -> -1
    end
  end

  defp to_int(_), do: -1
end
