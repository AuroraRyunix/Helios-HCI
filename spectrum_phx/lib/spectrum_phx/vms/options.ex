defmodule SpectrumPhx.Vms.Options do
  @moduledoc """
  What the create form offers to choose from: the containers a disk can be placed in, the
  images a CD-ROM can mount, the networks a NIC can join, and whether SPICE is on offer.

  Every one of these was a drop-down in the old console, populated from the same
  catalogues. The port replaced them with free text, so an operator had to know a container
  or a network id by heart, and a typo was found at create time instead of being impossible.

  ## SPICE is offered only when every host can run it

  `docs/console.md` is explicit that a `<graphics type='spice'>` on a QEMU built without
  SPICE is not a degraded console but a domain libvirt refuses to define -- the VM never
  starts. The graphics device is chosen at create time and the VM may be placed on any
  node, so SPICE is on offer only when *every* configured host reports it in
  `/api/v1/host/capabilities`. A host that cannot be read counts as not supporting it: an
  empty or missing list means "do not offer SPICE", wrong in the harmless direction.

  ## Test seam

  Under a static `:vms_source` nothing is queried; `Application.get_env(:spectrum_phx,
  :vm_options)` may supply any of the keys below.
  """

  alias SpectrumPhx.Cluster.Config
  alias SpectrumPhx.Images
  alias SpectrumPhx.Networking
  alias SpectrumPhx.Spark
  alias SpectrumPhx.Storage.Containers
  alias SpectrumPhx.Vms
  alias SpectrumPhx.Vms.Vm

  @doc """
  The choices, as `%{containers: [name], images: [%{name, label}], networks: [%{id, label}],
  spice?: boolean, notes: [string]}`.

  `notes` says what could not be read. A catalogue that failed to load is reported, and
  never drawn as an empty one: "no images" and "the image list is unavailable" are
  different statements.
  """
  def load(opts \\ []) do
    case Vms.source() do
      {:static, _} -> static()
      :hydra -> live(opts)
    end
  end

  @doc "The label an operator reads for a network, matching the old console's wording."
  def network_label(%{kind: :vlan} = net), do: "#{net.name} (VLAN #{net.vlan_id})"

  def network_label(%{kind: :overlay} = net) do
    cidr = if net.subnet_cidr, do: " — #{net.subnet_cidr}", else: ""
    "#{net.name} (Overlay / VNI #{net.vni}#{cidr})"
  end

  def network_label(net), do: "#{net.name} (Direct)"

  defp static do
    defaults = %{
      containers: [Containers.default_name()],
      images: [],
      networks: [fallback_network()],
      spice?: false,
      notes: []
    }

    Map.merge(defaults, Application.get_env(:spectrum_phx, :vm_options, %{}))
  end

  defp live(opts) do
    {containers, c_notes} = containers()
    {images, i_notes} = images()
    {networks, n_notes} = networks()

    %{
      containers: containers,
      images: images,
      networks: networks,
      spice?: if(Keyword.get(opts, :probe_hosts, true), do: spice_everywhere?(), else: false),
      notes: c_notes ++ i_notes ++ n_notes
    }
  end

  defp containers do
    case Containers.list() do
      {:ok, [_ | _] = rows} ->
        {Enum.map(rows, & &1.name), []}

      {:ok, []} ->
        {[Containers.default_name()], []}

      {:error, _reason} ->
        # The default is still a real container the create path will create on demand, so
        # offering it is honest; pretending the others do not exist is what the note is for.
        {[Containers.default_name()], ["The container list could not be read, so only the default is offered."]}
    end
  end

  defp images do
    case Images.list_images() do
      {:ok, rows} ->
        {for(image <- rows, do: %{name: image.name, label: image_label(image)}), []}

      {:error, _reason} ->
        {[], ["The image catalogue could not be read, so no CD-ROM image can be chosen."]}
    end
  end

  defp image_label(%{size_bytes: size} = image) when is_integer(size) and size > 0 do
    "#{image.name} (#{:erlang.float_to_binary(size / 1_073_741_824, decimals: 2)} GB)"
  end

  defp image_label(image), do: image.name

  defp networks do
    case Networking.overview() do
      %{available?: true, networks: [_ | _] = nets} ->
        {Enum.map(nets, &%{id: &1.id, label: network_label(&1)}), []}

      %{available?: true} ->
        {[fallback_network()], []}

      %{available?: false} ->
        {[fallback_network()], ["The network list could not be read, so only the system network is offered."]}
    end
  end

  defp fallback_network, do: %{id: Vm.default_network_id(), label: "Physical-Direct (System)"}

  defp spice_everywhere? do
    case Config.node_ips() do
      [] ->
        false

      ips ->
        ips
        |> Task.async_stream(&Spark.host_capabilities/1, timeout: 8_000, on_timeout: :kill_task)
        |> Enum.all?(fn
          {:ok, {:ok, %{"graphics" => list}}} when is_list(list) -> "spice" in list
          _ -> false
        end)
    end
  end
end
