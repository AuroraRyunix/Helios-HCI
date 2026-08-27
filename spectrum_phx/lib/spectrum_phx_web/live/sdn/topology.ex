defmodule SpectrumPhxWeb.Sdn.Topology do
  @moduledoc """
  The overlay drawn as the tree it is: T0 above, its T1 routers beneath, segments beneath
  those, and each segment's guests as a row of dots under it.

  Server-rendered SVG with the layout computed here, for the same reason the telemetry
  charts are: the page this replaces drew a *fixed* diagram -- the boxes were in the
  markup and the data was written into them -- so it could only ever show the shape
  somebody anticipated. A cluster with two T0s, or a segment with no route off itself,
  had nowhere to appear. Here the geometry is a function of the fabric, so the picture is
  of the fabric rather than of a drawing of one.

  ## Layout

  Four bands at fixed heights, each laid out left to right in its own row. Widths come
  from the widest band, so the diagram is exactly as wide as it needs to be and scales to
  the container rather than to a guessed viewport.

  Edges are drawn as vertical-then-horizontal elbows rather than diagonals: a diagonal
  through four bands of boxes crosses whatever happens to be between them, and the eye
  cannot follow which line entered which box.

  ## Orphans are drawn, detached

  A segment whose T1 no longer exists gets a band of its own on the right, with no edge
  running to it. That is the point: an operator looking for why a guest cannot reach
  anything should see a box with nothing above it, which is the actual fault.
  """
  use SpectrumPhxWeb, :html

  # Geometry. Everything else is derived.
  @box_w 168
  @box_h 52
  @gap_x 24
  @gap_y 74
  @pad 16

  @doc """
  Render the fabric.

  `fabric` is `SpectrumPhx.Sdn.fabric/1`. `selected` is the id of the element to
  highlight, or nil.
  """
  attr :fabric, :map, required: true
  attr :selected, :string, default: nil
  attr :id, :string, default: "sdn-topology"

  def diagram(assigns) do
    layout = layout(assigns.fabric)

    assigns =
      assigns
      |> assign(:nodes, layout.nodes)
      |> assign(:edges, layout.edges)
      |> assign(:width, layout.width)
      |> assign(:height, layout.height)

    ~H"""
    <div class="overflow-x-auto" id={@id}>
      <svg
        viewBox={"0 0 #{@width} #{@height}"}
        class="w-full min-w-[42rem]"
        style={"max-height: #{@height}px"}
        role="img"
        aria-label="Overlay network topology"
      >
        <line
          :for={edge <- @edges}
          :if={edge.kind == :vertical}
          x1={edge.x1}
          y1={edge.y1}
          x2={edge.x2}
          y2={edge.y2}
          class="stroke-base-content/25"
          stroke-width="1.5"
        />
        <path
          :for={edge <- @edges}
          :if={edge.kind == :elbow}
          d={edge.d}
          fill="none"
          class="stroke-base-content/25"
          stroke-width="1.5"
        />

        <g :for={node <- @nodes}>
          <rect
            x={node.x}
            y={node.y}
            width={node.w}
            height={node.h}
            rx="8"
            class={[
              "transition-colors",
              band_fill(node.band),
              @selected == node.id && "stroke-primary",
              @selected != node.id && band_stroke(node.band)
            ]}
            stroke-width={if @selected == node.id, do: "2", else: "1"}
          />
          <text
            x={node.x + 12}
            y={node.y + 20}
            class="fill-base-content text-[11px] font-semibold"
          >
            {truncate(node.label, 22)}
          </text>
          <text :if={node.detail} x={node.x + 12} y={node.y + 35} class="fill-base-content/60 text-[10px]">
            {truncate(node.detail, 26)}
          </text>
          <text :if={node.badge} x={node.x + node.w - 12} y={node.y + 20} text-anchor="end" class={["text-[10px] font-mono", badge_fill(node.band)]}>
            {node.badge}
          </text>
          <g :if={node.guests > 0}>
            <circle
              :for={{_guest, index} <- Enum.with_index(Enum.take(node.guest_list, 8))}
              cx={node.x + 14 + index * 13}
              cy={node.y + node.h - 11}
              r="4"
              class={guest_fill(Enum.at(node.guest_list, index))}
            />
            <text
              :if={node.guests > 8}
              x={node.x + 14 + 8 * 13}
              y={node.y + node.h - 7}
              class="fill-base-content/50 text-[9px]"
            >
              +{node.guests - 8}
            </text>
          </g>
        </g>
      </svg>
    </div>
    """
  end

  defp band_fill(:t0), do: "fill-primary/15 stroke-primary/40"
  defp band_fill(:t1), do: "fill-secondary/15 stroke-secondary/40"
  defp band_fill(:segment), do: "fill-accent/10 stroke-accent/40"
  defp band_fill(:orphan), do: "fill-error/10 stroke-error/50"

  defp band_stroke(:t0), do: "stroke-primary/40"
  defp band_stroke(:t1), do: "stroke-secondary/40"
  defp band_stroke(:segment), do: "stroke-accent/40"
  defp band_stroke(:orphan), do: "stroke-error/50"

  defp badge_fill(:t0), do: "fill-primary/70"
  defp badge_fill(:t1), do: "fill-secondary/70"
  defp badge_fill(:segment), do: "fill-accent/70"
  defp badge_fill(:orphan), do: "fill-error/70"

  # A guest that is not running is drawn hollow rather than omitted: a segment with four
  # stopped guests and one with none are different situations.
  defp guest_fill(%{state: state}) when is_binary(state) do
    if String.downcase(state) == "running",
      do: "fill-success",
      else: "fill-base-content/25"
  end

  defp guest_fill(_), do: "fill-base-content/25"

  defp truncate(nil, _limit), do: ""

  defp truncate(text, limit) do
    if String.length(text) > limit, do: String.slice(text, 0, limit - 1) <> "…", else: text
  end

  # -- layout --------------------------------------------------------------------------

  @doc """
  The geometry, exposed so it can be tested without rendering.

  Returns `%{nodes: [...], edges: [...], width: w, height: h}`.
  """
  def layout(fabric) do
    t0_boxes = band(fabric.tier0, :t0, 0, &t0_box/1)

    t1_entries = Enum.flat_map(fabric.tier0, fn t0 -> Enum.map(t0.tier1, &{&1, t0.id}) end)
    t1_boxes = band(t1_entries, :t1, 1, fn {router, parent} -> t1_box(router, parent) end)

    segment_entries =
      Enum.flat_map(t1_entries, fn {router, _t0} ->
        Enum.map(router.segments, &{&1, router.id})
      end)

    segment_boxes = band(segment_entries, :segment, 2, fn {segment, parent} -> segment_box(segment, parent) end)

    orphans =
      Enum.map(fabric.orphans.tier1, &{&1, :tier1}) ++
        Enum.map(fabric.orphans.segments, &{&1, :segment})

    orphan_boxes = band(orphans, :orphan, 3, fn {entry, kind} -> orphan_box(entry, kind) end)

    nodes = t0_boxes ++ t1_boxes ++ segment_boxes ++ orphan_boxes
    by_id = Map.new(nodes, &{&1.id, &1})

    edges =
      for node <- nodes, node.parent, parent = Map.get(by_id, node.parent), parent != nil do
        elbow(parent, node)
      end

    rows = if orphan_boxes == [], do: 3, else: 4

    %{
      nodes: nodes,
      edges: edges,
      width: max(width_of(nodes), 480),
      height: @pad * 2 + rows * @box_h + (rows - 1) * (@gap_y - @box_h)
    }
  end

  defp band([], _kind, _row, _fun), do: []

  defp band(entries, kind, row, fun) do
    entries
    |> Enum.with_index()
    |> Enum.map(fn {entry, index} ->
      fun.(entry)
      |> Map.merge(%{
        band: kind,
        x: @pad + index * (@box_w + @gap_x),
        y: @pad + row * @gap_y,
        w: @box_w,
        h: @box_h
      })
    end)
  end

  defp t0_box(t0) do
    %{
      id: t0.id,
      parent: nil,
      label: t0.name,
      detail: t0.uplink_ip || t0.uplink_interface || "no uplink",
      badge: "T0",
      guests: 0,
      guest_list: []
    }
  end

  defp t1_box(router, parent) do
    %{
      id: router.id,
      parent: parent,
      label: router.name,
      detail: "#{length(router.segments)} segment#{if length(router.segments) == 1, do: "", else: "s"}",
      badge: "T1",
      guests: 0,
      guest_list: []
    }
  end

  defp segment_box(segment, parent) do
    %{
      id: segment.id,
      parent: parent,
      label: segment.name,
      detail: segment.subnet_cidr || "no subnet",
      badge: segment.vni && "VNI #{segment.vni}",
      guests: length(segment.guests),
      guest_list: segment.guests
    }
  end

  defp orphan_box(entry, :tier1) do
    %{
      id: entry.id,
      parent: nil,
      label: entry.name,
      detail: "T1 with no T0",
      badge: "!",
      guests: 0,
      guest_list: []
    }
  end

  defp orphan_box(entry, :segment) do
    %{
      id: entry.id,
      parent: nil,
      label: entry.name,
      detail: "segment with no router",
      badge: "!",
      guests: length(entry.guests),
      guest_list: entry.guests
    }
  end

  defp width_of([]), do: 0

  defp width_of(nodes) do
    nodes |> Enum.map(fn node -> node.x + node.w end) |> Enum.max() |> Kernel.+(@pad)
  end

  # Down out of the parent, across, then down into the child. Never a diagonal: through
  # four bands of boxes a diagonal crosses whatever is between them and the eye cannot
  # tell which line entered which box.
  defp elbow(parent, child) do
    from_x = parent.x + div(parent.w, 2)
    from_y = parent.y + parent.h
    to_x = child.x + div(child.w, 2)
    to_y = child.y
    mid_y = from_y + div(to_y - from_y, 2)

    %{
      kind: :elbow,
      d: "M #{from_x} #{from_y} V #{mid_y} H #{to_x} V #{to_y}"
    }
  end
end
