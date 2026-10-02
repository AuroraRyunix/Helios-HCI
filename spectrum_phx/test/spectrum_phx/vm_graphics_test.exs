defmodule SpectrumPhx.VmGraphicsTest do
  @moduledoc """
  A VM's console protocol, as the console sees it.

  The graphics device lives in the domain XML, so it is per-VM by construction; the column
  in `hydra.vms` is the desired state and the running domain is the runtime truth. What is
  asserted here is the narrowing, because the rule exists in four languages and the failure
  when they drift is quiet: a console the UI calls SPICE and `vali` builds as VNC opens on
  the proxy's protocol-mismatch refusal, which reads as a broken console rather than as a
  disagreement about what the VM is.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhx.Vms.Vm

  defp graphics_of(row), do: Vm.from_row(row).graphics

  describe "narrowing the column" do
    test "a row written before the column exists is VNC, not nil" do
      # The whole reason null has to mean something: every VM in the cluster predates this.
      assert graphics_of(%{"name" => "old"}) == "vnc"
      assert graphics_of(%{"name" => "old", "graphics" => nil}) == "vnc"
    end

    test "spice is recognised however it was typed" do
      for value <- ["spice", "SPICE", "Spice", "  spice  ", "\tspice\n"] do
        assert graphics_of(%{"graphics" => value}) == "spice",
               "#{inspect(value)} was not recognised as SPICE"
      end
    end

    test "only the exact word counts" do
      # A near miss has to fall back rather than be guessed at: an unknown graphics type is
      # a domain libvirt refuses, and it refuses it at start time, long after the create
      # that caused it answered.
      for value <- ["vnc", "", "spicy", "spice2", "qxl", "rdp", "s p i c e", "nospice"] do
        assert graphics_of(%{"graphics" => value}) == "vnc",
               "#{inspect(value)} was treated as something other than VNC"
      end
    end

    test "a value that is not even a string is VNC" do
      for value <- [17, true, %{}, [], {:spice}] do
        assert graphics_of(%{"graphics" => value}) == "vnc"
      end
    end

    test "an already-decoded struct keeps its console" do
      vm = Vm.from_row(%{"name" => "v", "graphics" => "spice"})
      assert Vm.from_row(vm).graphics == "spice"
    end
  end

  describe "the column is actually read" do
    test "the VM query selects graphics" do
      # A reader that forgets the column sees nil, which narrows to "vnc" -- so it looks
      # like a working default rather than a missing field, and a SPICE VM silently offers
      # the wrong console.
      assert File.read!("lib/spectrum_phx/vms.ex") =~ ~r/@columns "[^"]*\bgraphics\b/
    end

    test "the struct carries it so a template can branch on it" do
      # :name is enforced on the struct, so the default is read off a decoded row.
      vm = Vm.from_row(%{"name" => "v"})
      assert Map.has_key?(vm, :graphics)
      assert vm.graphics == "vnc"
    end
  end

  describe "the console link" do
    test "the VM page routes to the page the domain can serve" do
      # This button did not exist. Every page is Phoenix now, and the only console buttons
      # in the tree were the two in the legacy app.js -- both of which opened the VNC page,
      # including the one labelled for the SPICE client. An operator on the current console
      # had no way to open a guest console at all.
      source = File.read!("lib/spectrum_phx_web/live/vms/show_live.ex")

      assert source =~ ~S|defp console_page(%{graphics: "spice"}), do: "spice_auto.html"|
      assert source =~ ~S|defp console_page(_vm), do: "vnc_auto.html"|
      assert source =~ ~S|id="console"|
    end

    test "it opens in a new tab without handing the console window opener rights" do
      source = File.read!("lib/spectrum_phx_web/live/vms/show_live.ex")
      assert source =~ ~S|target="_blank"|
      assert source =~ ~S|rel="noopener"|
    end

    test "the VM name is encoded into the link" do
      # VM names are validated elsewhere, but a console URL is not the place to rely on it.
      source = File.read!("lib/spectrum_phx_web/live/vms/show_live.ex")
      assert source =~ "URI.encode_www_form(@vm.name)"
    end
  end
end
