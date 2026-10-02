defmodule SpectrumPhxWeb.Lcm.PackageTransportStub do
  @moduledoc """
  Stands in for the Mint request the package writer holds open to the leader.

  Accumulates the chunks so a test can assert the bytes arrived intact and in order, which
  is the property a streaming writer is most likely to get wrong, and reports every call to
  the test process so the *sequence* -- open, chunk, finish, or open, chunk, cleanup -- is
  what gets asserted rather than a return value.
  """

  def open(node, size_bytes) do
    report({:open, node, size_bytes})

    case answer(:open, :ok) do
      :ok -> {:ok, %{written: "", size_bytes: size_bytes}}
      {:error, reason} -> {:error, reason}
    end
  end

  def send_chunk(handle, data) do
    case answer(:chunk, :ok) do
      :ok -> {:ok, %{handle | written: handle.written <> data}}
      {:error, reason} -> {:error, reason}
    end
  end

  def finish(handle) do
    report({:finish, handle.written})
    answer(:finish, {:ok, byte_size(handle.written)})
  end

  def close(_handle), do: :ok

  def cleanup(node) do
    report({:cleanup, node})
    :ok
  end

  def install(answers \\ %{}) do
    Application.put_env(:spectrum_phx, :package_upload_transport, __MODULE__)
    Application.put_env(:spectrum_phx, :package_upload_stub, {self(), answers})
  end

  defp report(message) do
    case Application.get_env(:spectrum_phx, :package_upload_stub) do
      {pid, _answers} -> send(pid, {:package_stub, message})
      _ -> :ok
    end
  end

  defp answer(key, default) do
    case Application.get_env(:spectrum_phx, :package_upload_stub) do
      {_pid, answers} -> Map.get(answers, key, default)
      _ -> default
    end
  end
end

defmodule SpectrumPhxWeb.Lcm.IndexLiveTest do
  @moduledoc """
  The two LCM controls: uploading a package and starting the upgrade.

  The upload's property is that nothing is staged in this tier -- the bytes go from the
  browser onto an open request to the leader's own daemon -- and that an upload which does
  not complete leaves nothing behind at the path the loader is about to read. A truncated
  archive there is worse than none: it fails validation as "not a zip file", which sends an
  operator looking at the package rather than at the transfer.

  The start's property is that it confirms first and refuses when there is nothing to
  install. It is one click from putting every node in the cluster through maintenance and a
  reboot.
  """
  # Not async: the LCM source, the Catalyst submitter and the upload transport are all
  # application env.
  use SpectrumPhxWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias SpectrumPhxWeb.Lcm.PackageTransportStub

  @job_id "9f1c2f38-3b7a-4a1e-bd0f-2a5c9d3e4f10"

  defp mount_view(conn), do: live(log_in(conn), "/lcm")

  defp put_source(overrides \\ %{}) do
    static = Map.merge(%{inventory: [], update: [], jobs: [], logs: []}, overrides)
    Application.put_env(:spectrum_phx, :lcm_source, {:static, static})
  end

  defp job_row(overrides \\ %{}) do
    Map.merge(
      %{
        "job_id" => @job_id,
        "state" => "IDLE",
        "target_nodes" => ["10.0.0.1", "10.0.0.2", "10.0.0.3"],
        "current_node" => "",
        "build_number" => "1.2.3-b4100"
      },
      overrides
    )
  end

  defp accepting do
    test = self()

    Application.put_env(:spectrum_phx, :catalyst_submitter, fn service, action, payload ->
      send(test, {:submitted, service, action, payload})
      {:ok, %{"task_id" => "task-13"}}
    end)
  end

  defp entry(name, size) do
    %{name: name, content: :binary.copy("0", size), size: size, type: "application/zip"}
  end

  setup do
    put_source()
    accepting()
    PackageTransportStub.install()

    on_exit(fn ->
      Application.delete_env(:spectrum_phx, :lcm_source)
      Application.delete_env(:spectrum_phx, :catalyst_submitter)
      Application.delete_env(:spectrum_phx, :package_upload_transport)
      Application.delete_env(:spectrum_phx, :package_upload_stub)
    end)

    :ok
  end

  describe "starting an upgrade" do
    test "asks before it puts every node through a reboot", %{conn: conn} do
      put_source(%{jobs: [job_row()]})
      {:ok, view, _html} = mount_view(conn)

      html = view |> element("#upgrade-start") |> render_click()

      assert html =~ "maintenance and a"
      assert html =~ "3 node(s)"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "the confirmation submits the task", %{conn: conn} do
      put_source(%{jobs: [job_row()]})
      {:ok, view, _html} = mount_view(conn)

      view |> element("#upgrade-start") |> render_click()
      view |> element("#upgrade-confirm-start") |> render_click()

      assert_receive {:submitted, "dagur", "execute", payload}
      assert payload["command"] =~ "--start-upgrade #{@job_id}"
      assert render(view) =~ "task-13"
    end

    test "with no package loaded it says so on the page", %{conn: conn} do
      # An upgrade with nothing to install is an operator who believes a package was
      # uploaded and was not.
      {:ok, view, _html} = mount_view(conn)

      view |> element("#upgrade-start") |> render_click()
      view |> element("#upgrade-confirm-start") |> render_click()

      assert view |> element("#upgrade-error") |> render() =~ "No upgrade package is loaded"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "cancelling submits nothing", %{conn: conn} do
      put_source(%{jobs: [job_row()]})
      {:ok, view, _html} = mount_view(conn)

      view |> element("#upgrade-start") |> render_click()
      view |> element("#upgrade-confirm button", "Cancel") |> render_click()

      refute has_element?(view, "#upgrade-confirm")
      refute_receive {:submitted, _service, _action, _payload}
    end
  end

  describe "uploading a package" do
    test "the page says where the bytes go", %{conn: conn} do
      # An operator watching a several-hundred-megabyte transfer is owed the fact that it
      # is not passing through this container's disk.
      {:ok, _view, html} = mount_view(conn)

      assert html =~ "streams from your browser"
      assert html =~ "/tmp/helios_update.zip"
    end

    test "an archive on the leader is not yet an archive the cluster has been told to install",
         %{conn: conn} do
      # Staging and installing are two acts. Transferring the bytes must not queue the task
      # that validates them, fans them out and truncates the job table -- that happens when
      # an operator says so, on submit.
      {:ok, view, _html} = mount_view(conn)

      upload = file_input(view, "#package-form", :package, [entry("helios.zip", 512)])
      assert render_upload(upload, "helios.zip", 100) =~ "helios.zip"

      assert_receive {:package_stub, {:finish, _bytes}}
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "a file that is not an archive is refused before anything opens", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      upload =
        file_input(view, "#package-form", :package, [
          %{name: "notes.txt", content: "x", size: 1, type: "text/plain"}
        ])

      render_upload(upload, "notes.txt", 0)

      assert render(view) =~ "a .zip archive"
      refute_receive {:package_stub, {:open, _node, _size}}
    end

    test "submitting with no file says so rather than failing silently", %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      html = view |> element("#package-form") |> render_submit()
      assert html =~ "Choose an update package first."
    end

    test "a submitted package is streamed to the leader and then queued as a task",
         %{conn: conn} do
      {:ok, view, _html} = mount_view(conn)

      upload = file_input(view, "#package-form", :package, [entry("helios-1.2.3.zip", 2048)])
      render_upload(upload, "helios-1.2.3.zip", 100)

      # The bytes reached the transport intact, and no disk of this tier's.
      assert_receive {:package_stub, {:finish, bytes}}
      assert byte_size(bytes) == 2048

      view |> element("#package-form") |> render_submit()

      assert_receive {:submitted, "dagur", "execute", payload}
      assert payload["command"] =~ "--load-package"
      assert payload["filename"] == "helios-1.2.3.zip"
      assert render(view) =~ "task-13"
    end

    test "an upload that cannot open leaves nothing staged and never queues a task",
         %{conn: conn} do
      PackageTransportStub.install(%{open: {:error, "connection refused"}})
      {:ok, view, _html} = mount_view(conn)

      upload = file_input(view, "#package-form", :package, [entry("helios.zip", 64)])

      # How LiveViewTest surfaces a writer failure is not the point; that the staged file
      # is removed and no task is submitted for a package that is not there, is.
      try do
        render_upload(upload, "helios.zip", 100)
      catch
        :exit, _reason -> :ok
      end

      assert_receive {:package_stub, {:cleanup, _node}}
      refute_receive {:submitted, _service, _action, _payload}
    end
  end
end
