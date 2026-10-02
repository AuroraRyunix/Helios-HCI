defmodule SpectrumPhx.ConsoleTasksTest do
  @moduledoc """
  The five controls that queue cluster-wide work: what they refuse, and what they submit.

  One file rather than three, because they are one subject -- and `async: false`, because
  the Catalyst submitter is an application-env seam and a second test swapping it out
  underneath these would make them flap.

  The refusals are the interesting half. Each is a condition the operation would otherwise
  discover minutes in, on a hypervisor, with the console showing a spinning ring: a second
  Kubernetes cluster overwriting the row describing the first, an overlay torn down under a
  cluster that is using it, a rolling upgrade started with no package loaded. Refusing at
  the console costs an operator a sentence; discovering it in the task costs a cluster.

  The other half is that a submission's *failure* is returned. The Python endpoint these
  replace wrote the settings row, tried to submit the bootstrap, printed the failure to a
  log nobody tails, and answered 200 -- leaving a cluster that believes it has an overlay
  and has not got one.
  """
  use ExUnit.Case, async: false

  alias SpectrumPhx.Lanayru
  alias SpectrumPhx.Lcm
  alias SpectrumPhx.Settings

  @job_id "9f1c2f38-3b7a-4a1e-bd0f-2a5c9d3e4f10"

  setup do
    on_exit(fn -> Application.delete_env(:spectrum_phx, :catalyst_submitter) end)
    :ok
  end

  defp accepting do
    test = self()

    Application.put_env(:spectrum_phx, :catalyst_submitter, fn service, action, payload ->
      send(test, {:submitted, service, action, payload})
      {:ok, %{"task_id" => "task-1", "status" => "pending"}}
    end)
  end

  defp refusing(reason \\ :econnrefused) do
    test = self()

    Application.put_env(:spectrum_phx, :catalyst_submitter, fn service, action, payload ->
      send(test, {:submitted, service, action, payload})
      {:error, reason}
    end)
  end

  # -- LCM: starting an upgrade -----------------------------------------------------

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

  defp start_upgrade(jobs), do: Lcm.start_upgrade(source: {:static, %{jobs: jobs}})

  describe "starting a rolling upgrade" do
    test "submits a task that runs hylia on the leader" do
      accepting()

      assert {:ok, "task-1"} = start_upgrade([job_row()])
      assert_receive {:submitted, "dagur", "execute", payload}
      assert payload["command"] == "python3 /usr/local/bin/hylia --start-upgrade #{@job_id}"
      assert payload["job_name"] == Lcm.job_names().upgrade
    end

    test "the task reports its own progress, so dagur's ticker stands down" do
      # The upgrade's real progress is nodes finished, and hylia counts them. A ticker
      # inventing 95% beside that would overwrite a true number with a false one.
      accepting()
      start_upgrade([job_row()])
      assert_receive {:submitted, "dagur", "execute", %{"reports_progress" => true}}
    end

    test "the command gets hours, not the daemon's forty-five seconds" do
      accepting()
      start_upgrade([job_row()])
      assert_receive {:submitted, "dagur", "execute", payload}
      assert payload["timeout"] >= 3600
    end

    test "is refused when no package is loaded" do
      # An upgrade with nothing to install is an operator who believes a package was
      # uploaded and was not, and hylia would resume whatever was last in the table.
      accepting()

      assert {:error, message} = start_upgrade([])
      assert message =~ "No upgrade package is loaded"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "is refused while one is already running" do
      accepting()

      assert {:error, message} = start_upgrade([job_row(%{"state" => "UPGRADING"})])
      assert message =~ "already running"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "a job id that is not a uuid never reaches a command line" do
      # It is interpolated into a root shell command on the leader. Refusing it here is
      # the difference between a rejected value and an injected one, and hylia refuses it
      # a second time for the same reason.
      accepting()

      assert {:error, message} = start_upgrade([job_row(%{"job_id" => "; rm -rf /"})])
      assert message =~ "no usable id"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "a submission that fails is reported rather than logged" do
      refusing()

      assert {:error, message} = start_upgrade([job_row()])
      assert message =~ "could not be started"
    end
  end

  # -- LCM: loading a package -------------------------------------------------------

  describe "loading an upgrade package" do
    test "submits the validation and the fan-out as a task" do
      accepting()

      assert {:ok, "task-1"} = Lcm.load_package("helios-1.2.3.zip", 8192)
      assert_receive {:submitted, "dagur", "execute", payload}

      assert payload["command"] ==
               "python3 /usr/local/bin/hylia --load-package " <>
                 Lcm.package_path()

      assert payload["filename"] == "helios-1.2.3.zip"
      assert payload["size_bytes"] == 8192
    end

    test "the browser's filename cannot carry a path into the task log" do
      # The row is read back and rendered. The name arrived from a browser, so it is a
      # basename of ordinary characters or it is nothing.
      accepting()

      Lcm.load_package("../../etc/passwd", 1)
      assert_receive {:submitted, "dagur", "execute", %{"filename" => filename}}
      refute filename =~ "/"
      refute filename =~ ".."
    end

    test "a submission that fails is reported" do
      refusing()

      assert {:error, message} = Lcm.load_package("helios.zip", 1)
      assert message =~ "could not be handed to the cluster"
    end
  end

  # -- Lanayru: deploying and destroying --------------------------------------------

  defp lanayru_static(overrides) do
    Map.merge(
      %{
        clusters: [],
        segments: [%{"segment_id" => "s1", "name" => "app-net"}],
        expected_nodes: 3,
        urbosa_enabled: "true"
      },
      overrides
    )
  end

  defp deploy(params, overrides \\ %{}) do
    Lanayru.deploy(params, source: {:static, lanayru_static(overrides)})
  end

  defp valid_deploy_params do
    %{"cluster_name" => "kube-01", "control_nodes" => "3", "overlay_segment_id" => "s1"}
  end

  describe "deploying Kubernetes" do
    test "submits to the queue the console backend drains" do
      accepting()

      assert {:ok, "task-1"} = deploy(valid_deploy_params())

      assert_receive {:submitted, "lanayru", "deploy", payload}
      assert payload["cluster_name"] == "kube-01"
      assert payload["control_nodes"] == 3
      assert payload["overlay_segment_id"] == "s1"
    end

    test "is refused while a cluster is already on record" do
      # `hydra.lanayru_clusters` holds one, and a second deploy would overwrite the row
      # describing the cluster that exists -- leaving guests running that nothing names.
      accepting()

      existing = [%{"cluster_id" => "c1", "name" => "kube-01", "status" => "Running"}]
      assert {:error, message} = deploy(valid_deploy_params(), %{clusters: existing})
      assert message =~ "already on record"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "is refused when the overlay is off" do
      # Kubernetes on no overlay is the failure that looks like success until a pod tries
      # to talk to another one, and by then the deploy has run for minutes.
      accepting()

      assert {:error, message} = deploy(valid_deploy_params(), %{urbosa_enabled: "false"})
      assert message =~ "overlay networking is disabled"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "refuses a name that is not an object name" do
      accepting()

      assert {:error, message} =
               deploy(%{valid_deploy_params() | "cluster_name" => "kube 01; reboot"})

      assert message =~ "cluster name"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "refuses a control plane larger than the cluster" do
      accepting()

      assert {:error, message} = deploy(%{valid_deploy_params() | "control_nodes" => "9"})
      assert message =~ "does not fit"
    end

    test "refuses a segment that does not exist" do
      accepting()

      assert {:error, message} =
               deploy(%{valid_deploy_params() | "overlay_segment_id" => "s404"})

      assert message =~ "No overlay segment"
    end

    test "no segment at all is legal, and means the worker's default routing" do
      accepting()

      assert {:ok, _} = deploy(%{valid_deploy_params() | "overlay_segment_id" => ""})
      assert_receive {:submitted, "lanayru", "deploy", %{"overlay_segment_id" => ""}}
    end
  end

  describe "destroying Kubernetes" do
    defp destroy(confirmation, overrides \\ %{}) do
      Lanayru.destroy(confirmation, source: {:static, lanayru_static(overrides)})
    end

    @existing [%{"cluster_id" => "c1", "name" => "kube-01", "status" => "Running"}]

    test "needs the cluster's name typed back" do
      # A second click is a reflex. On the other side of this button are every guest node
      # of a Kubernetes cluster and the rows describing them.
      accepting()

      assert {:error, message} = destroy("yes", %{clusters: @existing})
      assert message =~ "not the cluster's name"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "submits when the name matches" do
      accepting()

      assert {:ok, "task-1"} = destroy("kube-01", %{clusters: @existing})
      assert_receive {:submitted, "lanayru", "destroy", %{"cluster_name" => "kube-01"}}
    end

    test "is refused when there is nothing to destroy" do
      accepting()

      assert {:error, message} = destroy("kube-01")
      assert message =~ "no Kubernetes cluster on record"
      refute_receive {:submitted, _service, _action, _payload}
    end
  end

  # -- Settings: the overlay switch --------------------------------------------------

  defp urbosa_static(overrides) do
    Map.merge(%{settings: [], lanayru: [], cluster: %{nodes: 3}}, overrides)
  end

  defp set_urbosa(value, overrides \\ %{}) do
    Settings.set_urbosa_enabled(value, source: {:static, urbosa_static(overrides)})
  end

  defp urbosa_row(value), do: %{"key" => "urbosa_enabled", "value" => value}

  describe "the overlay switch" do
    test "turning it on submits the bootstrap" do
      accepting()

      assert {:ok, %{direction: :bootstrap, task_id: "task-1"}} = set_urbosa("true")
      assert_receive {:submitted, "dagur", "execute", payload}
      assert payload["job_name"] == Settings.urbosa_job_names().bootstrap
      assert payload["command"] == "python3 /usr/local/bin/urbosa-bootstrap"
    end

    test "turning it off submits the teardown, which is a different command" do
      # The two directions are not the same act and must not be reported as one. Removing
      # namespaces, bridges and VXLAN interfaces from every host is what `--cleanup` does.
      accepting()

      assert {:ok, %{direction: :teardown}} =
               set_urbosa("false", %{settings: [urbosa_row("true")]})

      assert_receive {:submitted, "dagur", "execute", payload}
      assert payload["job_name"] == Settings.urbosa_job_names().cleanup
      assert payload["command"] =~ "--cleanup"
    end

    test "asking for the state it is already in submits nothing" do
      accepting()

      assert {:ok, :unchanged} = set_urbosa("false")
      assert {:ok, :unchanged} = set_urbosa("true", %{settings: [urbosa_row("true")]})
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "tearing it down under a live Kubernetes cluster is refused, by name" do
      # The cluster routes over the overlay. Removing it takes that cluster's network away
      # while it is running, and "something is using it" is not a sentence an operator can
      # act on -- so the refusal names the cluster.
      accepting()

      static = %{
        settings: [urbosa_row("true")],
        lanayru: [%{"name" => "kube-01", "status" => "Active"}]
      }

      assert {:error, message} = set_urbosa("false", static)
      assert message =~ "kube-01"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "a cluster that is not running does not block the teardown" do
      accepting()

      static = %{
        settings: [urbosa_row("true")],
        lanayru: [%{"name" => "kube-01", "status" => "Failed"}]
      }

      assert {:ok, %{direction: :teardown}} = set_urbosa("false", static)
    end

    test "a submission that fails leaves the setting as it was and says so" do
      # This is the whole reason it is not a checkbox. A row saying the overlay is on with
      # no bootstrap behind it is a cluster that believes it has an overlay and has not
      # got one, and Lanayru's pre-flight, the SDN page and every deploy read that row.
      refusing()

      assert {:error, message} = set_urbosa("true")
      assert message =~ "left as it was"
      assert message =~ "could not be queued"
    end
  end
end
