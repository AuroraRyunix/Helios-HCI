defmodule SpectrumPhx.CatalystTest do
  @moduledoc """
  Handing a long operation to the cluster.

  Every one of these is about a way a task can be accepted and never happen, because that
  is the failure the task queue exists to remove and the one this module can reintroduce:
  a queue nothing drains, a command with no time to run in, a submission whose failure
  went to a log.
  """
  use ExUnit.Case, async: false

  alias SpectrumPhx.Catalyst

  setup do
    on_exit(fn -> Application.delete_env(:spectrum_phx, :catalyst_submitter) end)
    :ok
  end

  defp record_submissions do
    test = self()

    Application.put_env(:spectrum_phx, :catalyst_submitter, fn service, action, payload ->
      send(test, {:submitted, service, action, payload})
      {:ok, %{"task_id" => "task-1", "status" => "pending"}}
    end)
  end

  describe "submit/3" do
    test "a service with a worker is submitted" do
      record_submissions()

      assert {:ok, %{"task_id" => "task-1"}} = Catalyst.submit("dagur", "execute", %{})
      assert_receive {:submitted, "dagur", "execute", %{}}
    end

    test "a service nothing drains is refused rather than queued" do
      # `spark` is a queue in catalyst.py that no daemon long-polls. Submitting to it
      # writes a row, returns a task id, and then nothing happens -- and `pending` on the
      # task ring is indistinguishable from a task that is merely slow.
      record_submissions()

      assert {:error, {:unknown_service, message}} = Catalyst.submit("spark", "execute", %{})
      assert message =~ "would never run"
      refute_receive {:submitted, _service, _action, _payload}
    end

    test "the services it will submit to are the ones with workers" do
      assert Enum.sort(Catalyst.services()) == ["dagur", "lanayru", "vali"]
    end

    test "a transport failure is returned, not swallowed" do
      Application.put_env(:spectrum_phx, :catalyst_submitter, fn _s, _a, _p ->
        {:error, :econnrefused}
      end)

      assert {:error, :econnrefused} = Catalyst.submit("vali", "start", %{"vm_name" => "web"})
    end
  end

  describe "run_on_leader/3" do
    test "the command carries a timeout, because spark-daemon's default would kill it" do
      # An absent `timeout` is not "no limit": spark-daemon applies 45 seconds and kills
      # the command there. Every operation submitted this way is cluster-wide and runs
      # longer than that, so the number has to travel with the task.
      record_submissions()

      Catalyst.run_on_leader("urbosa_bootstrap", "python3 /usr/local/bin/urbosa-bootstrap")

      assert_receive {:submitted, "dagur", "execute", payload}
      assert payload["timeout"] == Catalyst.default_command_timeout()
      assert payload["timeout"] > 45
    end

    test "a caller may state its own ceiling" do
      record_submissions()
      Catalyst.run_on_leader("job", "true", timeout: 120)
      assert_receive {:submitted, "dagur", "execute", %{"timeout" => 120}}
    end

    test "a command that reports its own progress says so, so the ticker stands down" do
      # dagur's ticker climbs to 95 in ten seconds and stays there. Beside a command that
      # writes real progress it overwrites a true 20% with an invented 95%, and the bar
      # an operator is watching becomes a lie that moves.
      record_submissions()
      Catalyst.run_on_leader("job", "true", reports_progress: true)
      assert_receive {:submitted, "dagur", "execute", %{"reports_progress" => true}}
    end

    test "a command that does not is left alone" do
      record_submissions()
      Catalyst.run_on_leader("job", "true")
      assert_receive {:submitted, "dagur", "execute", payload}
      refute Map.has_key?(payload, "reports_progress")
    end

    test "extra payload travels into the row, which is where a task's subject lives" do
      record_submissions()
      Catalyst.run_on_leader("job", "true", payload: %{"filename" => "update.zip"})
      assert_receive {:submitted, "dagur", "execute", %{"filename" => "update.zip"}}
    end
  end

  describe "describe/1" do
    test "an error becomes something an operator can read" do
      assert Catalyst.describe({:unknown_service, "no worker"}) == "no worker"
      assert Catalyst.describe({:http, 500, %{"error" => "boom"}}) == "boom"
      assert Catalyst.describe({:http, 503, "unavailable"}) =~ "503"
      assert Catalyst.describe("plain") == "plain"
    end
  end
end
