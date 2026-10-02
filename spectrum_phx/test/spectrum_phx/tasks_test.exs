defmodule SpectrumPhx.TasksTest do
  use ExUnit.Case, async: true

  alias SpectrumPhx.Tasks

  defp row(attrs) do
    Map.merge(
      %{
        "task_id" => "00000000-0000-0000-0000-000000000000",
        "service" => "vali",
        "action" => "start",
        "status" => "completed",
        "payload" => nil,
        "progress" => 100,
        "error_msg" => nil,
        "created_at" => ~U[2026-08-19 10:00:00Z],
        "updated_at" => ~U[2026-08-19 10:00:05Z]
      },
      attrs
    )
  end

  describe "normalisation" do
    test "names a task from its payload rather than from its columns" do
      rows = [
        row(%{"payload" => ~s({"vm_name":"web-01"}), "action" => "start"}),
        row(%{
          "task_id" => "a",
          "service" => "dagur",
          "action" => "execute",
          "payload" => ~s({"job_name":"storage_scrub"})
        }),
        row(%{
          "task_id" => "b",
          "service" => "host",
          "action" => "host_maintenance_enter",
          "payload" => ~s({"hostname":"hci-02"})
        }),
        row(%{
          "task_id" => "d",
          "service" => "lanayru",
          "action" => "destroy",
          "payload" => ~s({"cluster_name":"kube-01"})
        }),
        row(%{"task_id" => "c", "service" => "gatoway", "action" => "sync", "payload" => nil})
      ]

      labels = Tasks.fetch(rows: rows).tasks |> Enum.map(& &1.label) |> Enum.sort()

      assert "VM 'web-01' - start" in labels
      assert "Job 'storage_scrub' - execute" in labels
      assert "Host 'hci-02' - enter maintenance" in labels
      assert "gatoway - sync" in labels
      # The ring announces this string when a task starts or fails, and "lanayru -
      # destroy" is the service and the verb with the subject missing -- which for the one
      # task that removes a whole Kubernetes cluster is the word that matters.
      assert "Kubernetes cluster 'kube-01' - destroy" in labels
    end

    test "accepts atom keys and epoch milliseconds as well as Xandra's shapes" do
      row = %{
        task_id: "abc",
        service: "vali",
        action: "stop",
        status: "completed",
        progress: "100",
        created_at: 1_755_600_000_000,
        updated_at: 1_755_600_030_000
      }

      assert %{tasks: [task]} = Tasks.fetch(rows: [row])
      assert task.id == "abc"
      assert task.progress == 100
      assert %DateTime{} = task.created_at
    end

    test "a malformed payload does not take the row with it" do
      assert %{tasks: [task]} = Tasks.fetch(rows: [row(%{"payload" => "{not json"})])
      assert task.payload == nil
      assert task.label == "vali - start"
    end

    test "progress outside 0..100 is clamped rather than rendered as an oversized bar" do
      rows = [
        row(%{
          "task_id" => "a",
          "progress" => 250,
          "created_at" => ~U[2026-08-19 11:00:00Z]
        }),
        row(%{
          "task_id" => "b",
          "progress" => -5,
          "created_at" => ~U[2026-08-19 10:00:00Z]
        })
      ]

      assert [100, 0] == Enum.map(Tasks.fetch(rows: rows).tasks, & &1.progress)
    end
  end

  describe "state classification" do
    test "a failed task is failed even though its progress reads 100" do
      # spectrum_server.py writes progress = 100 at every failure site. A full bar must
      # never be read as success.
      row =
        row(%{
          "status" => "failed",
          "progress" => 100,
          "error_msg" => "the vdisk attach was refused"
        })

      assert %{tasks: [task], summary: summary} = Tasks.fetch(rows: [row])
      assert task.state == :failed
      assert task.progress == 100
      assert task.error == "the vdisk attach was refused"
      assert summary.failed == 1
      assert summary.completed == 0
    end

    test "an unrecognised status is unknown, and is never counted as completed" do
      rows = [
        row(%{"task_id" => "a", "status" => "aborted"}),
        row(%{"task_id" => "b", "status" => nil}),
        row(%{"task_id" => "c", "status" => "   "})
      ]

      assert %{tasks: tasks, summary: summary} = Tasks.fetch(rows: rows)
      assert Enum.all?(tasks, &(&1.state == :unknown))
      assert summary.unknown == 3
      assert summary.completed == 0
      assert summary.failed == 0
    end

    test "counts every lifecycle state it knows about" do
      rows = [
        row(%{"task_id" => "a", "status" => "pending"}),
        row(%{"task_id" => "b", "status" => "processing"}),
        row(%{"task_id" => "c", "status" => "completed"}),
        row(%{"task_id" => "d", "status" => "failed"})
      ]

      summary = Tasks.fetch(rows: rows).summary

      assert summary.total == 4
      assert summary.pending == 1
      assert summary.running == 1
      assert summary.completed == 1
      assert summary.failed == 1
    end
  end

  describe "the task tree's columns" do
    test "a parent named in the column is read, and so is one named only in the payload" do
      # The payload is where the relationship lived before there was a column, because
      # `mipha.py` had nowhere else to put it. Those rows stay in the table for the length of
      # the thirty-day retention window, so a cluster that upgrades today reads them for a
      # month and dropping support for them would flatten a month of history.
      rows = [
        row(%{"task_id" => "parent"}),
        row(%{"task_id" => "by-column", "parent_task_id" => "parent"}),
        row(%{"task_id" => "by-payload", "payload" => ~s({"parent_task_id":"parent"})})
      ]

      tasks = Tasks.fetch(rows: rows).tasks
      by_id = Map.new(tasks, &{&1.id, &1})

      assert by_id["by-column"].parent_id == "parent"
      assert by_id["by-payload"].parent_id == "parent"
      assert by_id["by-column"].depth == 1
      assert by_id["by-payload"].depth == 1
    end

    test "the column wins over the payload rather than the two being adjudicated" do
      rows = [
        row(%{"task_id" => "a"}),
        row(%{"task_id" => "b"}),
        row(%{
          "task_id" => "child",
          "parent_task_id" => "a",
          "payload" => ~s({"parent_task_id":"b"})
        })
      ]

      child = Enum.find(Tasks.fetch(rows: rows).tasks, &(&1.id == "child"))
      assert child.parent_id == "a"
    end

    test "the component is the owner and falls back to the executor, never to nothing" do
      # `service` is the queue that ran the task. They differ exactly where it matters: a
      # Hylia upgrade step runs on the `dagur` queue, so reading `service` as the owner
      # attributes a rolling upgrade to the cron runner.
      owned = row(%{"service" => "dagur", "component" => "Hylia"})
      bare = row(%{"service" => "dagur", "component" => nil})

      assert [task] = Tasks.fetch(rows: [owned]).tasks
      assert task.component == "Hylia"
      assert task.service == "dagur"

      assert [older] = Tasks.fetch(rows: [bare]).tasks
      assert older.component == "dagur"
    end

    test "a missing sequence id stays nil rather than becoming a position" do
      # Null on every row written before the column existed, and on a submission whose claim
      # on the per-component counter was refused. Rendered as 0 it would read as "the first
      # thing this component ever did".
      assert [numbered] = Tasks.fetch(rows: [row(%{"sequence_id" => 255_227})]).tasks
      assert numbered.sequence_id == 255_227

      assert [unnumbered] = Tasks.fetch(rows: [row(%{"sequence_id" => nil})]).tasks
      assert unnumbered.sequence_id == nil
    end

    test "completion is read from its own column and is nil while a task is running" do
      # `updated_at` moves on every progress report, so a duration taken from it is the time
      # since the last report of a task that may still be running.
      running =
        row(%{
          "status" => "processing",
          "completed_at" => nil,
          "updated_at" => ~U[2026-08-19 10:00:30Z]
        })

      done = row(%{"status" => "completed", "completed_at" => ~U[2026-08-19 10:01:00Z]})

      assert [task] = Tasks.fetch(rows: [running]).tasks
      assert task.completed_at == nil

      assert [finished] = Tasks.fetch(rows: [done]).tasks
      assert finished.completed_at == ~U[2026-08-19 10:01:00Z]
    end

    test "the read asks for every column the tree needs" do
      cql = Tasks.list_cql()

      for column <- ~w(parent_task_id component sequence_id task_type completed_at) do
        assert cql =~ column, "the read does not ask for #{column}"
      end
    end
  end

  describe "parent and child tasks" do
    test "a child is nested under the parent its payload names" do
      rows = [
        row(%{
          "task_id" => "parent",
          "action" => "upgrade",
          "created_at" => ~U[2026-08-19 10:00:00Z]
        }),
        row(%{
          "task_id" => "child",
          "action" => "upgrade_node",
          "payload" => ~s({"parent_task_id":"parent"}),
          "created_at" => ~U[2026-08-19 10:01:00Z]
        })
      ]

      assert [parent, child] = Tasks.fetch(rows: rows).tasks
      assert parent.id == "parent"
      assert parent.depth == 0
      assert child.id == "child"
      assert child.depth == 1
    end

    test "a child whose parent is not in the result set is shown at the top level" do
      row = row(%{"task_id" => "orphan", "payload" => ~s({"parent_task_id":"gone"})})

      assert [task] = Tasks.fetch(rows: [row]).tasks
      assert task.depth == 0
    end

    test "a cycle in parent_task_id terminates instead of hanging the socket" do
      rows = [
        row(%{"task_id" => "a", "payload" => ~s({"parent_task_id":"b"})}),
        row(%{"task_id" => "b", "payload" => ~s({"parent_task_id":"a"})}),
        row(%{"task_id" => "self", "payload" => ~s({"parent_task_id":"self"})})
      ]

      tasks = Tasks.fetch(rows: rows).tasks

      # Every row is still shown. Losing them all would be a far worse failure than
      # losing the indentation.
      assert Enum.map(tasks, & &1.id) |> Enum.sort() == ~w(a b self)
      assert Enum.all?(tasks, &(&1.depth == 0))
    end
  end

  describe "ordering and limits" do
    test "newest first, and rows with no timestamps sort last" do
      rows = [
        row(%{"task_id" => "old", "created_at" => ~U[2026-08-19 09:00:00Z]}),
        row(%{"task_id" => "new", "created_at" => ~U[2026-08-19 11:00:00Z]}),
        row(%{"task_id" => "undated", "created_at" => nil, "updated_at" => nil})
      ]

      assert ["new", "old", "undated"] == Enum.map(Tasks.fetch(rows: rows).tasks, & &1.id)
    end

    test "the cap is applied after ordering and is reported" do
      rows =
        for index <- 1..5 do
          at = DateTime.add(~U[2026-08-19 10:00:00Z], index, :second)
          row(%{"task_id" => "task-#{index}", "created_at" => at, "updated_at" => at})
        end

      snapshot = Tasks.fetch(rows: rows, limit: 2)

      assert Enum.map(snapshot.tasks, & &1.id) == ["task-5", "task-4"]
      assert snapshot.truncated?
      assert snapshot.limit == 2
    end
  end

  describe "availability" do
    test "an unreadable database is reported as unavailable, not as an empty history" do
      snapshot = Tasks.fetch(error: {:connection, :econnrefused})

      refute snapshot.available?
      assert snapshot.tasks == []
      assert snapshot.error =~ "econnrefused"
      assert snapshot.summary.total == 0
    end

    test "a readable but empty table is available with nothing in it" do
      snapshot = Tasks.fetch(rows: [])

      assert snapshot.available?
      assert snapshot.error == nil
      assert snapshot.tasks == []
      refute snapshot.truncated?
    end
  end

  describe "statement" do
    test "reads named columns rather than SELECT JSON *" do
      assert Tasks.list_cql() =~ "FROM hydra.catalyst_tasks"
      assert Tasks.list_cql() =~ "task_id"
      refute Tasks.list_cql() =~ "JSON"
    end
  end
end
