defmodule SpectrumPhxWeb.Tasks.RingLiveTest do
  @moduledoc """
  The task ring's geometry and state machine.

  A port of the Python console's header widget, and the numbers are the port: an arc
  drawn against the wrong circumference is a ring that never quite fills, and a scale
  that does not track progress loses the thing that made it feel alive.
  """
  use ExUnit.Case, async: true

  alias SpectrumPhxWeb.Tasks.RingLive

  defp task(opts) do
    %{
      id: Keyword.get(opts, :id, "t1"),
      short_id: "t1",
      label: Keyword.get(opts, :label, "vali stop"),
      state: Keyword.get(opts, :state, :processing),
      progress: Keyword.get(opts, :progress, 0),
      error: Keyword.get(opts, :error),
      depth: 0
    }
  end

  describe "the arc" do
    test "is drawn against the circumference of an r=9 circle" do
      # 2 * pi * 9 = 56.548..., which the original rounds to 56.55 in both the CSS
      # dasharray and the JS. Drawing against anything else leaves the ring never quite
      # full, or full before the task is.
      assert RingLive.circumference() == 56.55
    end

    test "nothing done draws nothing" do
      assert RingLive.offset(0) == 56.55
    end

    test "finished draws the whole ring" do
      assert RingLive.offset(100) == 0.0
    end

    test "half done draws half the ring" do
      # 56.55 / 2 is 28.275, which is 28.2749... in binary and so rounds down.
      assert RingLive.offset(50) == 28.27
    end

    test "a progress outside 0..100 is clamped rather than drawn past the ring" do
      assert RingLive.offset(-20) == 56.55
      assert RingLive.offset(140) == 0.0
    end

    test "a progress that is not a number leaves the ring empty" do
      assert RingLive.offset(nil) == 56.55
    end
  end

  describe "the scale" do
    test "grows from 1.0 to 1.3 as the task approaches done" do
      # The detail that makes it feel alive: what you are watching gets bigger the
      # closer it is to finishing.
      assert RingLive.scale(0) == 1.0
      assert RingLive.scale(50) == 1.15
      assert RingLive.scale(100) == 1.3
    end

    test "is clamped, so a bad progress cannot inflate the header" do
      assert RingLive.scale(500) == 1.3
      assert RingLive.scale(nil) == 1.0
    end
  end

  describe "state" do
    test "nothing at all is idle, with an empty ring" do
      ring = RingLive.ring([], [])

      assert ring.state == :idle
      assert ring.count == 0
      assert ring.offset == 56.55
      refute ring.spin?
    end

    test "anything active spins, and fills to the first active task" do
      active = [task(progress: 40), task(id: "t2", progress: 90)]
      ring = RingLive.ring(active, active)

      assert ring.state == :running
      assert ring.spin?
      assert ring.progress == 40
      assert ring.offset == RingLive.offset(40)
      assert ring.count == 2, "the badge counts what is running, not what is remembered"
    end

    test "with nothing running, the most recent outcome colours a full ring" do
      done = [task(id: "t1", state: :completed, progress: 100)]
      ring = RingLive.ring([], done)

      assert ring.state == :done
      assert ring.offset == 0.0
      refute ring.spin?
    end

    test "a failure is reported ahead of the ring being full" do
      failed = [task(id: "t1", state: :failed, progress: 100)]
      assert RingLive.ring([], failed).state == :failed
    end

    test "an idle ring is never scaled up" do
      assert RingLive.ring([], []).scale == 1.0
      assert RingLive.ring([], [task(state: :completed)]).scale == 1.0
    end
  end

  describe "announcements" do
    test "a task that appears is announced by name" do
      assert {"vali stop", nil} = RingLive.transition([], [task(label: "vali stop")])
    end

    test "a task that finishes wins over one that starts" do
      # An operator who walked away wants to know how it went, not that it began.
      before = [task(id: "old", state: :processing)]
      now = [task(id: "old", state: :completed, label: "migrate web-01"), task(id: "new")]

      assert {"migrate web-01 finished", nil} = RingLive.transition(before, now)
    end

    test "a failure wins over a completion, and carries its reason" do
      before = [task(id: "a", state: :processing), task(id: "b", state: :processing)]

      now = [
        task(id: "a", state: :completed, label: "ok one"),
        task(id: "b", state: :failed, label: "start test", error: "no host had room")
      ]

      assert {"start test failed", "no host had room"} = RingLive.transition(before, now)
    end

    test "a list that has not changed announces nothing" do
      same = [task(id: "a", state: :processing)]
      assert RingLive.transition(same, same) == nil
    end

    test "a task already finished when first seen is not announced as finishing" do
      # Otherwise every page load replays the last hour's history at the operator.
      assert RingLive.transition([], [task(id: "a", state: :completed)]) == nil
    end
  end
end
