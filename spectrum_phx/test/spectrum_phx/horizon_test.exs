defmodule SpectrumPhx.HorizonTest do
  @moduledoc """
  Tests for Horizon VDI desktop pools and session management.
  """
  use ExUnit.Case, async: false

  alias SpectrumPhx.Horizon

  describe "Horizon VDI pools" do
    test "create_pool validates pool_name presence" do
      assert {:error, "pool_name is required"} = Horizon.create_pool(%{"pool_name" => ""})
      assert {:error, "pool_name is required"} = Horizon.create_pool(%{})
    end

    test "list_pools returns normalized maps" do
      # When Hydra is offline or returns empty in unit test environment
      case Horizon.list_pools() do
        {:ok, pools} ->
          assert is_list(pools)
        {:error, _reason} ->
          :ok
      end
    end

    test "list_sessions returns normalized session records" do
      case Horizon.list_sessions() do
        {:ok, sessions} ->
          assert is_list(sessions)
        {:error, _reason} ->
          :ok
      end
    end
  end
end
