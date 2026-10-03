defmodule SpectrumPhx.SparkProbeRetryTest do
  use ExUnit.Case, async: true

  # The new-VM page probes every host for SPICE while it loads. Req retries a closed socket with
  # back-off, so one host that dropped the connection stalled the page for about six seconds.
  # A probe made during a page load must not retry.
  @source Path.expand("../../lib/spectrum_phx/spark.ex", __DIR__)

  test "the capabilities probe does not retry and has a short timeout" do
    source = File.read!(@source)

    assert source =~ ~r/def host_capabilities\(ip\).*retry: false/s
    assert source =~ ~r/def host_capabilities\(ip\).*timeout: 4/s
  end

  test "get_json passes the retry option to Req and defaults to Req's own behaviour" do
    source = File.read!(@source)

    assert source =~ "retry = Keyword.get(opts, :retry, :safe_transient)"
    assert source =~ "retry: retry"
  end
end
