defmodule SpectrumPhx.UploadStubs do
  @moduledoc """
  Stand-ins for the two things an image upload cannot do in a test: talk to Spark, and
  hold a socket open.

  Both are configured through application env and report every call back to the test
  process, so a test asserts on the *sequence* -- allocate, promote, write, permissions,
  flush, demote, register, and the rollback -- rather than on a mock's return value. That
  ordering is the part with the failure modes: a resource created and never deleted holds
  storage on every node, and a row written before the bytes land is an image that looks
  usable and is not.
  """

  defmodule Uploader do
    @moduledoc "Stands in for `SpectrumPhx.Images.SparkUploader`."

    def linstor_create(ip, resource, size_kib) do
      report({:linstor_create, ip, resource, size_kib})
      answer(:linstor_create, {:ok, %{"resource" => resource, "created" => true}})
    end

    def linstor_delete(ip, resource) do
      report({:linstor_delete, ip, resource})
      answer(:linstor_delete, {:ok, %{"deleted" => true}})
    end

    def device_info(ip, device) do
      report({:device_info, ip, device})
      answer(:device_info, {:ok, %{"is_block" => true}})
    end

    def drbd_role(ip, resource, role) do
      report({:drbd_role, ip, resource, role})
      answer(:drbd_role, {:ok, %{"role" => role}})
    end

    def device_prepare(ip, device, owner, mode) do
      report({:device_prepare, ip, device, owner, mode})
      answer(:device_prepare, {:ok, %{}})
    end

    def device_flush(ip, device) do
      report({:device_flush, ip, device})
      answer(:device_flush, {:ok, %{}})
    end

    defp report(message), do: SpectrumPhx.UploadStubs.report(message)
    defp answer(key, default), do: SpectrumPhx.UploadStubs.answer(key, default)
  end

  defmodule Transport do
    @moduledoc """
    Stands in for `SpectrumPhx.Images.UploadWriter.MintTransport`.

    Accumulates the chunks it is given so a test can assert the bytes arrived intact and
    in order -- the property a streaming writer is most likely to get wrong.
    """

    def open(allocation, size_bytes) do
      SpectrumPhx.UploadStubs.report({:open, allocation.node, allocation.device, size_bytes})

      case SpectrumPhx.UploadStubs.answer(:open, :ok) do
        :ok -> {:ok, %{written: "", size_bytes: size_bytes}}
        {:error, reason} -> {:error, reason}
      end
    end

    def send_chunk(handle, data) do
      SpectrumPhx.UploadStubs.report({:chunk, byte_size(data)})

      case SpectrumPhx.UploadStubs.answer(:send_chunk, :ok) do
        :ok -> {:ok, %{handle | written: handle.written <> data}}
        {:error, reason} -> {:error, reason}
      end
    end

    def finish(handle) do
      SpectrumPhx.UploadStubs.report({:finish, handle.written})
      SpectrumPhx.UploadStubs.answer(:finish, {:ok, byte_size(handle.written)})
    end

    def close(_handle) do
      SpectrumPhx.UploadStubs.report(:closed)
      :ok
    end
  end

  @doc """
  Point `SpectrumPhx.Images` and the writer at the stubs for the duration of one test.

  `answers` overrides individual calls, e.g. `%{drbd_role: {:ok, %{"role" => "secondary"}}}`
  to make a promotion fail to take.
  """
  def install(answers \\ %{}) do
    Application.put_env(:spectrum_phx, :images_uploader, Uploader)
    Application.put_env(:spectrum_phx, :images_upload_transport, Transport)
    Application.put_env(:spectrum_phx, :upload_stub_owner, self())
    Application.put_env(:spectrum_phx, :upload_stub_answers, answers)
    # The real rollback retries for six seconds because the device is released
    # asynchronously. A test has no such device, so it must not wait for one.
    Application.put_env(:spectrum_phx, :images_rollback_attempts, 2)
    Application.put_env(:spectrum_phx, :images_rollback_interval_ms, 1)

    ExUnit.Callbacks.on_exit(fn ->
      Application.delete_env(:spectrum_phx, :images_uploader)
      Application.delete_env(:spectrum_phx, :images_upload_transport)
      Application.delete_env(:spectrum_phx, :upload_stub_owner)
      Application.delete_env(:spectrum_phx, :upload_stub_answers)
      Application.delete_env(:spectrum_phx, :images_rollback_attempts)
      Application.delete_env(:spectrum_phx, :images_rollback_interval_ms)
    end)

    :ok
  end

  @doc false
  def report(message) do
    case Application.get_env(:spectrum_phx, :upload_stub_owner) do
      pid when is_pid(pid) -> send(pid, {:upload_stub, message})
      _other -> :ok
    end
  end

  @doc false
  def answer(key, default) do
    :spectrum_phx
    |> Application.get_env(:upload_stub_answers, %{})
    |> Map.get(key, default)
  end
end
