defmodule SpectrumPhx.UploadStubs do
  @moduledoc """
  Stand-ins for the two things an image upload cannot do in a test: talk to Spark, and
  hold a socket open.

  Both are configured through application env and report every call back to the test
  process, so a test asserts on the *sequence* -- create, attach, write, seal, register,
  and the rollback -- rather than on a mock's return value. That ordering is the part with
  the failure modes: a vdisk created and never deleted holds storage on every replica, an
  unsealed image is a writable template, and a row written before the bytes land is an
  image that looks usable and is not.
  """

  defmodule Uploader do
    @moduledoc "Stands in for `SpectrumPhx.Images.SparkUploader`."

    def create(ip, vdisk, size_bytes) do
      report({:create, ip, vdisk, size_bytes})
      answer(:create, {:ok, %{"vdisk_id" => vdisk, "created" => true}})
    end

    def attach(ip, vdisk) do
      report({:attach, ip, vdisk})
      answer(:attach, {:ok, %{"socket" => "/var/lib/hci/sidon/nbd/#{vdisk}.sock"}})
    end

    def detach(ip, vdisk) do
      report({:detach, ip, vdisk})
      answer(:detach, {:ok, %{}})
    end

    def seal(ip, vdisk) do
      report({:seal, ip, vdisk})
      answer(:seal, {:ok, %{"class" => "immutable"}})
    end

    def delete(ip, vdisk) do
      report({:delete, ip, vdisk})
      answer(:delete, {:ok, %{"deleted" => true}})
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
      SpectrumPhx.UploadStubs.report({:open, allocation.node, allocation.vdisk, size_bytes})

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

  `answers` overrides individual calls, e.g. `%{attach: {:error, {409, "hci-02 owns it"}}}`
  to make the ownership claim lose.
  """
  def install(answers \\ %{}) do
    Application.put_env(:spectrum_phx, :images_uploader, Uploader)
    Application.put_env(:spectrum_phx, :images_upload_transport, Transport)
    Application.put_env(:spectrum_phx, :upload_stub_owner, self())
    Application.put_env(:spectrum_phx, :upload_stub_answers, answers)
    # The real rollback retries for six seconds because the vdisk is released
    # asynchronously. A test has no such vdisk, so it must not wait for one.
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
