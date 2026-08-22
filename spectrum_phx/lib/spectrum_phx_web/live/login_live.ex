defmodule SpectrumPhxWeb.LoginLive do
  @moduledoc """
  Sign-in page.

  The form posts to `SessionController` rather than handling a LiveView event, because
  establishing a session cookie needs a real HTTP response.
  """
  use SpectrumPhxWeb, :live_view

  alias SpectrumPhx.Accounts

  @impl true
  def mount(_params, _session, socket) do
    # Surface database trouble here rather than letting it look like a wrong password.
    db_state =
      case Accounts.user_count() do
        {:ok, 0} -> :no_users
        {:ok, _} -> :ok
        {:error, _} -> :unavailable
      end

    {:ok, assign(socket, db_state: db_state, page_title: "Sign in"), layout: false}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="min-h-screen flex items-center justify-center bg-base-200 px-4">
      <div class="w-full max-w-sm">
        <div class="text-center mb-8">
          <h1 class="text-3xl font-semibold tracking-tight">Helios</h1>
          <p class="text-sm text-base-content/60 mt-1">Hyper-converged infrastructure</p>
        </div>

        <div class="card bg-base-100 shadow-sm">
          <div class="card-body gap-4">
            <SpectrumPhxWeb.Layouts.flash_group flash={@flash} />

            <div :if={@db_state == :unavailable} class="alert alert-warning text-sm">
              <span>
                The metadata database is unreachable, so sign-in cannot be verified.
                Check that <code>hydra-db</code> and <code>daruk</code> are running.
              </span>
            </div>

            <div :if={@db_state == :no_users} class="alert alert-info text-sm">
              <span>No accounts exist yet. The cluster seeds <code>helios</code> on first boot.</span>
            </div>

            <form action={~p"/login"} method="post" class="flex flex-col gap-3">
              <input type="hidden" name="_csrf_token" value={Plug.CSRFProtection.get_csrf_token()} />

              <label class="form-control w-full">
                <span class="label-text mb-1">Username</span>
                <input
                  type="text"
                  name="username"
                  autocomplete="username"
                  autofocus
                  required
                  class="input input-bordered w-full"
                />
              </label>

              <label class="form-control w-full">
                <span class="label-text mb-1">Password</span>
                <input
                  type="password"
                  name="password"
                  autocomplete="current-password"
                  required
                  class="input input-bordered w-full"
                />
              </label>

              <button type="submit" class="btn btn-primary w-full mt-2">Sign in</button>
            </form>
          </div>
        </div>
      </div>
    </div>
    """
  end
end
