defmodule SpectrumPhxWeb.PageController do
  use SpectrumPhxWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
