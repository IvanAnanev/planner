defmodule Planner.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    # List all child processes to be supervised
    children = [
      # Starts a worker by calling: Planner.Worker.start_link(arg)
      # {Planner.Worker, arg},
      {Planner.Storage, []},
      {Planner.Producer, []},
      {Planner.Sup, []},
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Planner.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
