defmodule Planner.Sup do
  use ConsumerSupervisor

  def start_link(_) do
    ConsumerSupervisor.start_link(__MODULE__, :ok)
  end

  # Callbacks

  def init(:ok) do
    children = [
      worker(Planner.Worker, [], restart: :temporary)
    ]

    {:ok, children, strategy: :one_for_one, subscribe_to: [{Planner.Producer, max_demand: 100_000}]}
  end
end