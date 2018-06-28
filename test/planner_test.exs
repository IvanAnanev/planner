defmodule PlannerTest do
  use ExUnit.Case
  doctest Planner

  test "greets the world" do
    assert Planner.hello() == :world
  end
end
