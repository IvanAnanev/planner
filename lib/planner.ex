defmodule Planner do
  alias Planner.Scheduling.Producer
  alias Planner.Scheduling.SomeModule

  @count 100
  @some_time 60_000 # 1 minute

  def hello do
    :observer.start
    # 10 потоков
    step = div(@count, 10)
    for x <- 1..10 do
      start_pos = (x - 1) * step + 1
      end_pos = x * step
      Task.start fn ->
        Enum.each(start_pos..end_pos, &doit/1)
      end
    end
  end

  defp doit(i) do
    IO.inspect(i)
    Producer.schedule({SomeModule, :execute, [1, i]}, {:unix, :os.system_time(:millisecond) + @some_time})
  end
end
