defmodule Planner.Scheduling.Producer do
  use GenStage
  require Logger

  def start_link(_) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @schedule_dets :schedule_dets
  @schedule_dets_file 'schedule_dets_file'

  @second 1_000
  @minute 60_000
  @hour   3_600_000
  @day    86_400_000

  ## Callbacks

  def init(_) do
    GenServer.cast(__MODULE__, :init_dets)
    {:producer, {:queue.new(), 0}, dispatcher: GenStage.BroadcastDispatcher}
  end

  ## API

  # апи для пинка
  def schedule(mfa_term, time_doit, opts \\ []) do
    %{mfa_term: mfa_term, time_doit: time_doit, opts: opts}
    |> parse_time()
    |> parse_period()
    |> cancel_old_timer()
    |> diff_time()
    |> to_schedule()
    |> save_ref()
    |> log()
  end

  # парсим время
  # возможны форматы {:unix, 1523622513603} | {:iso8601, "2018-04-13T12:28:33.603Z"}
  defp parse_time(%{time_doit: {:unix, unix_time}} = storage_map) when is_integer(unix_time) do
    case DateTime.from_unix!(unix_time, :millisecond) do
      datetime ->
        doit_iso8601 = DateTime.to_iso8601(datetime)
        {:ok, Map.merge(storage_map, %{time_doit: %{unix: unix_time, iso8601: doit_iso8601}})}

      _ ->
        {:error, "Can't parse time #{inspect {:unix, unix_time}}"}
    end
  end
  defp parse_time(%{time_doit: {:iso8601, iso8601_time}} = storage_map) when is_bitstring(iso8601_time) do
    case DateTime.from_iso8601(iso8601_time) do
      {:ok, datetime, _} ->
        doit_unix = DateTime.to_unix(datetime, :millisecond)
        {:ok, Map.merge(storage_map, %{time_doit: %{unix: doit_unix, iso8601: iso8601_time}})}
      _ ->
        {:error, "Can't parse time #{inspect {:iso8601, iso8601_time}}"}
    end
  end
  defp parse_time(%{time_doit: time_doit}), do: {:error, "Can't parse time #{inspect time_doit}"}

  # парсим период
  defp parse_period({:error, _} = e), do: e
  defp parse_period({:ok, %{opts: []} = storage_map}) do
    {:ok, Map.merge(storage_map, %{period: :no_period})}
  end
  defp parse_period({:ok, %{opts: [period: {:second, count}]} = storage_map}) when is_integer(count) and count > 0 do
    {:ok, Map.merge(storage_map, %{period: %{unix: count * @second, human: {:second, count}}})}
  end
  defp parse_period({:ok, %{opts: [period: {:minute, count}]} = storage_map}) when is_integer(count) and count > 0 do
    {:ok, Map.merge(storage_map, %{period: %{unix: count * @minute, human: {:minute, count}}})}
  end
  defp parse_period({:ok, %{opts: [period: {:hour, count}]} = storage_map}) when is_integer(count) and count > 0 do
    {:ok, Map.merge(storage_map, %{period: %{unix: count * @hour, human: {:hour, count}}})}
  end
  defp parse_period({:ok, %{opts: [period: {:day, count}]} = storage_map}) when is_integer(count) and count > 0 do
    {:ok, Map.merge(storage_map, %{period: %{unix: count * @day, human: {:day, count}}})}
  end
  defp parse_period({:ok, %{opts: [period: period]}}) do
    {:error, "Can't parse period #{inspect period}"}
  end

  # отменяем старый таймер для mfa_term
  defp cancel_old_timer({:error, _} = e), do: e
  defp cancel_old_timer({:ok, %{mfa_term: mfa_term}} = ok) do
    case :dets.lookup(@schedule_dets, mfa_term) do
      [{_mfa, ref, _time_doit, _period}] -> Process.cancel_timer(ref)
      [] -> :ok
    end
    ok
  end

  # разница времени
  defp diff_time({:error, _} = e), do: e
  defp diff_time({:ok, %{time_doit: %{unix: doit_unix}} = storage_map}) do
    unix_time_now = :os.system_time(:millisecond)
    case doit_unix - unix_time_now do
      diff_time when diff_time > 0 ->
        {:ok, Map.merge(storage_map, %{diff_time: diff_time})}

      _ ->
        {:ok, Map.merge(storage_map, %{diff_time: 0})}
    end
  end

  # шедулим
  defp to_schedule({:error, _} = e), do: e
  defp to_schedule({:ok, %{mfa_term: mfa_term, period: period, diff_time: 0} = storage_map}) do
    Process.send(__MODULE__, {:schedule, {mfa_term, period}}, [])
    {:ok, Map.merge(storage_map, %{ref: :no_ref})}
  end
  defp to_schedule({:ok, %{mfa_term: mfa_term, period: period, diff_time: diff_time} = storage_map}) do
    ref = Process.send_after(__MODULE__, {:schedule, {mfa_term, period}}, diff_time)
    {:ok, Map.merge(storage_map, %{ref: ref})}
  end

  # сохраняем ref в dets
  defp save_ref({:error, _} = e), do: e
  defp save_ref({:ok, %{ref: :no_ref}} = ok), do: ok
  defp save_ref({:ok, %{mfa_term: mfa_term, time_doit: time_doit, period: period, ref: ref}} = ok) do
    :dets.insert(@schedule_dets, {mfa_term, ref, time_doit, period})
    ok
  end

  defp log(result), do: IO.inspect(result)

  # инитим таблицу
  def handle_cast(:init_dets, state) do
    Logger.info("start init dets")
    # открываем dets
    :dets.open_file(@schedule_dets, [type: :set, file: @schedule_dets_file])
    # перезапускаем таймеры
    reschedule_dets()
    {:noreply, [], state}
  end

  defp reschedule_dets() do
    case :dets.first(@schedule_dets) do
      :"$end_of_table" ->
        Logger.info("end init dets")
        :ok
      key -> reschedule_record(key)
    end
  end

  defp reschedule_record(key) do
    Logger.info("reschedule #{inspect key}")
    [{mfa_term, _ref, time_doit, period}] = :dets.lookup(@schedule_dets, key)
    case time_doit.unix - :os.system_time(:millisecond) do
      diff_time when diff_time > 0 ->
        ref = Process.send_after(__MODULE__, {:schedule, {mfa_term, period}}, diff_time)
        :dets.insert(@schedule_dets, {mfa_term, ref, time_doit, period})
      _ ->
        Process.send(__MODULE__, {:schedule, {mfa_term, period}}, [])
    end
    # используем механизм first next, что б не перегружать память обработки всех записей
    case :dets.next(@schedule_dets, key) do
      :"$end_of_table" ->
        Logger.info("end init dets")
        :ok
      next_key -> reschedule_record(next_key)
    end
  end

  # здесь принимаем пинок о запуске планировщика
  # который можно будет сделать так
  # Process.send_after(Planner.Scheduling.Producer, {:schedule, mfa_term}, time_different)
  def handle_info({:schedule, mfa_term}, {queue, demand}) do
    dispatch(:queue.in(mfa_term, queue), demand, [])
  end

  # магия GenStage
  def handle_demand(incoming_demand, {queue, demand}) do
    dispatch(queue, incoming_demand + demand, [])
  end

  defp dispatch(queue, 0, mfa_terms) do
    {:noreply, Enum.reverse(mfa_terms), {queue, 0}}
  end

  defp dispatch(queue, demand, mfa_terms) do
    case :queue.out(queue) do
      {{:value, mfa_term}, queue} ->
        dispatch(queue, demand - 1, [mfa_term | mfa_terms])

      {:empty, queue} ->
        {:noreply, Enum.reverse(mfa_terms), {queue, demand}}
    end
  end
end