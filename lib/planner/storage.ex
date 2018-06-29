defmodule Planner.Storage do
  use GenServer
  alias Planner.Producer
  require Logger

  @schedule_dets :schedule_dets
  @schedule_dets_file 'schedule_dets_file'

  ## init

  def start_link(_), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  def init(_) do
    Process.flag(:trap_exit, true)
    {:ok, %{}, {:continue, :init_dets}}
  end

  ## API

  def set(schedule_term), do: GenServer.cast(__MODULE__, {:set, schedule_term})

  def get(mfa_term), do: GenServer.call(__MODULE__, {:get, mfa_term})

  def delete(mfa_term), do: GenServer.cast(__MODULE__, {:delete, mfa_term})

  ## Callbacks

  def handle_continue(:init_dets, state) do
    Logger.info("start init schedule storage")
    # открываем dets
    :dets.open_file(@schedule_dets, [type: :set, file: @schedule_dets_file])
    # перезапускаем таймеры
    reschedule_dets()

    {:noreply, state}
  end

  def handle_cast({:set, schedule_term}, state) do
    :dets.insert(@schedule_dets, schedule_term)

    {:noreply, state}
  end

  def handle_cast({:delete, mfa_term}, state) do
    :dets.delete(@schedule_dets, mfa_term)

    {:noreply, state}
  end

  def handle_call({:get, mfa_term}, _from, state) do
    response = :dets.lookup(@schedule_dets, mfa_term)

    {:reply, response, state}
  end

  def terminate(reason, state) do
    :dets.close(@schedule_dets)
    Logger.info("close dets storage")

    reason
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
        ref = Process.send_after(Producer, {:schedule, {mfa_term, period}}, diff_time)
        :dets.insert(@schedule_dets, {mfa_term, ref, time_doit, period})

      _ ->
        Process.send(Producer, {:schedule, {mfa_term, period}}, [])
    end
    # используем механизм first next, что б не перегружать память обработки всех записей
    case :dets.next(@schedule_dets, key) do
      :"$end_of_table" ->
        Logger.info("end init dets")
        :ok

      next_key -> reschedule_record(next_key)
    end
  end
end
