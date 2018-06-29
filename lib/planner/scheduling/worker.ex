defmodule Planner.Scheduling.Worker do
  def start_link(msg_term) do
    IO.inspect msg_term
    execute(msg_term)
  end

  # без периода повтора
  defp execute({mfa, :no_period}) do
    delete_ref(mfa)
    doit(mfa)
  end

  # с периодом повтора
  defp execute({mfa, period}) do
    reschedule(mfa, period)
    doit(mfa)
  end

  # запуск
  defp doit({module, function, args}) do
    Task.start_link(fn ->
      apply(module, function, args)
    end)
  end

  # чистим dets schedulinga
  defp delete_ref(mfa) do
    :dets.delete(:schedule_dets, mfa)
  end

  # пинаем шедулер для повторного запуска и переписываем dets c новой ref
  defp reschedule(mfa, %{unix: diff} = period) do
    ref = Process.send_after(Planner.Scheduling.Producer, {:schedule, {mfa, period}}, diff)
    time_doit_unix = :os.system_time(:millisecond) + diff
    time_doit_iso8601 = time_doit_unix |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()
    :dets.insert(:schedule_dets, {mfa, ref, %{unix: time_doit_unix, iso8601: time_doit_iso8601}, period})
  end
end
