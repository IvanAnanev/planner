defmodule Planner.Worker do
  alias Planner.Storage
  require Logger

  def start_link(msg_term) do
    Logger.info("Doit #{inspect msg_term}")
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
    Task.start fn ->
      apply(module, function, args)
    end
  end

  # чистим dets schedulinga
  defp delete_ref(mfa) do
    Storage.delete(mfa)
  end

  # пинаем шедулер для повторного запуска и переписываем dets c новой ref
  defp reschedule(mfa, %{unix: diff} = period) do
    ref = Process.send_after(Planner.Producer, {:schedule, {mfa, period}}, diff)
    time_doit_unix = :os.system_time(:millisecond) + diff
    time_doit_iso8601 = time_doit_unix |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()
    Storage.set({mfa, ref, %{unix: time_doit_unix, iso8601: time_doit_iso8601}, period})
  end
end
