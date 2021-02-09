defmodule Rax.Agent do
  def start(cluster, agent, fun) do
    Rax.call(cluster, {:start, agent, fun})
  end

  def stop(cluster, agent) do
    Rax.call(cluster, {:stop, agent})
  end

  def get(cluster, agent, fun) do
    Rax.query(cluster, &safe_call(agent, fun, &1))
    |> handle_result()
  end

  def get(cluster, agent, module, fun, args) do
    Rax.query(cluster, &safe_call(agent, fn s -> Kernel.apply(module, fun, [s | args]) end, &1))
    |> handle_result()
  end

  def get_and_update(cluster, agent, fun) do
    Rax.call(cluster, {:get_and_update, agent, fun})
    |> handle_result()
  end

  def get_and_update(cluster, agent, module, fun, args) do
    Rax.call(cluster, {:get_and_update, agent, module, fun, args})
    |> handle_result()
  end

  def update(cluster, agent, fun) do
    Rax.call(cluster, {:update, agent, fun})
    |> handle_result()
  end

  def update(cluster, agent, module, fun, args) do
    Rax.call(cluster, {:update, agent, fn s -> Kernel.apply(module, fun, [s | args]) end})
    |> handle_result()
  end

  def cast(cluster, agent, fun) do
    Rax.cast(cluster, {:update, agent, fun})
  end

  def cast(cluster, agent, module, fun, args) do
    Rax.cast(cluster, {:update, agent, fn s -> Kernel.apply(module, fun, [s | args]) end})
  end

  defp safe_call(agent, fun, state) do
    if agent_state = Map.get(state, agent) do
      try do
        {:ok, fun.(agent_state)}
      catch
        :error, value ->
          {:error, :error, value, __STACKTRACE__}
        kind, value ->
          {:error, kind, value}
      end
    else
      {:error, :no_agent, agent}
    end
  end

  defp handle_result(:ok), do: :ok
  defp handle_result({:ok, value}), do: value
  defp handle_result({:error, :no_agent, agent}), do: raise ArgumentError, message: "agent not found: #{inspect agent}"
  defp handle_result({:error, :badarg}), do: raise ArgumentError
  defp handle_result({:error, :error, ex, st}), do: raise Exception.normalize(:error, ex, st)
  defp handle_result({:error, :exit, reason}), do: exit(reason)
  defp handle_result({:error, :throw, value}), do: throw(value)

  # State Machine

  def init(_), do: %{}

  def apply(_meta, {:start, agent, fun}, state) do
    try do
      {:ok, fun.()}
    catch
      :error, value ->
        {:error, :error, value, __STACKTRACE__}
      kind, value ->
        {:error, kind, value}
    end
    |> case do
      {:ok, agent_state} ->
        {Map.put(state, agent, agent_state), :ok}

      error ->
        {state, error}
    end
  end

  def apply(_meta, {:stop, agent}, state) do
    {Map.delete(state, agent), :ok}
  end

  def apply(_meta, {:get_and_update, agent, fun}, state) do
    safe_call(agent, fun, state)
    |> handle_get_and_update_result(agent, state)
  end

  def apply(_meta, {:get_and_update, agent, mod, fun, args}, state) do
    safe_call(agent, fn s -> Kernel.apply(mod, fun, [s | args]) end, state)
    |> handle_get_and_update_result(agent, state)
  end

  def apply(_meta, {:update, agent, fun}, state) do
    safe_call(agent, fun, state)
    |> handle_update_result(agent, state)
  end

  def apply(_meta, {:update, agent, mod, fun, args}, state) do
    safe_call(agent, fn s -> Kernel.apply(mod, fun, [s | args]) end, state)
    |> handle_update_result(agent, state)
  end

  defp handle_get_and_update_result({:ok, {reply, agent_state}}, agent, state) do
    {Map.put(state, agent, agent_state), {:ok, reply}}
  end
  defp handle_get_and_update_result({:ok, _badarg}, _agent, state) do
    {state, {:error, :badarg}}
  end
  defp handle_get_and_update_result(error, _agent, state) do
    {state, error}
  end

  defp handle_update_result({:ok, agent_state}, agent, state) do
    {Map.put(state, agent, agent_state), :ok}
  end
  defp handle_update_result(error, _agent, state) do
    {state, error}
  end

end
