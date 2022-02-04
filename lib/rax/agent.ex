defmodule Rax.Agent do
  # API

  @spec new(Rax.Cluster.name(), any, any) :: any
  def new(cluster, agent, fun) do
    Rax.call(cluster, {:new, agent, fun})
    |> handle_result()
  end

  def delete(cluster, agent) do
    Rax.call(cluster, {:delete, agent})
  end

  def get(cluster, agent, fun) do
    Rax.query(cluster, &call(agent, fun, &1.agents))
    |> handle_result()
  end

  def get(cluster, agent, module, fun, args) do
    Rax.query(cluster, &call(agent, fn s -> Kernel.apply(module, fun, [s | args]) end, &1.agents))
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

  def set_auto_snapshot(cluster, n) do
    Rax.call(cluster, {:set_auto_snapshot, n})
  end

  defp handle_result(:ok), do: :ok
  defp handle_result({:ok, value}), do: value

  defp handle_result({:error, :no_agent, agent}),
    do: raise(ArgumentError, message: "agent not found: #{inspect(agent)}")

  # State Machine

  use Rax.Machine

  def init(opts), do: %{agents: %{}, auto_snapshot: opts[:auto_snapshot]}

  def apply(_meta, {:new, agent, fun}, state) do
    case state.agents do
      %{^agent => _} ->
        {:error, :agent_not_new, agent}

      _ ->
        {:ok, fun.()}
    end
    |> handle_update_result(agent, state)
  end

  def apply(_meta, {:delete, agent}, state) do
    {Map.update!(state, :agents, &Map.delete(&1, agent)), :ok}
  end

  def apply(_meta, {:get_and_update, agent, fun}, state) do
    call(agent, fun, state.agents)
    |> handle_get_and_update_result(agent, state)
  end

  def apply(_meta, {:get_and_update, agent, mod, fun, args}, state) do
    call(agent, fn s -> Kernel.apply(mod, fun, [s | args]) end, state.agents)
    |> handle_get_and_update_result(agent, state)
  end

  def apply(_meta, {:update, agent, fun}, state) do
    call(agent, fun, state.agents)
    |> handle_update_result(agent, state)
  end

  def apply(_meta, {:update, agent, mod, fun, args}, state) do
    call(agent, fn s -> Kernel.apply(mod, fun, [s | args]) end, state.agents)
    |> handle_update_result(agent, state)
  end

  def apply(meta, cmd, state) do
    super(meta, cmd, state)
  end

  defp call(agent, fun, state) do
    if agent_state = Map.get(state, agent) do
      {:ok, fun.(agent_state)}
    else
      {:error, :no_agent, agent}
    end
  end

  defp handle_get_and_update_result({:ok, {reply, agent_state}}, agent, state) do
    {put_in(state, [:agents, agent], agent_state), {:ok, reply}}
  end

  defp handle_get_and_update_result({:ok, badret}, _agent, _state) do
    raise ArgumentError,
      message: "bad return value from agent, expected {reply, state}, got: #{inspect(badret)}"
  end

  defp handle_get_and_update_result(error, _agent, state) do
    {state, error}
  end

  defp handle_update_result({:ok, agent_state}, agent, state) do
    {put_in(state, [:agents, agent], agent_state), :ok}
  end

  defp handle_update_result(error, _agent, state) do
    {state, error}
  end
end
