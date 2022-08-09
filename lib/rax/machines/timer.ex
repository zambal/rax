defmodule Rax.Timer do
  require Logger

  # API

  @type opts :: [opt]
  @type opt ::
          {:interval, non_neg_integer()}
          | {:type, :repeat | :once}

  @type func :: (() -> any())

  @type name :: atom()

  @default_opts [
    interval: 1000,
    type: :repeat
  ]

  @spec set(Rax.Cluster.name(), name(), func(), opts()) :: :ok
  def set(cluster, name, fun, opts \\ []) when is_function(fun, 0) do
    case init_opts(opts) do
      {:ok, opts} ->
        Rax.call(cluster, {:set_timer, name, fun, opts, cluster})

      :error ->
        raise ArgumentError, message: "invalid opts: #{inspect(opts)}"
    end
  end

  @spec remove(Rax.Cluster.name(), atom()) :: :ok | nil
  def remove(cluster, name) do
    Rax.call(cluster, {:remove, name})
  end

  @spec remove_all(Rax.Cluster.name()) :: :ok
  def remove_all(cluster) do
    Rax.call(cluster, :remove_all)
  end

  @spec list(Rax.Cluster.name()) :: Keyword.t()
  def list(cluster) do
    Rax.query(cluster, fn state ->
      for {name, {_fun, opts, _cluster, _busy}} <- state do
        {name, opts}
      end
    end)
  end

  # State machine

  use Rax.Machine
  @doc false
  def init(_config) do
    %{}
  end

  @doc false
  def apply(_meta, {:set_timer, name, fun, opts, cluster}, state) do
    state = Map.put(state, name, {fun, opts, cluster, false})
    interval = Keyword.fetch!(opts, :interval)
    {state, :ok, [{:timer, name, interval}]}
  end

  def apply(_meta, {:remove, name}, state) do
    if Map.has_key?(state, name) do
      {Map.delete(state, name), :ok, [{:timer, name, :infinity}]}
    else
      {state, nil}
    end
  end

  def apply(_meta, :remove_all, state) do
    effects =
      for {name, _} <- state do
        {:timer, name, :infinity}
      end

    {%{}, :ok, effects}
  end

  def apply(_meta, {:timeout, name}, state) do
    case Map.fetch(state, name) do
      {:ok, {fun, opts, cluster, false}} ->
        effect = {:mod_call, Rax.Timer, :apply_fun, [fun, cluster, name]}
        {state, effects} = handle_state(state, name, opts)
        {state, :ok, [effect | effects]}

      {:ok, {_fun, _opts, cluster, true}} ->
        Logger.warn("Rax timer #{cluster}:#{name} is still busy, skipping current timeout.")
        {state, nil}

      :error ->
        {state, nil}
    end
  end

  def apply(_meta, {:reset_busy, name}, state) do
    # Reset busy state to false
    state =
      Map.fetch!(state, name)
      |> put_elem(3, false)

      {state, :ok}
  end

  def apply(meta, cmd, state) do
    super(meta, cmd, state)
  end

  @doc false
  def state_enter(:leader, state) do
    for {name, {_fun, opts}} <- state do
      interval = Keyword.fetch!(opts, :interval)
      {:timer, name, interval}
    end
  end

  def state_enter(_, _state) do
    []
  end

  @doc false
  def apply_fun(fun, cluster, name) do
    spawn(fn ->
      try do
        fun.()
      catch
        kind, payload ->
          err = Exception.format(kind, payload, __STACKTRACE__)
          Logger.error(err)
      end
      Rax.cast(cluster, {:reset_busy, name})
    end)
  end

  defp handle_state(state, name, opts) do
    case Keyword.fetch!(opts, :type) do
      :once ->
        {Map.delete(state, name), []}

      :repeat ->
        # Set to busy
        state =
          Map.fetch!(state, name)
          |> put_elem(3, true)

        interval = Keyword.fetch!(opts, :interval)
        {state, [{:timer, name, interval}]}
    end
  end

  defp init_opts(opts) do
    opts = Keyword.merge(@default_opts, opts)

    with {:ok, n} when (is_integer(n) and n >= 0) or n == :infinity <-
           Keyword.fetch(opts, :interval),
         {:ok, type} when type in [:repeat, :once] <- Keyword.fetch(opts, :type) do
      {:ok, opts}
    else
      _ ->
        :error
    end
  end
end
