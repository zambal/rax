defmodule Rax.Cluster.Config do
  @moduledoc false

  alias __MODULE__

  defstruct name: nil,
            local_id: nil,
            local_uid: nil,
            initial_members: [],
            machine: nil,
            status: :new,
            timeout: 5_000,
            circuit_breaker: false,
            auto_snapshot: false,
            snapshot_interval: 4096,
            retry: {0, 0}

  @type retry :: {non_neg_integer(), non_neg_integer()}

  @type option ::
          {:name, Rax.Cluster.name()}
          | {:initial_members, nonempty_list(Rax.Cluster.node())}
          | {:machine, module() | {module(), map()}}
          | {:timeout, non_neg_integer()}
          | {:circuit_breaker, boolean()}
          | {:auto_snapshot, pos_integer() | false}
          | {:snapshot_interval, pos_integer()}
          | {:retry, retry() | false}

  @type opts :: [option]

  @type validation_error ::
          :invalid_cluster_name
          | :invalid_initial_members
          | :invalid_machine
          | :invalid_timeout
          | :invalid_circuit_breaker
          | :invalid_auto_snapshot
          | :invalid_retry

  @type t :: %Config{
          name: Rax.Cluster.name(),
          local_id: :ra.server_id(),
          local_uid: String.t(),
          initial_members: [:ra.server_id()],
          machine: :ra_machine.machine(),
          status: Rax.Cluster.status(),
          timeout: timeout(),
          circuit_breaker: boolean(),
          auto_snapshot: pos_integer() | false,
          snapshot_interval: non_neg_integer(),
          retry: retry()
        }

  @spec new(Keyword.t()) :: {:ok, t()} | {:error, validation_error()}
  def new(opts) do
    {:ok, %Config{}}
    |> validate_cluster_name(opts)
    |> validate_initial_members(opts)
    |> validate_machine(opts)
    |> validate_timeout(opts)
    |> validate_circuit_breaker(opts)
    |> validate_auto_snapshot(opts)
    |> validate_snapshot_interval(opts)
    |> validate_retry(opts)
  end

  @spec to_ra_server_config(t()) :: map()
  def to_ra_server_config(%Config{} = cluster) do
    %{
      cluster_name: cluster.name,
      id: cluster.local_id,
      uid: cluster.local_uid,
      initial_members: cluster.initial_members,
      machine: cluster.machine,
      log_init_args: %{uid: cluster.local_uid, snapshot_interval: cluster.snapshot_interval}
    }
  end

  defp validate_cluster_name({:ok, cluster}, opts) do
    case opts[:name] do
      name when is_atom(name) and not is_nil(name) ->
        server_id = make_server_id(node(), name)
        uid = name |> to_string() |> :ra.new_uid()
        {:ok, %Config{cluster | name: name, local_id: server_id, local_uid: uid}}

      _ ->
        {:error, :invalid_cluster_name}
    end
  end

  defp validate_initial_members({:ok, cluster}, opts) do
    case opts[:initial_members] do
      members when is_list(members) and length(members) > 0 ->
        if Enum.all?(members, &is_atom/1) do
          server_ids = for m <- members, do: make_server_id(m, cluster.name)
          {:ok, %Config{cluster | initial_members: server_ids}}
        else
          {:error, :invalid_initial_members}
        end

      _ ->
        {:error, :invalid_initial_members}
    end
  end

  defp validate_initial_members({:error, e}, _opts) do
    {:error, e}
  end

  defp validate_machine({:ok, cluster}, opts) do
    case opts[:machine] do
      {mod, conf} when is_atom(mod) and is_map(conf) ->
        {:ok, %Config{cluster | machine: {:module, mod, conf}}}

      mod when not is_nil(mod) and is_atom(mod) ->
        {:ok, %Config{cluster | machine: {:module, mod, %{}}}}

      _ ->
        {:error, :invalid_machine_config}
    end
  end

  defp validate_machine({:error, e}, _opts) do
    {:error, e}
  end

  defp validate_timeout({:ok, cluster}, opts) do
    case opts[:timeout] do
      n when n == :infinity or (is_integer(n) and n > 0) ->
        {:ok, %Config{cluster | timeout: n}}

      nil ->
        {:ok, %Config{cluster | timeout: 5_000}}

      _ ->
        {:error, :invalid_timeout}
    end
  end

  defp validate_timeout({:error, e}, _opts) do
    {:error, e}
  end

  defp validate_circuit_breaker({:ok, cluster}, opts) do
    case opts[:circuit_breaker] do
      cb when is_boolean(cb) or is_nil(cb) ->
        {:ok, %Config{cluster | circuit_breaker: !!cb}}

      _ ->
        {:error, :invalid_circuit_breaker}
    end
  end

  defp validate_circuit_breaker({:error, e}, _opts) do
    {:error, e}
  end

  defp validate_auto_snapshot({:ok, cluster}, opts) do
    case opts[:auto_snapshot] do
      n when is_integer(n) and n > 0 ->
        {:ok, %Config{cluster | auto_snapshot: n}}

      b when b in [false, nil] ->
        {:ok, %Config{cluster | auto_snapshot: false}}

      _ ->
        {:error, :invalid_auto_snapshot}
    end
  end

  defp validate_auto_snapshot({:error, e}, _opts) do
    {:error, e}
  end

  defp validate_snapshot_interval({:ok, cluster}, opts) do
    case opts[:snapshot_interval] do
      n when is_integer(n) and n > 0 ->
        {:ok, %Config{cluster | snapshot_interval: n}}

      nil ->
        # default value from ra
        {:ok, %Config{cluster | snapshot_interval: 4096}}

      _ ->
        {:error, :invalid_snapshot_interval}
    end
  end

  defp validate_snapshot_interval({:error, e}, _opts) do
    {:error, e}
  end


  defp validate_retry({:ok, cluster}, opts) do
    case opts[:retry] do
      {n, i} when is_integer(n) and n >= 0 and is_integer(i) and i >= 0 ->
        {:ok, %Config{cluster | retry: {n, i}}}

      b when b in [false, nil] ->
        {:ok, %Config{cluster | retry: {0, 0}}}

      _ ->
        {:error, :invalid_retry}
    end
  end

  defp validate_retry({:error, e}, _opts) do
    {:error, e}
  end

  defp make_server_id(node, name) do
    str_node = Atom.to_string(node)

    case str_node |> String.split("@") do
      [_name, _host] ->
        {name, node}

      _ ->
        [_, host] = Atom.to_string(node()) |> String.split("@")
        {name, String.to_atom(str_node <> "@" <> host)}
    end
  end
end
