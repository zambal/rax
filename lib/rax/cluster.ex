defmodule Rax.Cluster do
  @moduledoc false

  require Logger

  alias __MODULE__

  defstruct name: nil,
            local_server_id: nil,
            local_server_uid: nil,
            initial_members: [],
            machine: nil,
            status: :new,
            timeout: 5_000,
            circuit_breaker: true,
            auto_snapshot: false

  @type name :: atom()
  @type node_name :: atom()
  @type machine :: {module(), map()}
  @type states :: :new | :started | :health_check | :ready

  @type t :: %Cluster{
          name: Rax.name(),
          local_server_id: :ra.server_id(),
          local_server_uid: String.t(),
          initial_members: [:ra.server_id()],
          machine: machine(),
          status: states(),
          timeout: timeout(),
          circuit_breaker: boolean()
        }

  @spec new(Rax.name(), Keyword.t()) ::
          {:ok, t()}
          | {:error, :invalid_name | :invalid_initial_members | :invalid_machine_config}
  def new(name, opts \\ [])

  def new(nil, _opts) do
    {:error, :invalid_name}
  end

  def new(x, _opts) when not is_atom(x) do
    {:error, :invalid_name}
  end

  def new(name, opts) do
    server_id = make_server_id(node(), name)

    initial_members =
      for n <- Keyword.get(opts, :initial_members, []) do
        make_server_id(n, name)
      end

    with [_ | _] <- initial_members,
         {:ok, machine} <- validate_machine(opts[:machine]),
         {:ok, timeout} <- validate_timeout(opts[:timeout]),
         {:ok, cb} <- validate_circuit_breaker(opts[:circuit_breaker]),
         {:ok, as} <- validate_auto_snapshot(opts[:auto_snapshot]) do
      cluster = %Cluster{
        name: name,
        local_server_id: server_id,
        local_server_uid: name |> to_string() |> :ra.new_uid(),
        initial_members: initial_members,
        machine: machine,
        timeout: timeout,
        circuit_breaker: cb,
        auto_snapshot: as
      }

      {:ok, cluster}
    else
      [] ->
        {:error, :invalid_initial_members}

      error ->
        error
    end
  end

  def start_local_server(cluster) do
    case :ra.restart_server(cluster.local_server_id) do
      :ok ->
        {:ok, %Cluster{cluster | status: :started}}

      {:error, _e} ->
        cluster
        |> to_ra_server_config()
        |> :ra.start_server()
        |> case do
          :ok ->
            {:ok, %Cluster{cluster | status: :started}}

          error ->
            error
        end
    end
  end

  @spec members(t()) ::
          {:ok, [:ra.server_id()], :ra.server_id()}
          | {:error, any}
          | {:timeout, :ra.server_id()}
  def members(cluster) do
    :ra.members(cluster.local_server_id)
  end

  @spec check_health(t()) :: t()
  def check_health(cluster) do
    connect_initial_nodes(cluster)
    server_id = cluster.local_server_id

    case Enum.sort(cluster.initial_members) |> hd() do
      ^server_id ->
        cluster
        |> evaluate_health()
        |> maybe_trigger_election(server_id)

      server_to_call ->
        cluster
        |> evaluate_health()
        |> maybe_add_member(server_to_call)
    end
  end

  defp evaluate_health(cluster) do
    case members(cluster) do
      {:ok, members, leader} ->
        Logger.info(
          "\r\n== Rax health check results for #{inspect(cluster.name)} ==\r\nmembers: #{
            inspect(members)
          }\r\nleader: #{inspect(leader)}"
        )

        %Cluster{cluster | status: :ready}

      {:timeout, server_id} ->
        Logger.info(
          "\r\n== Rax health check results for #{inspect(cluster.name)} ==\r\ntimeout: #{
            inspect(server_id)
          }"
        )

        %Cluster{cluster | status: :health_check}

      {:error, e} ->
        Logger.info(
          "\r\n== Rax health check results for #{inspect(cluster.name)} ==\r\nerror: #{inspect(e)}"
        )

        %Cluster{cluster | status: :health_check}
    end
  end

  defp maybe_trigger_election(%Cluster{status: :ready} = cluster, _server_id) do
    cluster
  end

  defp maybe_trigger_election(%Cluster{} = cluster, server_id) do
    :ok = :ra.trigger_election(server_id)
    cluster
  end

  defp maybe_add_member(%Cluster{status: :ready} = cluster, _server_id) do
    cluster
  end

  defp maybe_add_member(%Cluster{} = cluster, server_id) do
    case :ra.add_member(server_id, cluster.local_server_id) do
      {:ok, _, _} ->
        %Cluster{cluster | status: :ready}

      {:error, :already_member} ->
        %Cluster{cluster | status: :ready}

      _ ->
        cluster
    end
  end

  defp connect_initial_nodes(%Cluster{initial_members: members}) do
    Enum.each(members, fn {_id, node} -> Node.connect(node) end)
  end

  @spec to_ra_server_config(t()) :: map()
  def to_ra_server_config(%Cluster{} = cluster) do
    opts = %{
      cluster_name: cluster.name,
      machine: cluster.machine,
      auto_snapshot: cluster.auto_snapshot
    }

    %{
      cluster_name: cluster.name,
      id: cluster.local_server_id,
      uid: cluster.local_server_uid,
      initial_members: cluster.initial_members,
      machine: {:module, Rax.Machine, opts},
      log_init_args: %{uid: cluster.local_server_uid}
    }
  end

  defp validate_machine(nil) do
    {:error, :invalid_machine_config}
  end

  defp validate_machine(mod) when is_atom(mod) do
    {:ok, {mod, %{}}}
  end

  defp validate_machine({mod, opts}) when is_atom(mod) and is_map(opts) do
    {:ok, {mod, opts}}
  end

  defp validate_machine(_) do
    {:error, :invalid_machine_config}
  end

  defp validate_timeout(n) when is_integer(n) and n > 0 do
    {:ok, n}
  end

  defp validate_timeout(:infinity) do
    {:ok, :infinity}
  end

  defp validate_timeout(nil) do
    {:ok, 5_000}
  end

  defp validate_timeout(_) do
    {:error, :invalid_timeout}
  end

  defp validate_circuit_breaker(cb) when is_boolean(cb) do
    {:ok, cb}
  end

  defp validate_circuit_breaker(nil) do
    {:ok, true}
  end

  defp validate_circuit_breaker(_) do
    {:error, :invalid_circuit_breaker}
  end

  defp validate_auto_snapshot(n) when is_integer(n) do
    {:ok, n}
  end

  defp validate_auto_snapshot(a) when a in [false, nil] do
    {:ok, false}
  end

  defp validate_auto_snapshot(_) do
    {:error, :invalid_auto_snapshot}
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
