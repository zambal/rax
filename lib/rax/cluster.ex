defmodule Rax.Cluster do
  @moduledoc false

  alias Rax.Cluster.Config
  require Logger

  @health_check_interval 1_000
  @auto_snapshot_check_interval 2_000

  @type name :: atom()
  @type cluster_node :: node() | atom()
  @type status :: :new | :started | :health_check | :ready

  # API

  @spec request_health_check(Rax.name()) :: :ok
  def request_health_check(name) do
    name = {:via, Registry, {Rax.Cluster.Registry, name}}
    GenServer.cast(name, :request_health_check)
  end

  def request_snapshot(name) do
    Rax.call(name, {:"$rax_cmd", :request_snapshot, name})
  end

  @spec remove_member(Rax.name(), cluster_node()) :: :ok
  def remove_member(name, member) do
    name = {:via, Registry, {Rax.Cluster.Registry, name}}
    GenServer.call(name, {:remove_member, member})
  end

  def get_local_uid(name) do
    name = {:via, Registry, {Rax.Cluster.Registry, name}}
    GenServer.call(name, :get_local_uid)
  end

  # GenServer

  @spec child_spec(Config.opts()) :: Supervisor.child_spec()
  def child_spec(opts) do
    child_id = String.to_atom(to_string(opts[:name]) <> "_cluster_manager")

    %{
      id: child_id,
      start: {Rax.Cluster, :start_link, [opts]}
    }
  end

  @spec start_link(Config.opts()) :: GenServer.on_start() | {:error, Config.validation_error()}
  def start_link(opts) do
    case Config.new(opts) do
      {:ok, config} ->
        name = {:via, Registry, {Rax.Cluster.Registry, config.name}}
        GenServer.start_link(__MODULE__, config, name: name)

      {:error, e} ->
        {:error, e}
    end
  end

  def init(config) do
    send_start_or_restart_server()
    {:ok, config}
  end

  def handle_call(:get_local_uid, _from, config) do
    {:reply, config.local_uid, config}
  end

  def handle_call({:remove_member, member}, _from, config) do
    {:reply, do_remove_member(config, member), config}
  end

  def handle_cast(:request_health_check, config) do
    if config.status == :ready do
      if config.circuit_breaker do
        set_unavailable(config.name)

        Logger.warn(
          "Rax #{inspect(config.name)} cluster: circuit breaker activated, config unavailable until health check is finished"
        )
      end

      send_do_health_check(:now)
      Logger.info("Rax #{inspect(config.name)} cluster: health check started")
    end

    {:noreply, config}
  end

  def handle_info(:do_health_check, config) do
    config = check_health(config)

    if config.status == :ready do
      set_available(config.name)

      Logger.info("Rax #{inspect(config.name)} cluster: health check finished, cluster available")

      {:noreply, config}
    else
      send_do_health_check(:interval)
      {:noreply, config}
    end
  end

  def handle_info(:start_or_restart_server, config) do
    case start_or_restart_server(config) do
      {:ok, config} ->
        insert_info(config, false)
        send_do_health_check(:now)

        if config.auto_snapshot do
          send_auto_snapshot_check()
        end

        {:noreply, config}

      {:error, e} ->
        {:stop, e, config}
    end
  end

  def handle_info(:check_auto_snapshot, config) do
    {:noreply, check_auto_snapshot(config)}
  end

  def terminate(_reason, config) do
    if config.status != :new do
      :ra.stop_server(config.local_id)
    end

    :ok
  end

  defp send_start_or_restart_server() do
    send(self(), :start_or_restart_server)
  end

  defp send_do_health_check(timing) when timing in [:now, :interval] do
    case timing do
      :now ->
        send(self(), :do_health_check)

      :interval ->
        Process.send_after(self(), :do_health_check, @health_check_interval)
    end
  end

  defp send_auto_snapshot_check() do
    Process.send_after(self(), :check_auto_snapshot, @auto_snapshot_check_interval)
  end

  # Interaction with :ra

  defp start_or_restart_server(config) do
    case :ra.restart_server(config.local_id) do
      :ok ->
        Logger.info("Rax #{inspect(config.name)} #{node()}: Server restarted")
        {:ok, %Config{config | status: :restarted}}

      {:error, :name_not_registered} ->
        start_server(config)

      {:error, e} ->
        {:error, e}
    end
  end

  defp start_server(%Config{initial_member: m, local_id: m} = config) do
    ra_server_config = Config.to_ra_server_config(config)

    case :ra.start_cluster(:default, [ra_server_config]) do
      {:ok, _, _} ->
        Logger.info("Rax #{inspect(config.name)} #{node()}: Cluster started")
        {:ok, %Config{config | status: :started}}

      error ->
        error
    end
  end

  defp start_server(config) do
    ra_server_config = Config.to_ra_server_config(config)

    case :ra.start_server(ra_server_config) do
      :ok ->
        Logger.info("Rax #{inspect(config.name)} #{node()}: Server started")
        {:ok, %Config{config | status: :started}}

      error ->
        error
    end
  end

  defp check_auto_snapshot(config) do
    if config.auto_snapshot do
      send_auto_snapshot_check()
      ndx = get_last_index(config)

      with true <- config.status == :ready and leader?(config),
           true <- ndx > config.last_auto_snapshot_ndx + config.snapshot_interval do
        {:ok, new_ndx} = Rax.call(config.name, {:"$rax_cmd", :request_snapshot, config.name})
        %Config{config | last_auto_snapshot_ndx: new_ndx}
      else
        _ ->
          config
      end
    end
  end

  defp check_health(config) do
    config =
      if config.status == :ready do
        %Config{config | status: :health_check}
      else
        config
      end

    connected_nodes = connect_known_members(config)

    case pick_random_member(config, connected_nodes) do
      nil ->
        config

      server_to_call ->
        config |> evaluate_health() |> maybe_add_member(server_to_call)
    end
  end

  defp evaluate_health(config) do
    log_line = "\r\n== Rax health check results for #{inspect(config.name)} at #{node()}==\r\n"

    case get_ra_server_overview(config) do
      %{state: status} = overview when status in [:leader, :follower] ->
        Logger.info(log_line <> "ra server overview: #{inspect(overview)}")

        case ping(config) do
          {:pong, leader} ->
            Logger.debug("#{inspect(leader)} leader")
            %Config{config | status: :ready}

          {:timeout, server_id} ->
            Logger.debug("#{inspect(server_id)} timeout")
            config

          {:error, e} ->
            Logger.debug("error: #{inspect(e)}")
            config
        end

      overview ->
        Logger.info(log_line <> "ra server overview: #{inspect(overview)}")
        config
    end
  end

  defp ping(%Config{name: name, local_id: from}) do
    case :ra_leaderboard.lookup_leader(name) do
      :undefined ->
        {:error, :leader_undefined}

      leader_id ->
        do_ping(leader_id, from)
    end
  end

  defp do_ping(to, from) do
    case :ra.process_command(to, {:"$rax_cmd", :ping, from}) do
      {:ok, ^from, leader} ->
        {:pong, leader}

      error ->
        error
    end
  end

  defp maybe_add_member(
         %Config{status: :started, local_id: l, initial_member: m} = config,
         server_id
       ) when l != m do
    :ra.add_member(server_id, l)
    config
  end

  defp maybe_add_member(config, _server_id) do
    config
  end

  defp do_remove_member(%Config{name: name}, member) do
    case :ra_leaderboard.lookup_leader(name) do
      :undefined ->
        {:error, :leader_undefined}

      leader_id ->
        case :ra.remove_member(leader_id, member) do
          {:ok, res, _leader} ->
            {:ok, res}

          error ->
            error
        end
    end
  end

  defp connect_known_members(%Config{known_members: members}) do
    self = node()

    Enum.reduce(members, [], fn {_id, node}, acc ->
      case node != self and Node.connect(node) do
        true ->
          [node | acc]

        _ ->
          acc
      end
    end)
  end

  defp get_ra_server_overview(%Config{name: name}) do
    case :ra.overview() do
      %{servers: %{^name => status}} ->
        status

      _ ->
        nil
    end
  end

  defp leader?(%Config{name: name} = config) do
    case :ra_leaderboard.lookup_leader(name) do
      :undefined ->
        false

      leader_id ->
        config.local_id == leader_id
    end
  end

  defp pick_random_member(config, connected_nodes) do
    connected_nodes
    |> Enum.filter(fn node ->
      {_, local_node} = config.local_id
      node != local_node
    end)
    |> Enum.shuffle()
    |> case do
      [] ->
        nil

      nodes ->
        n = hd(nodes)
        Enum.find(config.known_members, fn {_name, node} -> node == n end)
    end
  end

  defp get_last_index(config) do
    :ra.aux_command(config.local_id, {:"$rax_cmd", :get_log_index})
  end

  # Ckuster info ETS table management

  @doc false
  def create_info_table do
    __MODULE__ = :ets.new(__MODULE__, [:named_table, :public, {:read_concurrency, true}])
    :ok
  end

  @doc false
  def delete_info_table do
    true = :ets.delete(__MODULE__)
    :ok
  end

  @doc false
  def lookup_info(name) do
    :ets.lookup(__MODULE__, name)
  end

  defp insert_info(%Config{} = c, available?) do
    true = :ets.insert(__MODULE__, {c.name, c.local_id, c.timeout, c.retry, available?})
    :ok
  end

  defp set_available(name) do
    case :ets.update_element(__MODULE__, name, {5, true}) do
      false ->
        raise ArgumentError,
          message: "Rax #{inspect(name)} cluster is not initialized on this node"

      true ->
        :ok
    end
  end

  defp set_unavailable(name) do
    case :ets.update_element(__MODULE__, name, {5, false}) do
      false ->
        raise ArgumentError,
          message: "Rax #{inspect(name)} cluster is not initialized on this node"

      true ->
        :ok
    end
  end
end
