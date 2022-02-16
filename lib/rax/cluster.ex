defmodule Rax.Cluster do
  @moduledoc false

  alias Rax.Cluster.Config
  require Logger

  @health_check_interval 3_000

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
      {:ok, cluster} ->
        name = {:via, Registry, {Rax.Cluster.Registry, cluster.name}}
        GenServer.start_link(__MODULE__, cluster, name: name)

      {:error, e} ->
        {:error, e}
    end
  end

  def init(cluster) do
    send_start_or_restart_server()
    {:ok, cluster}
  end

  def handle_call(:get_local_uid, _from, cluster) do
    {:reply, cluster.local_uid, cluster}
  end

  def handle_cast(:request_health_check, cluster) do
    if cluster.status != :health_check do
      if cluster.circuit_breaker do
        set_unavailable(cluster.name)

        Logger.warn(
          "Rax #{inspect(cluster.name)} cluster: circuit breaker activated, cluster unavailable until health check is finished"
        )
      end

      send_do_health_check(:now)
      Logger.info("Rax #{inspect(cluster.name)} cluster: health check started")
    end

    {:noreply, cluster}
  end

  def handle_info(:do_health_check, cluster) do
    cluster = check_health(cluster)

    if cluster.status == :ready do
      set_available(cluster.name)

      Logger.info(
        "Rax #{inspect(cluster.name)} cluster: health check finished, cluster available"
      )

      {:noreply, cluster}
    else
      send_do_health_check(:interval)
      {:noreply, cluster}
    end
  end

  def handle_info(:start_or_restart_server, cluster) do
    case start_or_restart_server(cluster) do
      {:ok, cluster} ->
        insert_info(cluster, false)
        send_do_health_check(:now)
        {:noreply, cluster}

      {:error, e} ->
        {:stop, e, cluster}
    end
  end

  def terminate(_reason, cluster) do
    if cluster.status != :new do
      :ra.stop_server(cluster.local_id)
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

  # Interaction with :ra

  defp start_or_restart_server(cluster) do
    case :ra.restart_server(cluster.local_id) do
      :ok ->
        Logger.info("Rax #{inspect(cluster.name)} #{node()}: Server restarted")
        {:ok, %Config{cluster | status: :restarted}}

      {:error, :name_not_registered} ->
        start_server(cluster)

      {:error, e} ->
        {:error, e}
    end
  end

  defp start_server(%Config{initial_member: m, local_id: m} = cluster) do
    ra_server_config = Config.to_ra_server_config(cluster)
    case :ra.start_cluster(:default, [ra_server_config]) do
      {:ok, _, _} ->
        Logger.info("Rax #{inspect(cluster.name)} #{node()}: Cluster started")
        {:ok, %Config{cluster | status: :started}}

      error ->
        error
    end
  end

  defp start_server(cluster) do
    ra_server_config = Config.to_ra_server_config(cluster)
    case :ra.start_server(ra_server_config) do
      :ok ->
        Logger.info("Rax #{inspect(cluster.name)} #{node()}: Server started")
        {:ok, %Config{cluster | status: :started}}

      error ->
        error
    end
  end

  defp check_health(cluster) do
    connected_nodes =
      connect_known_members(cluster)

    case pick_random_member(cluster, connected_nodes) do
      nil ->
        if cluster.status == :ready do
          %Config{cluster | status: :health_check}
        else
          cluster
        end

      server_to_call ->
        cluster |> evaluate_health() |> maybe_add_member(server_to_call, cluster.status == :started)
    end
  end

  defp evaluate_health(cluster) do
    log_line = "\r\n== Rax health check results for #{inspect(cluster.name)} at #{node()}==\r\n"

    case get_ra_server_overview(cluster) do
      %{state: status} = overview when status in [:leader, :follower] ->
        Logger.info(log_line <> "ra server overview: #{inspect overview}")
        case ping(cluster) do
          {:pong, leader} ->
            Logger.debug("#{inspect(leader)} leader")
            %Config{cluster | status: :ready}

          {:timeout, server_id} ->
            Logger.debug("#{inspect(server_id)} timeout")
            %Config{cluster | status: :health_check}

          {:error, e} ->
            Logger.debug("error: #{inspect(e)}")
            %Config{cluster | status: :health_check}
        end

      overview ->
        Logger.info(log_line <> "ra server overview: #{inspect overview}")
        %Config{cluster | status: :health_check}
    end
  end

  defp ping(%Config{name: name, local_id: from}) do
    leader_id = :ra_leaderboard.lookup_leader(name)

    if leader_id == :undefined do
      {:error, :leader_undefined}
    else
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

  defp maybe_add_member(%Config{local_id: local_id, initial_member: m} = cluster, server_id, true) when local_id != m do
    :ra.add_member(server_id, local_id)
    cluster
  end

  defp maybe_add_member(cluster, _server_id, _started?) do
    cluster
  end

  defp do_remove_member(%Config{name: name}, member) do
    leader_id = :ra_leaderboard.lookup_leader(name)

    if leader_id == :undefined do
      {:error, :leader_undefined}
    else
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
      if node != self do
        case Node.connect(node) do
          true ->
            [node | acc]

          _ ->
            acc
        end
      else
        acc
      end
    end)
  end

  defp get_ra_server_overview(%Config{name: name}) do
    case :ra.overview do
      %{servers: %{^name => status}} ->
        status

      _ ->
        nil
    end
  end

  defp pick_random_member(cluster, connected_nodes) do
    case connected_nodes |> Enum.filter(fn node -> {_, local_node} = cluster.local_id; node != local_node end) |> Enum.shuffle() do
      [] ->
        nil

      nodes ->
        n = hd(nodes)
        Enum.find(cluster.known_members, fn {_name, node} -> node == n end)
    end
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
