defmodule Rax.ClusterManager do
  @moduledoc false

  alias Rax.Cluster
  require Logger

  @health_check_interval 1_000

  # API

  @spec request_health_check(Cluster.name()) :: :ok
  def request_health_check(cluster_name) do
    name = {:via, Registry, {Rax.ClusterManager.Registry, cluster_name}}
    GenServer.cast(name, :request_health_check)
  end

  @spec fetch_cluster_info(Cluster.name()) :: {local, leader, timeout()} | :unavailable
        when local: :ra.server_id() | nil, leader: :ra.server_id() | nil
  def fetch_cluster_info(cluster_name, ignore_availability \\ false) do
    case :ets.lookup(:rax_cluster_info, cluster_name) do
      [{_name, local, leader, timeout, av}] when av or ignore_availability ->
        {local, leader, timeout}

      [{_name, _local, _leader, _timeout, false}] ->
        :unavailable

      [] ->
        raise ArgumentError, message: "Rax cluster #{inspect(cluster_name)} not found"
    end
  end

  @spec update_leader(Cluster.name(), :ra.server_id()) :: :ok
  def update_leader(cluster_name, server_id) do
    if :ets.update_element(:rax_cluster_info, cluster_name, {3, server_id}) do
      :ok
    else
      raise ArgumentError,
        message: "Rax #{inspect(cluster_name)} cluster is not initialized on this node"
    end
  end

  @spec verify_local_leadership(Cluster.name()) :: :ok
  def verify_local_leadership(cluster_name) do
    name = {:via, Registry, {Rax.ClusterManager.Registry, cluster_name}}
    GenServer.cast(name, :verify_local_leadership)
  end

  # GenServer

  def start_link(%Cluster{} = cluster) do
    name = {:via, Registry, {Rax.ClusterManager.Registry, cluster.name}}
    GenServer.start_link(__MODULE__, cluster, name: name)
  end

  def init(cluster) do
    start_local_server()
    {:ok, cluster}
  end

  def handle_cast(:verify_local_leadership, %Cluster{local_server_id: local_server_id} = cluster) do
    case Cluster.members(cluster) do
      {:ok, members, ^local_server_id} ->
        Enum.each(members, fn {_, node} ->
          if Node.connect(node) == true do
            :erpc.cast(node, __MODULE__, :update_leader, [cluster.name, local_server_id])
          end
        end)

        {:noreply, cluster}

      _ ->
        {:noreply, cluster}
    end
  end

  def handle_cast(:request_health_check, cluster) do
    if cluster.status != :health_check do
      if cluster.circuit_breaker do
        set_unavailable(cluster.name)

        Logger.warn(
          "Rax #{inspect(cluster.name)} cluster: circuit breaker activated, cluster unavailable until health check is finished"
        )
      end

      do_health_check()
      Logger.info("Rax #{inspect(cluster.name)} cluster: health check started")
    end

    {:noreply, cluster}
  end

  def handle_info(:start_local_server, cluster) do
    case Cluster.start_local_server(cluster) do
      {:ok, cluster} ->
        insert_cluster_info(cluster)
        do_health_check()
        {:noreply, cluster}

      {:error, e} ->
        {:stop, e, cluster}
    end
  end

  def handle_info(:do_health_check, cluster) do
    cluster = Cluster.check_health(cluster)

    if cluster.status == :ready do
      case update_leader(cluster) do
        {:ok, cluster} ->
          set_available(cluster.name)

          Logger.info(
            "Rax #{inspect(cluster.name)} cluster: health check finished, cluster available"
          )

          {:noreply, cluster}

        :error ->
          do_health_check(:interval)
          {:noreply, cluster}
      end
    else
      do_health_check(:interval)
      {:noreply, cluster}
    end
  end

  def terminate(_reason, cluster) do
    if cluster.status != :new do
      :ra.stop_server(cluster.local_server_id)
    end
  end

  defp start_local_server() do
    send(self(), :start_local_server)
  end

  defp do_health_check(timing \\ :now) do
    case timing do
      :now ->
        send(self(), :do_health_check)

      :interval ->
        Process.send_after(self(), :do_health_check, @health_check_interval)
    end
  end

  defp update_leader(%Cluster{} = cluster) do
    case Cluster.members(cluster) do
      {:ok, _, leader} ->
        case :ets.update_element(:rax_cluster_info, cluster.name, {3, leader}) do
          false ->
            raise ArgumentError,
              message: "Rax #{inspect(cluster.name)} cluster is not initialized on this node"

          true ->
            {:ok, cluster}
        end

      _ ->
        :error
    end
  end

  defp insert_cluster_info(%Cluster{
         name: name,
         local_server_id: server_id,
         timeout: timeout
       }) do
    :ets.insert(:rax_cluster_info, {name, server_id, nil, timeout, false})
  end

  defp set_available(cluster_name) do
    case :ets.update_element(:rax_cluster_info, cluster_name, {5, true}) do
      false ->
        raise ArgumentError,
          message: "Rax #{inspect(cluster_name)} cluster is not initialized on this node"

      true ->
        :ok
    end
  end

  defp set_unavailable(cluster_name) do
    case :ets.update_element(:rax_cluster_info, cluster_name, {5, false}) do
      false ->
        raise ArgumentError,
          message: "Rax #{inspect(cluster_name)} cluster is not initialized on this node"

      true ->
        :ok
    end
  end
end
