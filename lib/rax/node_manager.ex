defmodule Rax.NodeManager do
  @moduledoc false

  alias Rax.Cluster
  require Logger

  @health_check_interval 1_000

  # API

  def start_local_server(cluster_name) do
    GenServer.call(:rax_node_manager, {:start_local_server, cluster_name})
  end

  @spec request_health_check(Rax.name()) :: :ok
  def request_health_check(cluster_name) do
    GenServer.cast(:rax_node_manager, {:request_health_check, cluster_name})
  end

  @spec fetch_cluster_info(Cluster.name()) :: {local, leader, timeout()} | :unavailable
        when local: :ra.server_id() | nil, leader: :ra.server_id() | nil
  def fetch_cluster_info(cluster, ignore_availability \\ false) do
    case :ets.lookup(:rax_cluster_info, cluster) do
      [{_name, local, leader, timeout, av}] when av or ignore_availability ->
        {local, leader, timeout}

      [{_name, _local, _leader, _timeout, false}] ->
        :unavailable

      [] ->
        raise ArgumentError, message: "Rax cluster #{inspect(cluster)} not found"
    end
  end

  @spec update_leader(Cluster.name(), :ra.server_id()) :: :ok
  def update_leader(cluster_name, server_id) do
    case :ets.update_element(:rax_cluster_info, cluster_name, {3, server_id}) do
      false ->
        raise ArgumentError, message: "Rax cluster #{inspect(cluster_name)} not found"

      true ->
        :ok
    end
  end

  # GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: :rax_node_manager)
  end

  def init([]) do
    create_cluster_info_table()

    Application.get_env(:rax, :clusters, [])
    |> Enum.reduce({:ok, %{}}, fn
      {name, opts}, {:ok, acc} ->
        with {:ok, cluster} <- Cluster.new(name, opts),
             {:ok, cluster} <- Cluster.start_local_server(cluster) do
          init_cluster_info(cluster)
          send(self(), {:do_health_check, name})
          {:ok, Map.put(acc, name, cluster)}
        else
          {:error, e} ->
            {:stop, e}
        end

      _, {:error, e} ->
        {:stop, e}
    end)
  end

  def handle_call({:start_local_server, cluster_name}, _from, clusters) do
    if cluster = Map.get(clusters, cluster_name) do
      case Cluster.start_local_server(cluster) do
        {:ok, cluster} ->
          init_cluster_info(cluster)
          send(self(), {:do_health_check, cluster_name})
          {:reply, :ok, Map.put(clusters, cluster_name, cluster)}

        {:error, e} ->
          {:reply, {:error, e}, clusters}
      end
    else
      {:reply, {:error, :no_cluster}, clusters}
    end
  end

  def handle_cast({:request_health_check, cluster_name}, clusters) do
    if cluster = Map.get(clusters, cluster_name) do
      if cluster.status != :health_check do
        if cluster.circuit_breaker do
          set_unavailable(cluster_name)

          Logger.warn(
            "Rax circuit breaker activated: #{inspect(cluster_name)} cluster unavailable until health check finished"
          )
        end

        send(self(), {:do_health_check, cluster_name})
        Logger.info("Rax health check started: #{inspect(cluster_name)} cluster")
      end
    end

    {:noreply, clusters}
  end

  def handle_info({:do_health_check, cluster_name}, clusters) do
    if cluster = Map.get(clusters, cluster_name) do
      cluster = Cluster.check_health(cluster)

      if cluster.status == :ready do
        case update_leader(cluster) do
          :ok ->
            set_available(cluster_name)
            Logger.info("Rax health check finished: #{inspect(cluster_name)} cluster available")

          :error ->
            Process.send_after(self(), {:do_health_check, cluster_name}, @health_check_interval)
        end
      else
        Process.send_after(self(), {:do_health_check, cluster_name}, @health_check_interval)
      end

      {:noreply, Map.put(clusters, cluster_name, cluster)}
    else
      {:noreply, clusters}
    end
  end

  def terminate(_reason, clusters) do
    delete_cluster_info_table()

    Enum.each(clusters, fn {_, cluster} ->
      :ra.stop_server(cluster.local_server_id)
    end)
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 2_000
    }
  end

  defp update_leader(%Cluster{} = cluster) do
    case Cluster.members(cluster) do
      {:ok, _, leader} ->
        case :ets.update_element(:rax_cluster_info, cluster.name, {3, leader}) do
          false ->
            raise ArgumentError, message: "Rax cluster #{inspect(cluster.name)} not found"

          true ->
            :ok
        end

      _ ->
        :error
    end
  end

  defp create_cluster_info_table do
    :ets.new(:rax_cluster_info, [:named_table, :public, {:read_concurrency, true}])
    :ok
  end

  defp delete_cluster_info_table do
    :ets.delete(:rax_cluster_info)
    :ok
  end

  defp init_cluster_info(%Cluster{
         name: name,
         local_server_id: server_id,
         timeout: timeout
       }) do
    :ets.insert(:rax_cluster_info, {name, server_id, nil, timeout, false})
    :ok
  end

  defp set_available(cluster_name) do
    case :ets.update_element(:rax_cluster_info, cluster_name, {5, true}) do
      false ->
        raise ArgumentError, message: "Rax cluster #{inspect(cluster_name)} not found"

      true ->
        :ok
    end
  end

  defp set_unavailable(cluster_name) do
    case :ets.update_element(:rax_cluster_info, cluster_name, {5, false}) do
      false ->
        raise ArgumentError, message: "Rax cluster #{inspect(cluster_name)} not found"

      true ->
        :ok
    end
  end
end
