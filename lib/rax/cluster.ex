defmodule Rax.Cluster do
  @moduledoc false

  alias Rax.Cluster.Config
  require Logger

  @health_check_interval 1_000

  # API

  @spec request_health_check(Rax.cluster_name()) :: :ok
  def request_health_check(cluster_name) do
    name = {:via, Registry, {Rax.Cluster.Registry, cluster_name}}
    GenServer.cast(name, :request_health_check)
  end

  def update_auto_snapshot(cluster_name, n) when n == false or is_integer(n) and n > 0 do
    Rax.call(cluster_name, {:"$rax_cmd", :update_auto_snapshot, n})
  end

  # GenServer

  @spec child_spec(Config.opts()) :: Supervisor.child_spec()
  def child_spec(opts) do
    child_id = String.to_atom(to_string(opts[:cluster_name]) <> "_cluster_manager")

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

  @spec init(Config.t()) :: {:ok, Config.t()}
  def init(cluster) do
    send_start_local_server()
    {:ok, cluster}
  end

  @spec handle_cast(:request_health_check, Config.t()) :: {:noreply, Config.t()}
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

  @spec handle_info(:do_health_check | :start_local_server, Config.t()) ::
          {:noreply, Config.t()} | {:stop, any, Config.t()}
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

  def handle_info(:start_local_server, cluster) do
    case start_local_server(cluster) do
      {:ok, cluster} ->
        insert_info(cluster, false)
        send_do_health_check(:now)
        {:noreply, cluster}

      {:error, e} ->
        {:stop, e, cluster}
    end
  end

  @spec terminate(any, Config.t()) :: :ok
  def terminate(_reason, cluster) do
    if cluster.status != :new do
      :ra.stop_server(cluster.local_server_id)
    end

    :ok
  end

  defp send_start_local_server() do
    send(self(), :start_local_server)
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

  defp start_local_server(cluster) do
    case :ra.restart_server(cluster.local_server_id) do
      :ok ->
        {:ok, %Config{cluster | status: :started}}

      {:error, _e} ->
        cluster
        |> Config.to_ra_server_config()
        |> :ra.start_server()
        |> case do
          :ok ->
            {:ok, %Config{cluster | status: :started}}

          error ->
            error
        end
    end
  end

  defp members(cluster) do
    :ra.members(cluster.local_server_id)
  end

  defp check_health(cluster) do
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
    log_line = "\r\n== Rax health check results for #{inspect(cluster.name)} ==\r\n"

    case members(cluster) do
      {:ok, members, leader} ->
        Logger.info(log_line <> "members: #{inspect(members)}\r\nleader: #{inspect(leader)}")
        %Config{cluster | status: :ready}

      {:timeout, server_id} ->
        Logger.info(log_line <> "timeout: #{inspect(server_id)}")
        %Config{cluster | status: :health_check}

      {:error, e} ->
        Logger.info(log_line <> "error: #{inspect(e)}")
        %Config{cluster | status: :health_check}
    end
  end

  defp maybe_trigger_election(%Config{status: :ready} = cluster, _server_id) do
    cluster
  end

  defp maybe_trigger_election(%Config{} = cluster, server_id) do
    :ok = :ra.trigger_election(server_id)
    cluster
  end

  defp maybe_add_member(%Config{status: :ready} = cluster, _server_id) do
    cluster
  end

  defp maybe_add_member(%Config{} = cluster, server_id) do
    case :ra.add_member(server_id, cluster.local_server_id) do
      {:ok, _, _} ->
        %Config{cluster | status: :ready}

      {:error, :already_member} ->
        %Config{cluster | status: :ready}

      _ ->
        cluster
    end
  end

  defp connect_initial_nodes(%Config{initial_members: members}) do
    Enum.each(members, fn {_id, node} -> Node.connect(node) end)
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
  def lookup_info(cluster_name) do
    :ets.lookup(__MODULE__, cluster_name)
  end

  defp insert_info(%Config{} = c, available?) do
    true = :ets.insert(__MODULE__, {c.name, c.local_server_id, c.timeout, c.retry, available?})
    :ok
  end

  defp set_available(cluster_name) do
    case :ets.update_element(__MODULE__, cluster_name, {5, true}) do
      false ->
        raise ArgumentError,
          message: "Rax #{inspect(cluster_name)} cluster is not initialized on this node"

      true ->
        :ok
    end
  end

  defp set_unavailable(cluster_name) do
    case :ets.update_element(__MODULE__, cluster_name, {5, false}) do
      false ->
        raise ArgumentError,
          message: "Rax #{inspect(cluster_name)} cluster is not initialized on this node"

      true ->
        :ok
    end
  end
end
