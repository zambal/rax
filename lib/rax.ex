defmodule Rax do
  @moduledoc """
  Documentation for `Rax`.
  """

  require Logger

  alias Rax.{Cluster, ClusterManager}

  defmodule TimeoutError do
    defexception [:message]
  end

  @spec call(Cluster.name(), term()) :: term()
  def call(cluster, cmd) do
    case ClusterManager.fetch_cluster_info(cluster) do
      :unavailable ->
        exit({:unavailable, {__MODULE__, :call, [cluster, cmd]}})

      {local, leader, timeout} ->
        do_call(cluster, cmd, local, leader, timeout)
    end
  end

  defp do_call(cluster, cmd, local, nil, timeout) do
    case :ra.process_command(local, cmd, timeout) do
      {:ok, reply, leader} ->
        ClusterManager.update_leader(cluster, leader)
        reply

      {:error, error} ->
        exit({error, {__MODULE__, :call, [cluster, cmd]}})

      {:timeout, _server_id} ->
        ClusterManager.request_health_check(cluster)
        raise TimeoutError, message: "Rax.call(#{inspect(cluster)}, #{inspect(cmd)})"
    end
  end

  defp do_call(cluster, cmd, local, leader, timeout) do
    case :ra.process_command(leader, cmd, timeout) do
      {:ok, reply, rleader} ->
        if rleader != leader, do: ClusterManager.update_leader(cluster, rleader)
        reply

      {:error, _error} ->
        Logger.debug(
          "Rax.call(#{inspect(cluster)}, #{inspect(cmd)}) to last known leader failed, trying local server"
        )

        do_call(cluster, cmd, local, nil, timeout)

      {:timeout, _server_id} ->
        ClusterManager.request_health_check(cluster)
        raise TimeoutError, message: "Rax.call(#{inspect(cluster)}, #{inspect(cmd)})"
    end
  end

  @spec cast(Cluster.name(), term()) :: :ok
  def cast(cluster, cmd) do
    {_, leader, _} = ClusterManager.fetch_cluster_info(cluster, true)

    unless is_nil(leader) do
      :ra.pipeline_command(leader, cmd)
    end

    :ok
  end

  @spec query(Cluster.name(), :ra.query_fun()) :: term()
  def query(cluster, query_fun) do
    case ClusterManager.fetch_cluster_info(cluster) do
      :unavailable ->
        exit({:unavailable, {__MODULE__, :query, [cluster, query_fun]}})

      {local, leader, timeout} ->
        do_query(cluster, query_fun, local, leader, timeout)
    end
  end

  defp do_query(cluster, query_fun, local, nil, timeout) do
    case :ra.consistent_query(local, query_fun, timeout) do
      {:ok, reply, leader} ->
        ClusterManager.update_leader(cluster, leader)
        reply

      {:error, error} ->
        exit({error, {__MODULE__, :query, [cluster, query_fun]}})

      {:timeout, _server_id} ->
        ClusterManager.request_health_check(cluster)
        raise TimeoutError, message: "Rax.query(#{inspect(cluster)}, #{inspect(query_fun)})"
    end
  end

  defp do_query(cluster, query_fun, local, leader, timeout) do
    case :ra.consistent_query(leader, query_fun, timeout) do
      {:ok, reply, rleader} ->
        if rleader != leader, do: ClusterManager.update_leader(cluster, rleader)
        reply

      {:error, _error} ->
        Logger.debug(
          "Rax.query(#{inspect(cluster)}, #{inspect(query_fun)}) redirected to local server"
        )

        do_query(cluster, query_fun, local, nil, timeout)

      {:timeout, _server_id} ->
        ClusterManager.request_health_check(cluster)
        raise TimeoutError, message: "Rax.query(#{inspect(cluster)}, #{inspect(query_fun)})"
    end
  end

  @spec local_query(Cluster.name(), :ra.query_fun()) :: term()
  def local_query(cluster, query_fun) do
    case ClusterManager.fetch_cluster_info(cluster) do
      :unavailable ->
        exit({:unavailable, {__MODULE__, :query, [cluster, query_fun]}})

      {local, _leader, timeout} ->
        case :ra.local_query(local, query_fun, timeout) do
          {:ok, {_idx_term, reply}, _leader} ->
            reply

          {:error, error} ->
            exit({error, {__MODULE__, :local_query, [cluster, query_fun]}})

          {:timeout, _server_id} ->
            Rax.ClusterManager.request_health_check(cluster)

            raise TimeoutError,
              message: "Rax.local_query(#{inspect(cluster)}, #{inspect(query_fun)})"
        end
    end
  end

  @spec members(Cluster.name(), timeout() | nil) ::
          {:ok, members, leader} | {:error, any} | {:timeout, :ra.server_id()}
        when members: [:ra.server_id()], leader: :ra.server_id()
  def members(cluster, timeout \\ nil) do
    {local, _, def_timeout} = ClusterManager.fetch_cluster_info(cluster, true)
    :ra.members(local, timeout || def_timeout)
  end
end
