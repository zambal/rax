defmodule Rax do
  @moduledoc """
  Documentation for `Rax`.
  """

  require Logger

  alias Rax.{Cluster, Machine, Request}

  defmodule TimeoutError do
    defexception [:message]
  end

  @spec call(Cluster.name(), Machine.cmd()) :: term()
  def call(cluster, cmd) do
    Request.new(cluster, &call/1, cmd)
    |> call()
  end

  defp call(req) do
    req |> do_call() |> handle_result()
  end

  defp do_call(%Request{leader_id: :undefined} = req) do
    {:ra.process_command(req.local_id, req.arg, req.timeout), req}
  end

  defp do_call(req) do
    {:ra.process_command(req.leader_id, req.arg, req.timeout), req}
  end

  @spec cast(Cluster.name(), Machine.cmd()) :: :ok
  def cast(cluster, cmd) do
    leader = :ra_leaderboard.lookup_leader(cluster)

    unless leader == :undefined do
      :ra.pipeline_command(leader, cmd)
    end

    :ok
  end

  @spec query(Cluster.name(), :ra.query_fun()) :: term()
  def query(cluster, query_fun) when is_function(query_fun, 1) do
    Request.new(cluster, &query/1, query_fun)
    |> query()
  end

  defp query(req) do
    req |> do_query() |> handle_result()
  end

  defp do_query(%Request{leader_id: :undefined} = req) do
    {:ra.consistent_query(req.local_id, req.arg, req.timeout), req}
  end

  defp do_query(req) do
    {:ra.consistent_query(req.leader_id, req.arg, req.timeout), req}
  end

  @spec local_query(Cluster.name(), :ra.query_fun()) :: term()
  def local_query(cluster, query_fun) when is_function(query_fun, 1) do
    {_idx_term, reply} =
      Request.new(cluster, &local_query/1, query_fun)
      |> local_query()

    reply
  end

  defp local_query(req) do
    req |> do_local_query() |> handle_result()
  end

  defp do_local_query(req) do
    {:ra.local_query(req.local_id, req.arg, req.timeout), req}
  end

  @spec members(Cluster.name(), timeout() | nil) ::
          :ra_server_proc.ra_leader_call_ret([:ra.server_id()])
  def members(cluster, timeout \\ nil) do
    req = Request.new(cluster, nil, nil, true)
    :ra.members(req.local_id, timeout || req.timeout)
  end

  defp handle_result({{:ok, reply, _lesder}, _req}) do
    reply
  end

  defp handle_result({{:error, error}, req}) do
    if req.retry_counter < req.retry_max do
      Process.sleep(req.retry_interval)

      req = %Request{
        req
        | retry_counter: req.retry_counter + 1,
          leader_id: :ra_leaderboard.lookup_leader(req.cluster_name)
      }

      req.retry_fun.(req)
    else
      if req.retry_max > 0 do
        Cluster.request_health_check(req.cluster_name)
      end

      fun_name = Function.info(req.retry_fun)[:name]
      exit({error, {__MODULE__, fun_name, [req.cluster_name, req.arg]}})
    end
  end

  defp handle_result({{:timeout, _server_id}, req}) do
    Cluster.request_health_check(req.cluster_name)
    raise TimeoutError, message: inspect(req)
  end
end
