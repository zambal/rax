defmodule Rax do
  @moduledoc """
  Documentation for `Rax`.
  """

  require Logger

  alias Rax.{Cluster, Request}

  @type cluster_name :: atom()
  @type node_name :: atom()
  @type machine :: {module(), map()}
  @type cluster_states :: :new | :started | :health_check | :ready

  defmodule TimeoutError do
    defexception [:message]
  end

  @spec call(cluster_name(), term()) :: term()
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

  @spec cast(cluster_name(), term()) :: :ok
  def cast(cluster, cmd) do
    leader = :ra_leaderboard.lookup_leader(cluster)

    unless leader == :undefined do
      :ra.pipeline_command(leader, cmd)
    end

    :ok
  end

  @spec query(cluster_name(), :ra.query_fun()) :: term()
  def query(cluster, query_fun) when is_function(query_fun, 1) do
    Request.new(cluster, &query/1, fn s -> query_fun.(s.machine_state) end)
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

  @spec local_query(cluster_name(), :ra.query_fun()) :: term()
  def local_query(cluster, query_fun) when is_function(query_fun, 1) do
    {_idx_term, reply} =
      Request.new(cluster, &local_query/1, fn s -> query_fun.(s.machine_state) end)
      |> local_query()

    reply
  end

  defp local_query(req) do
    req |> do_local_query() |> handle_result()
  end

  defp do_local_query(req) do
    {:ra.local_query(req.local_id, req.arg, req.timeout), req}
  end

  @spec members(cluster_name(), timeout() | nil) ::
          {:ok, members, leader} | {:error, any} | {:timeout, :ra.server_id()}
        when members: [:ra.server_id()], leader: :ra.server_id()
  def members(cluster, timeout \\ nil) do
    req = Request.new(cluster, nil, nil, true)
    :ra.members(req.local_id, timeout || req.timeout)
  end

  defp handle_result({{:ok, reply, _lesder}, _req}) do
    reply
  end

  defp handle_result({{:error, error}, req}) do
    cond do
      req.retry_counter < req.retry_max - 1 ->
        Process.sleep(req.retry_interval)

        req = %Request{
          req
          | retry_counter: req.retry_counter + 1,
            leader_id: :ra_leaderboard.lookup_leader(req.cluster_name)
        }

        req.retry_fun.(req)

      req.retry_counter == req.retry_max - 1 ->
        req = %Request{
          req
          | retry_counter: req.retry_counter + 1,
            leader_id: nil
        }

        req.retry_fun.(req)

      true ->
        fun_name = Function.info(req.retry_fun)[:name]
        exit({error, {__MODULE__, fun_name, [req.cluster_name, req.arg]}})
    end
  end

  defp handle_result({{:timeout, _server_id}, req}) do
    Cluster.request_health_check(req.cluster_name)
    raise TimeoutError, message: inspect(req)
  end
end
