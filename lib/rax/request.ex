defmodule Rax.Request do
  defstruct cluster_name: nil,
            arg: nil,
            local_id: nil,
            leader_id: :undefined,
            timeout: 5_000,
            retry_max: 0,
            retry_interval: 0,
            retry_counter: 0,
            retry_fun: nil

  def new(cluster_name, fun, arg, ignore_availability \\ false) do
    case Rax.Cluster.lookup_info(cluster_name) do
      [{_name, local, timeout, retry, av}] when av or ignore_availability ->
        leader = :ra_leaderboard.lookup_leader(cluster_name)
        {max, interval} = transform_retry(retry)

        %__MODULE__{
          cluster_name: cluster_name,
          arg: arg,
          local_id: local,
          leader_id: leader,
          timeout: timeout,
          retry_max: max,
          retry_interval: interval,
          retry_counter: 0,
          retry_fun: fun
        }

      [_] ->
        exit({:cluster_unavailable, {__MODULE__, Function.info(fun)[:name], [cluster_name, arg]}})

      [] ->
        exit({:cluster_not_started, {__MODULE__, Function.info(fun)[:name], [cluster_name, arg]}})
    end
  end

  defp transform_retry(false) do
  end

  defp transform_retry({_max, _interval} = retry) do
    retry
  end
end
