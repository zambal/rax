defmodule Rax.Application do
  @moduledoc false

  use Application

  require Logger

  def start(_type, _args) do
    case child_specs_from_config() do
      {:ok, cluster_managers} ->
        create_cluster_info_table()

        children = [
          {Registry, keys: :unique, name: Rax.ClusterManager.Registry},
          %{
            id: Rax.ClusterManager.Supervisor,
            start: {Supervisor, :start_link, [cluster_managers, [strategy: :one_for_one]]},
            type: :supervisor
          }
        ]

        opts = [strategy: :one_for_all, name: Rax.Supervisor]
        Supervisor.start_link(children, opts)

      {:error, e} ->
        {:error, e}
    end
  end

  def stop(_state) do
    delete_cluster_info_table()
  end

  defp child_specs_from_config() do
    Application.get_env(:rax, :clusters, [])
    |> Enum.reduce({:ok, []}, fn
      {name, opts}, {:ok, acc} ->
        case Rax.Cluster.new(name, opts) do
          {:ok, cluster} ->
            {:ok, [make_child_spec(cluster) | acc]}

          {:error, e} ->
            {:error, e}
        end

      _, {:error, e} ->
        {:error, e}
    end)
  end

  defp make_child_spec(cluster) do
    child_id = String.to_atom(to_string(cluster.name) <> "_cluster_manager")

    %{
      id: child_id,
      start: {Rax.ClusterManager, :start_link, [cluster]}
    }
  end

  defp create_cluster_info_table do
    :ets.new(:rax_cluster_info, [:named_table, :public, {:read_concurrency, true}])
  end

  defp delete_cluster_info_table do
    :ets.delete(:rax_cluster_info)
  end
end
