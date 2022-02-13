defmodule Rax.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    env_opts = Application.get_env(:rax, :env, [])
    {:ok, _} = :ra.start(env_opts)
    Rax.Cluster.create_info_table()

    children = [
      {Registry, keys: :unique, name: Rax.Cluster.Registry}
    ]

    opts = [strategy: :one_for_one, name: Rax.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def stop(_state) do
    Rax.Cluster.delete_info_table()
  end
end
