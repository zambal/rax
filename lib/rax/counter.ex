defmodule Rax.Counter do
  @behaviour :ra_machine

  # API

  @spec inc(Rax.cluster_name()) :: integer()
  def inc(cluster) do
    Rax.call(cluster, :inc)
  end

  @spec dec(Rax.cluster_name()) :: integer()
  def dec(cluster) do
    Rax.call(cluster, :dec)
  end

  @spec set(Rax.cluster_name(), integer()) :: integer()
  def set(cluster, n) when is_integer(n) do
    Rax.call(cluster, {:set, n})
  end

  @spec reset(Rax.cluster_name()) :: :ok
  def reset(cluster) do
    Rax.cast(cluster, :reset)
  end

  @spec fetch(Rax.cluster_name()) :: integer()
  def fetch(cluster) do
    Rax.query(cluster, & &1)
  end

  # State machine

  @doc false
  def init(_config) do
    0
  end

  @doc false
  def apply(_meta, :inc, state) do
    {state + 1, state}
  end

  def apply(_meta, :dec, state) do
    {state - 1, state}
  end

  def apply(_meta, {:set, n}, state) do
    {n, state}
  end

  def apply(_meta, :reset, _state) do
    {0, :ok}
  end
end
