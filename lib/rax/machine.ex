defmodule Rax.Machine do

  @type cmd :: term()
  @type state :: term()

  defmacro __using__(_) do
    quote do
      @behaviour :ra_machine

      require Logger

      # Handle :ping health check command
      def apply(meta, {:"$rax_cmd", :ping, from}, state) do
        {state, from}
      end

      # Handle :request_snapshot command
      def apply(%{index: ndx}, {:"$rax_cmd", :request_snapshot, cluster_name}, state) do
        Logger.info(
          "snapshot requested for #{inspect(cluster_name)} cluster by #{node()} at index #{inspect(ndx)}"
        )

        {state, {:ok, ndx}, [{:release_cursor, ndx, state}]}
      end

      def handle_aux(_server_state, {:call, _from}, {:"$rax_cmd", :get_log_index}, aux_state, log_state, _mac_state) do
        {:reply, elem(log_state, Rax.Machine.last_index_field()), aux_state, log_state}
      end

      def handle_aux(_server_state, _type, _cmd, _aux_state, _log_state, _mac_state) do
        :undefined
      end

      defoverridable(:ra_machine)
    end
  end

  @ra_log_rec Record.extract(:ra_log, from: "deps/ra/src/ra_log.erl")
  @last_index_field Enum.find_index(@ra_log_rec, fn {k, _v} -> k == :last_index end) + 1

  @doc false
  def last_index_field, do: @last_index_field
end
