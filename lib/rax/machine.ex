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

      # Handle :ping health check command
      def apply(%{index: current}, {:"$rax_cmd", :log_read, {from, ref, ndx}}, state) do
        if ndx >= current do
          {state, :eol}
        else
          {state, :ok, [{:log, [ndx], fn entry -> [{:send_msg, from, entry}] end}]}
        end

      end

      # Handle :request_snapshot command
      def apply(%{index: ndx}, {:"$rax_cmd", :request_snapshot, cluster_name}, state) do
        Logger.info(
          "snapshot requested for #{inspect(cluster_name)} cluster at index #{inspect(ndx)}"
        )

        {state, :ok, [{:release_cursor, ndx, state}]}
      end


      def tick(time_ms, state) do
        []
      end


      defoverridable(:ra_machine)
    end
  end
end
