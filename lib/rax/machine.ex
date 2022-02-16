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



      defoverridable(:ra_machine)
    end
  end
end
