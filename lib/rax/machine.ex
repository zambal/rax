defmodule Rax.Machine do
  @behaviour :ra_machine

  require Logger

  defmodule State do
    defstruct [:cluster_name, :machine, :machine_state, :auto_snapshot]

    @type t :: %State{
            cluster_name: Rax.cluster_name(),
            machine: module(),
            machine_state: term(),
            auto_snapshot: pos_integer() | false
          }
  end

  defmodule Apply do
    defstruct status: :cont, meta: nil, cmd: nil, state: nil, effects: [], reply: nil

    @type t :: %Apply{
            status: :cont | :done,
            meta: :ra_server.command_meta(),
            cmd: term(),
            state: Rax.Machine.State.t(),
            effects: :ra_machine.effects(),
            reply: term()
          }
  end

  def init(opts) do
    {mod, machine_opts} = opts.machine
    machine_state = mod.init(machine_opts)

    state = %State{
      cluster_name: opts.cluster_name,
      machine: mod,
      machine_state: machine_state,
      auto_snapshot: opts.auto_snapshot
    }

    state
  end

  def apply(meta, cmd, state) do
    %Apply{meta: meta, cmd: cmd, state: state}
    |> handle_pre_apply()
    |> handle_apply()
    |> handle_post_apply()
    |> make_apply_result()
  end

  # Handle auto_snapshot update command
  defp handle_pre_apply(%Apply{status: :cont, cmd: {:"$rax_cmd", :update_auto_snapshot, n}} = ctx) do
    %Apply{ctx | status: :done, state: Map.put(ctx.state, :auto_snapshot, n), reply: :ok}
  end

  # Handle ping health check command
  defp handle_pre_apply(%Apply{status: :cont, cmd: {:"$rax_cmd", :ping, from}} = ctx) do
    %Apply{ctx | status: :done, reply: from}
  end

  defp handle_pre_apply(ctx) do
    ctx
  end

  defp handle_apply(%Apply{status: :cont} = ctx) do
    case ctx.state.machine.apply(ctx.meta, ctx.cmd, ctx.state.machine_state) do
      {ms, reply} ->
        %Apply{ctx | status: :done, state: %State{ctx.state | machine_state: ms}, reply: reply}

      {ms, reply, meffects} ->
        %Apply{
          ctx
          | status: :done,
            state: %State{ctx.state | machine_state: ms},
            reply: reply,
            effects: meffects ++ ctx.effects
        }
    end
  end

  defp handle_apply(ctx) do
    ctx
  end

  # Handle auto_snapshot option
  defp handle_post_apply(%Apply{meta: %{index: ndx}, state: %{auto_snapshot: n} = state} = ctx)
       when is_integer(n) and n > 0 and ndx != 0 and rem(ndx, n) == 0 do
    Logger.info(
      "Rax autosnapshot requested for #{inspect(ctx.state.cluster_name)} cluster at index #{
        inspect(ndx)
      }"
    )

    %Apply{ctx | effects: [{:release_cursor, ndx, state} | ctx.effects]}
  end

  defp handle_post_apply(ctx) do
    ctx
  end

  defp make_apply_result(%Apply{state: state, reply: reply, effects: effects}) do
    {state, reply, effects}
  end

  @spec state_enter(any, atom | %{machine: atom, machine_state: any}) :: :ra_machine.effects()
  def state_enter(raft_state, state) do
    opt_effects_call(state.machine, :state_enter, [raft_state, state.machine_state], [])
  end

  def tick(time_ms, state) do
    Logger.warn("tick: #{inspect(time_ms)}")
    opt_effects_call(state.machine, :tick, [time_ms, state.machine_state])
  end

  def init_aux(name) do
    Logger.warn("init_aux: #{inspect(name)}")
  end

  defp opt_effects_call(mod, fun, args, effects \\ []) do
    if function_exported?(mod, fun, length(args)) do
      Kernel.apply(mod, fun, args) ++ effects
    else
      effects
    end
  end
end
