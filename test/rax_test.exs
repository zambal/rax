defmodule RaxTest do
  use ExUnit.Case
  doctest Rax

  test "greets the world" do
    assert Rax.hello() == :world
  end
end
