defmodule Rax.MixProject do
  use Mix.Project

  def project do
    [
      app: :rax,
      version: "0.4.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      applications: [:ra],
      extra_applications: [:logger],
      mod: {Rax.Application, []}
    ]
  end

  defp deps do
    [
      {:ra, "~> 2.0"}
    ]
  end
end
