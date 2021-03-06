defmodule Rax.MixProject do
  use Mix.Project

  def project do
    [
      app: :rax,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      applications: [:ra],
      extra_applications: [:logger],
      mod: {Rax.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ra, "~> 1.1"}
    ]
  end
end
