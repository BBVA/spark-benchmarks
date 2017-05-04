lazy val sparkBenchmarks = project
  .in(file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(dfsio)

lazy val dfsio = project
  .enablePlugins(AutomateHeaderPlugin, AssemblyPlugin, BuildInfoPlugin)
  .settings(
    name := "spark-benchmarks-dfsio",
    Dependencies.Spark,
    Dependencies.Scopt,
    Dependencies.Alluxio
  )