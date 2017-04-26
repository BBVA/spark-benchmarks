lazy val sparkBenchmarks = project
  .in(file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(alluxio)

lazy val alluxio = project
  .enablePlugins(AutomateHeaderPlugin, AssemblyPlugin)
  .settings(
    name := "spark-benchmarks-alluxio",
    Dependencies.Spark,
    Dependencies.Scopt,
    Dependencies.Alluxio
  )