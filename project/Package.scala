import sbt._
import Keys._
import sbtassembly.AssemblyPlugin

object Package extends AutoPlugin {

  import sbtassembly.AssemblyPlugin.autoImport._

  override def trigger = allRequirements

  override def requires = AssemblyPlugin

  override lazy val projectSettings = Seq(
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyJarName in assembly := s"${name.value}-${version.value}-with-dependencies.jar",
    assemblyMergeStrategy in assembly := {
      case "application.conf" => MergeStrategy.concat
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

}