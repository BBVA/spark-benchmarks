import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import de.heikoseeberger.sbtheader._
import de.heikoseeberger.sbtheader.license.Apache2_0
import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin

object Common extends AutoPlugin {

  override def trigger = allRequirements

  override def requires = JvmPlugin && AutomateHeaderPlugin

  override lazy val projectSettings =
    Dependencies.Common ++ Seq(
      organization := "com.bbva.spark",
      organizationName := "BBVA",
      version := "0.1.0",
      organizationHomepage := Some(url("http://bbva.com")),
      //scmInfo := Some(ScmInfo(url("https://github.com/bbva/spark-benchmarks"), "git@github.com:bbva/spark-benchmarks.git")),
      //developers += Developer(),

      licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),

      scalaVersion := Dependencies.ScalaVersion,

      scalacOptions ++= Seq(
        "-encoding", "UTF-8",
        "-feature",
        "-unchecked",
        "-deprecation",
        //"-Xfatal-warnings",
        "-Xlint",
        "-Yno-adapted-args",
        "-Ywarn-dead-code",
        "-Xfuture"
      ),

      javacOptions ++= Seq(
        "-Xlint:unchecked"
      ),

      //autoAPIMappings := true,
      //apiURL := Some(url(s"http://developer.bbva.com/docs/api/spark-benchmarks/${version.value}")),

      headers := headers.value ++ Map(
        "scala" -> Apache2_0("2017", "BBVA"),
        "java" -> Apache2_0("2017", "BBVA"),
        "conf" -> Apache2_0("2017", "BBVA", "#")
      )
    )

}