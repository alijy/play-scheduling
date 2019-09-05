import play.core.PlayVersion
import sbt._

object AppDependencies {

  val compile: Seq[ModuleID] =
    Seq(
      "uk.gov.hmrc"       %% "mongo-lock" % "6.15.0-play-25",
      "com.typesafe.play" %% "play"       % PlayVersion.current % "provided"
    )

  val test: Seq[ModuleID] =
    Seq(
      //"org.scalatest"          %% "scalatest"          % "3.0.5"          % "test",
      "org.scalatest"          %% "scalatest"          % "2.2.6"          % "test",
      "org.pegdown"            % "pegdown"             % "1.6.0"          % "test",
      //"com.typesafe.play" %% "play-netty-server"       % "2.5.19" % "test",
      //"io.netty" %% "netty"       % "3.8.0.Final" % "test",
      //"ch.qos.logback"         % "logback-classic"     % "1.2.3"          % "test",
      "org.scalatestplus.play" %% "scalatestplus-play" % "2.0.1"          % "test",
      "uk.gov.hmrc"            %% "reactivemongo-test" % "4.15.0-play-25" % "test"
    )

}
