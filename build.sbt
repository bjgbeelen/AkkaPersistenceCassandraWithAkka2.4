organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.4-SNAPSHOT"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.10.4", "2.11.6")

resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"

fork in Test := true

javaOptions in Test += "-Xmx2500M"

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-feature",
  "-language:postfixOps",
  "-unchecked",
  "-deprecation",
  //"-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture"
)

Revolver.settings

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"             % "2.1.5",
  "com.typesafe.akka"      %% "akka-actor"                        % "2.4-SNAPSHOT",
  "com.typesafe.akka"      %% "akka-contrib"                      % "2.4-SNAPSHOT",
  "com.typesafe.akka"      %% "akka-persistence-experimental"     % "2.4-SNAPSHOT",
  "com.typesafe.akka"      %% "akka-testkit"                      % "2.4-SNAPSHOT",
  "org.scalatest"          %% "scalatest"                         % "2.1.4"   % "test",
  "org.cassandraunit"       % "cassandra-unit"                    % "2.0.2.2" % "test"
)
