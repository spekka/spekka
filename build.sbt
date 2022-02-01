import Dependencies._

lazy val scalaVersions = List("2.12.15", "2.13.8")

ThisBuild / scalaVersion     := "2.13.8"
ThisBuild / organization     := "io.github.spekka"

ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.5.0"
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision


lazy val commonSettings = Seq(
  scalacOptions --= Seq(
    "-Xfatal-warnings"
  ),
  
  libraryDependencies := Seq(
    scalaTest % Test
  ),

  headerLicense := Some(HeaderLicense.ALv2("2022", "Andrea Zito")),

  addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full)
)

lazy val `spekka-docs` = project
  .enablePlugins(ParadoxPlugin)
  .settings(
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    headerLicense := Some(HeaderLicense.ALv2("2022", "Andrea Zito")),
    publish := false
  )

lazy val `spekka-test` = project
  .settings(commonSettings)
  .settings(
    publish := false
  )
  .settings(
    libraryDependencies ++= Seq(
      `akka-stream` % Test,
      `akka-stream-testkit` % Test
    ),
    crossScalaVersions := scalaVersions
  )

lazy val `spekka-stream` = project
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      `akka-stream` % "provided",
      `akka-stream-testkit` % Test
    ),
    crossScalaVersions := scalaVersions
  ).dependsOn(`spekka-test` % "test->test")

lazy val `spekka-benchmark` = project
  .settings(commonSettings)
  .settings(
    publish := false,
    libraryDependencies ++= Seq(
      `akka-stream`
    ),
    crossScalaVersions := scalaVersions
  )
  .dependsOn(`spekka-stream`)

lazy val spekka = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "spekka",
    publish := false,
    crossScalaVersions := Nil
  ).aggregate(
    `spekka-docs`,
    `spekka-test`,
    `spekka-stream`,
    `spekka-benchmark`
  )
