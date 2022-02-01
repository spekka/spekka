import Dependencies._

lazy val scalaVersions = List("2.12.15", "2.13.8")

ThisBuild / scalaVersion     := "2.13.8"
ThisBuild / organization     := "io.github.spekka"

ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.5.0"
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / versionScheme := Some("semver-spec")

inThisBuild(List(
  organization := "io.github.spekka",
  homepage := Some(url("https://github.com/spekka/spekka")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "nivox",
      "Andrea Zito",
      "zito.andrea@gmail.com",
      url("https://nivox.github.io")
    )
  )
))


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
  .enablePlugins(GitHubPagesPlugin)
  .settings(
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    gitHubPagesOrgName := "spekka",
    gitHubPagesRepoName := "spekka.github.io",
    gitHubPagesSiteDir := target.value / "paradox" / "site" / "main",
    gitHubPagesBranch := "master",
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
