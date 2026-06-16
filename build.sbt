lazy val root = project
  .in(file("."))
  .aggregate(
    core,
    aws,
    awsDistroless,
    itAws
  )

lazy val core = project
  .in(file("modules/core"))
  .settings(moduleName := "snowplow-elasticsearch-loader-core")
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.coreDependencies)

lazy val aws: Project = project
  .in(file("modules/aws"))
  .settings(BuildSettings.awsSettings)
  .settings(libraryDependencies ++= Dependencies.awsDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val awsDistroless: Project = project
  .in(file("modules/distroless/aws"))
  .settings(BuildSettings.awsSettings)
  .settings(libraryDependencies ++= Dependencies.awsDependencies)
  .settings(sourceDirectory := (aws / sourceDirectory).value)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

lazy val itAws: Project = project
  .in(file("modules/it/aws"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.itDependencies)
  .settings(
    (Test / test) := (Test / test).dependsOn(awsDistroless / Docker / publishLocal).value,
    (Test / testOnly) := (Test / testOnly).dependsOn(awsDistroless / Docker / publishLocal).evaluated
  )
  .dependsOn(awsDistroless)

ThisBuild / fork := true
ThisBuild / dependencyOverrides ++= Dependencies.cveOverrides
