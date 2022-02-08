lazy val root = (project in file(".")).
  settings(
    name := "ParquetLoader",
    version := "1.0",
    scalaVersion := "2.11.8",
    target in assembly := file("bin/ParquetLoader/lib/"),
    assemblyJarName in assembly := "parquetLoader.jar",
    mainClass in Compile := Option("ua.com.lifecell.Main")
  )

lazy val sparkVersion  = "2.3.2"
lazy val hadoopVersion = "3.1.1"
lazy val circeVersion  = "0.9.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % sparkVersion  % "provided" excludeAll (ExclusionRule(organization = "javax.servlet")),
  "org.apache.spark" %% "spark-sql"     % sparkVersion  % "provided",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
  "org.apache.spark" %% "spark-hive"    % sparkVersion  % "provided",
  "io.circe"         %% "circe-core"    % circeVersion,
  "io.circe"         %% "circe-generic" % circeVersion,
  "io.circe"         %% "circe-parser"  % circeVersion
)

scalacOptions += "-Ylog-classpath"

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}
