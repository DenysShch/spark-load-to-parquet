lazy val root = (project in file(".")).
  settings(
    name := "load-to-hive-parq",
    version := "1.0",
    scalaVersion := "2.10.6",
    mainClass in Compile := Some("loadtohive.collectToParquet")        
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.1" % "provided" excludeAll (ExclusionRule(organization = "javax.servlet")), 
  "org.apache.spark" %% "spark-sql"  % "1.5.1" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "2.7.2" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.5.1" % "provided"
)

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}