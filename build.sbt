lazy val root = (project in file("."))
  .settings(
    organization  := "com.barclays",
    name := "barcs",
    version := "0.1",
    scalaVersion  := "2.11.6",
    resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.1.1",
      "org.apache.spark" %% "spark-sql" % "2.1.1",
      "com.typesafe" % "config" % "1.3.1"
    ),
    mainClass in assembly := Some("bar.ds.cs.three"),
    assemblyMergeStrategy in assembly := {
      case PathList("org", "apache", xs @ _*) => MergeStrategy.first
      case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.first
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.first
      case PathList("overview.html", xs @ _*) => MergeStrategy.first
      case x => {
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
      }
    }
  )