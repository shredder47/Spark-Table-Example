name := "Spark Demos"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies := Seq("org.apache.spark" %% "spark-core" % "3.0.0" % Compile,
                           "org.apache.spark" %% "spark-sql" % "3.0.0" % Compile,
                           "org.apache.spark" %% "spark-hive" % "3.0.0" % Compile
)