name := "Wiki_Importer_Indexer"

version := "1.0"

scalaVersion := "2.11.7"

fork := true

parallelExecution in Test := false

//Resolvers

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"


// test dependencies


val testDependencies = Seq(
"org.scalatest" %% "scalatest" % "3.0.1" % "test",
"junit" % "junit" % "4.11" % "test"
)


// dependencies


val sparkCoreDep = "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"

val sparkSQLDep = "org.apache.spark" % "spark-sql_2.11" % "2.1.1" % "provided"

val unescapeDep = "org.unbescape" % "unbescape" % "1.1.4.RELEASE"

val sparkDataBricksDep = "com.databricks" % "spark-xml_2.11" % "0.4.1"

val mysqldriverDep = "mysql" % "mysql-connector-java" % "5.1.43"

val mongospark = "org.mongodb.spark" %% "mongo-spark-connector" % "2.0.0" 
  
val CommonsCliDep = "commons-cli" % "commons-cli" % "1.2"

val CassandraSparkDep = "datastax" % "spark-cassandra-connector" % "2.0.1-s_2.11" 

val dependencies = Seq(
	sparkCoreDep,
	sparkSQLDep,
	sparkDataBricksDep,
	unescapeDep,
	CommonsCliDep,
	CassandraSparkDep,
	mysqldriverDep,
	mongospark
).map(_.exclude("org.objectweb", "asm"))  

libraryDependencies ++= testDependencies

libraryDependencies ++= dependencies 

mainClass in assembly := Some("de.htw.ai.Wiki_Importer.SparkApp")

assemblyJarName in assembly := "wiki_importer_indexer.jar"
