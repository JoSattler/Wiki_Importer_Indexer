

package de.htw.ai.Wiki_Importer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark
import org.apache.commons.cli._
import org.apache.spark.api.java.JavaRDD.toRDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.util.Calendar
import org.apache.spark.sql.functions._

/*Created by Jörn Sattler for Projekt: "Wikiplag"
 *
 * 25.07.2017
 * 
 *Class which uses the WikidumpParser.scala and the InverseIndexBuilderImp.scala
 * to parse the wikipedia.xml dump and write its content to
 * a MySQLDB, a MongoDB, and a CassandraDB. Also builds an inverse Index
 * structure and writes this structure to the databases as well.
 */
object SparkApp {

  private def createCLiOptions() = {
    val options = new Options()

    /* CLi Options*/

    OptionBuilder.withLongOpt("db_host")
    OptionBuilder.withDescription("Database Host")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("host")
    options.addOption(OptionBuilder.create("dh"))

    OptionBuilder.withLongOpt("db_port")
    OptionBuilder.withDescription("Database Port")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[Number])
    OptionBuilder.withArgName("port")
    options.addOption(OptionBuilder.create("dp"))

    OptionBuilder.withLongOpt("db_user")
    OptionBuilder.withDescription("Database User")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("user")
    options.addOption(OptionBuilder.create("du"))

    OptionBuilder.withLongOpt("db_password")
    OptionBuilder.withDescription("Database Password")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("password")
    options.addOption(OptionBuilder.create("dpw"))

    OptionBuilder.withLongOpt("db_name")
    OptionBuilder.withDescription("Database Name")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("database")
    options.addOption(OptionBuilder.create("dn"))

    OptionBuilder.withLongOpt("db_type")
    OptionBuilder.withDescription("Database Type: MySQL = mysql, MongoDB = mong, Cassandra = cass")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("type")
    options.addOption(OptionBuilder.create("dt"))

    OptionBuilder.withLongOpt("db_wtable")
    OptionBuilder.withDescription("Wiki Table Name")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("tablew")
    options.addOption(OptionBuilder.create("wt"))

    OptionBuilder.withLongOpt("db_itable")
    OptionBuilder.withDescription("inv Index Table Name")
    OptionBuilder.isRequired
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("tablei")
    options.addOption(OptionBuilder.create("it"))

    OptionBuilder.withLongOpt("help")
    OptionBuilder.hasArg(false)
    options.addOption(OptionBuilder.create("h"))

    /* Commands */

    val group = new OptionGroup()
    group.setRequired(false)

    OptionBuilder.withLongOpt("extract_Text")
    OptionBuilder.withDescription("Path to the XML-File containing the Wiki-Articles")
    OptionBuilder.hasArgs(1)
    OptionBuilder.withType(classOf[String])
    OptionBuilder.withArgName("wiki_file")
    group.addOption(OptionBuilder.create("e"))

    OptionBuilder.withLongOpt("index")
    OptionBuilder.withDescription("use db-entries to create an inverse index and stores it back")
    OptionBuilder.hasArgs(0)
    group.addOption(OptionBuilder.create("i"))

    OptionBuilder.withLongOpt("b_test")
    OptionBuilder.withDescription("benchmark test")
    OptionBuilder.hasArgs(0)
    options.addOption(OptionBuilder.create("bt"))

    options.addOptionGroup(group)
    options
  }

  private def printHelp(options: Options) = {
    new HelpFormatter().printHelp(115, "wiki_importer_indexer.jar", "\nOptions are:", options, "\n BA Jörn Sattler", true)
  }

  def main(args: Array[String]) {
    val options = createCLiOptions()

    try {
      val commandLine = new GnuParser().parse(options, args)
      val dbHost = commandLine.getParsedOptionValue("db_host").asInstanceOf[String]
      val dbPort = commandLine.getParsedOptionValue("db_port").asInstanceOf[Number].intValue()
      val dbUser = commandLine.getParsedOptionValue("db_user").asInstanceOf[String]
      val dbPass = commandLine.getParsedOptionValue("db_password").asInstanceOf[String]
      val dbName = commandLine.getParsedOptionValue("db_name").asInstanceOf[String]
      val dbType = commandLine.getParsedOptionValue("db_type").asInstanceOf[String]
      val dbWTab = commandLine.getParsedOptionValue("db_wtable").asInstanceOf[String]
      val dbITab = commandLine.getParsedOptionValue("db_itable").asInstanceOf[String]

      if (commandLine.hasOption("h")) {
        printHelp(options)
        return
      }
      if (commandLine.hasOption("e")) {
        val file = commandLine.getParsedOptionValue("e").asInstanceOf[String]
        if (dbType == "cass") {
          extractTextCassandra(file, dbHost, dbPort, dbUser, dbPass, dbName, dbWTab)
        }
        if (dbType == "mong") {
          extractTextMongo(file, dbHost, dbPort, dbUser, dbPass, dbName, dbWTab)
        }
        if (dbType == "mysql") {
          extractTextMySQL(file, dbHost, dbPort, dbUser, dbPass, dbName, dbWTab)
        }
      } else if (commandLine.hasOption("i")) {
        if (dbType == "cass") {
          createInverseIndexCass(dbHost, dbPort, dbUser, dbPass, dbName, dbWTab, dbITab)
        }
        if (dbType == "mong") {
          createInverseIndexMongo(dbHost, dbPort, dbUser, dbPass, dbName, dbWTab, dbITab)
        }
        if (dbType == "mysql") {
          createInverseIndexMySQL(dbHost, dbPort, dbUser, dbPass, dbName, dbWTab, dbITab)
        }
      } else if (commandLine.hasOption("bt")) {
        if (dbType == "cass") {
          Benchmarktest.testCassandra(dbHost, dbPort, dbUser, dbPass, dbName, dbWTab, dbITab)
        }
        if (dbType == "mong") {
          Benchmarktest.testMongo(dbHost, dbPort, dbUser, dbPass, dbName, dbWTab, dbITab)
        }
        if (dbType == "mysql") {
          Benchmarktest.testMySQl(dbHost, dbPort, dbUser, dbPass, dbName, dbWTab, dbITab)
        }
      }
    } catch {
      case e: ParseException =>
        println("Unexpected ParseException: " + e.getMessage)
        printHelp(options)
      case e: Exception =>
        e.printStackTrace()
        printHelp(options)
    }

  }

  /*
   * Method that parses the wikidump and writes the articles in a cassandra dataspace.
   * Method for parsing the given Wikipedia-XML-Dump and wiritng its contents in a Cassandra Database. 
   * 
   * @param wikidump Filepath to the Wikipedia-XML Dump
   * @param cassandraPort Portnumber required to access Cassandra
   * @param cassandraDatabase IP required to access Cassandra
   * @param cassandraUser Cassandra Database User
   * @param cassandraPW Cassandra Database Password
   * @param  cassandraKeyspace Cassandra Keyspace (analog with MySQL Database) name 
   *  
   * If the given Cassandra Column Family doens't exist it will try to create it, note that this isnt optimal in cassandra.
   * Tables should be ALWAYS created beforehand to ensure efficient reads. If no table is created beforehand regardless spark will create one and
   * sample it's structure from the given Dataframe
   * 
   * 
   *  docID (Wikipedia documentID) [BIGINT]
   *  title (Wikipedia article title) [TEXT]
   *	wikitext (Wikipedia article text) [TEXT]
   * 
   *
   */
  private def extractTextCassandra(wikidump: String, cassandraHost: String, cassandraPort: Int, cassandraUser: String, cassandraPW: String, cassandraKeyspace: String, cassandraTables: String) {
    println("Import Wiki-Articles Cassandra")
    println("Starting Import")
    val t0 = System.nanoTime()
    val sparkConf = new SparkConf(true).setAppName("WikiImporter_Cassandra")
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.port", cassandraPort.toString())
      .set("spark.cassandra.auth.username", cassandraUser)
      .set("spark.cassandra.auth.password", cassandraPW)

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load(wikidump)

    df.filter("ns = 0")
      .select("id", "title", "revision.text")
      .rdd.map(X => (X.getLong(0), X.getString(1), WikiDumpParser.parseXMLWikiPage(X.getStruct(2).getString(0))))
      .saveToCassandra(cassandraKeyspace, cassandraTables, SomeColumns("docid", "title", "wikitext"))
    println("Import Complete")
    val t1 = System.nanoTime()
    println((t1 - t0) / 1e9d)
    sc.stop()
  }

  /*
   * Method that parses the wikidump and writes the articles in a MySQL dataspace.
   * Method for parsing the given Wikipedia-XML-Dump and wiritng its contents in a MySQL Database. 
   * 
   * @param wikidump Filepath to the Wikipedia-XML Dump
   * @param mySQLDBPath IP required to access the MySQL Database
   * @param mySQLDBPort Portnumber required to access the MySQL Database
   * @param mySQLDBUser MySQL Database User
   * @param mySQLDBPW MySQL Database Password
   * @param mySQLDatabase MySQL Database name
   * @param mySQLTables MySQL Table Name where the Wikipedia-articles are written to
   *  
   *
   * If the given mySQLTable doesn't exist a table with the given name will be created. The Schema of the table will be 
   * sampled from the spark.dataframes schema. 
   * In This case that means a table with the following collumns will be created:
   *  docID (Wikipedia documentID) [BIGINT]
   *  title (Wikipedia article title) [TEXT]
   *	wikitext (Wikipedia article text) [TEXT]
   * 
   * Note: 
   * 1. If a table with the matching name is created beforehand (recommended) different MySQL DATATYPES can be used, since Spark 
   * always seem to map Strings regardless of length to TEXT this could be a good thing to improve performance (e.g. use VARCHAR(x). 
   * 2. Spark will create a table without PRIMARY KEYs or INDEXes (if no table is created beforehand)
   */
  private def extractTextMySQL(wikidump: String, mySQLDBHost: String, mySQLDBPort: Int, mySQLDBUser: String, mySQLDBPW: String, mySQLDBDatabase: String, mySQLTables: String) {
    println("Import Wiki-Articles MySQL")
    println("Starting Import")

    val mySQLClient = s"jdbc:mysql://${mySQLDBHost}:${mySQLDBPort}/${mySQLDBDatabase}"
    val sparkConf = new SparkConf(true).setAppName("WikiImporter_MySQL")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    //Load Wikipedia XML Dump
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load(wikidump)
    println("parsingfile complete")
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", mySQLDBUser)
    prop.setProperty("password", mySQLDBPW)
    //Parse Dump for docID, title, articletex
    val wikirdd = df
      .filter("ns = 0")
      .select("id", "title", "revision.text")
      .rdd.map(X => (X.getLong(0), X.getString(1), WikiDumpParser.parseXMLWikiPage(X.getStruct(2).getString(0))))
    //Wirte into existing table with mode "append" see: http://bigdatums.net/2016/10/16/writing-to-a-database-from-spark/
    sqlContext.createDataFrame(wikirdd).toDF("docID", "title", "wikitext").write.mode("append").jdbc(mySQLClient, mySQLTables, prop)
    println("Import Complete")
    sc.stop()
  }

  /*
   * Method that parses the wikidump and writes the articles in a MongoDB.
   * Method for parsing the given Wikipedia-XML-Dump and writing its contents in a MongoDB. 
   * 
   * @param wikidump Filepath to the Wikipedia-XML Dump
   * @param mongoDBPath IP required to access the MySQL Database
   * @param mongoDBPort Portnumber required to access the MongoDB
   * @param mongoDBUser MongoDB Database User
   * @param mongoDBPW MongoDB Database Password
   * @param mongoDBDatabase MongoDB name
   * @param mongoDBCollection MongoDB Collection name where the Wikipedia-Articles are written to
   *
   * If the given Collection doesn't exist a Collection with the given name will be created. The Schema of the Collection will be 
   * sampled from the spark.dataframes schema. 
   * In This case that means a table with the following fields will be created:
   *  docID (Wikipedia documentID) [NumberLong]
   *  title (Wikipedia article title) [String]
   *	wikitext (Wikipedia article text) [String]
   * 
   * Note: 
   * 1. If a Collection with the matching name is created beforehand (not recommended) different MongoDB DATATYPES can be used, since Spark samples the
   * Collections Schema from the Dataframe
   * 2. Spark will create a coelletion without PRIMARY KEYs or INDEXes if you want those on your collections alter it afterwards
   * 3. In this case since we use the Wikipedia-DocumentID (docID) as "_id" field in MongoDB it will have an INDEX 
   */

  private def extractTextMongo(wikidump: String, mongoDBHost: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String, mongoDBDatabase: String, mongoDBCollection: String) {
    println("Import Wiki-Articles MongoDB")
    println("Starting Import")
    val t0 = System.nanoTime()
    val mongoClient = s"mongodb://${mongoDBUser}:${mongoDBPW}@${mongoDBHost}:${mongoDBPort}/${mongoDBDatabase}"
    val sc = SparkSession.builder().appName("WikiImporter_MongoDB")
      .config("spark.mongodb.input.uri", s"${mongoClient}.${mongoDBCollection}")
      .config("spark.mongodb.output.uri", s"${mongoClient}.${mongoDBCollection}")
      .getOrCreate()

    val sqlContext = new SQLContext(sc.sparkContext)
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load(wikidump)
      .filter("ns = 0")
      .select("id", "title", "revision.text")

    val wikirdd =
      df.rdd.map(X => (X.getLong(0), X.getString(1), WikiDumpParser.parseXMLWikiPage(X.getStruct(2).getString(0))))
    MongoSpark.save(sqlContext.createDataFrame(wikirdd).toDF("_id", "title", "wikitext").write.mode("append"))
    println("Import Complete")
    val t1 = System.nanoTime()
    println((t1 - t0) / 1e9d)
    sc.stop()

  }

  /*
   * Method that gets the wiki articles from the Wikipedia Articles Cassandra Column 
   * Family and then builds an inverse index for them and stores them in a new Column Family in a new Column Family 
   * in the Cassandra Database
   * 
   * @param cassandraPort Portnumber required to access Cassandra
   * @param cassandraDatabase IP required to access Cassandra
   * @param cassandraUser Cassandra Database User
   * @param cassandraPW Cassandra Database Password
   * @param cassandraKeyspace Cassandra Keyspace (analog with MySQL Database) name 
   * @param cassandraWikiTables Name of the Cassandra Column Family containing the Wikipedia-Articles
   * @param cassandraInvIndexTables Name of the Cassandra Column Family containing the inverse Indezes
   * 
   */
  private def createInverseIndexCass(cassandraHost: String, cassandraPort: Int, cassandraUser: String, cassandraPW: String, cassandraKeyspace: String, cassandraWikiTables: String, cassandraInvIndexTables: String) {
    println("Creating InverseIndex for Cassandra")

    val sparkConf = new SparkConf(true).setAppName("InvIndexBuilder_Cassandra")
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.port", cassandraPort.toString())
      .set("spark.cassandra.auth.username", cassandraUser)
      .set("spark.cassandra.auth.password", cassandraPW)
    val sc = new SparkContext(sparkConf)
   
    //Highest docID in the wikiarticles-collection
     val c = 9893064
     //fetch one doc at a time with cassandra
    for (i <- 1 to c by 1) {
   
    println(i)
    
    val rdd =
      sc.cassandraTable(cassandraKeyspace, cassandraWikiTables).select("docid", "wikitext").where("docid = ?", s"${i}")
    val documents = rdd
      .map(x => (x.get[Long]("docid"), InverseIndexBuilderImpl.buildIndexKeys(x.get[String]("wikitext"))))
    val invIndexEntries = documents
      .map(x => InverseIndexBuilderImpl.buildInverseIndexEntry(x._1, x._2))
    val merge = invIndexEntries
      .flatMap(identity).map(x => (x._1, x._2._1, x._2._2))
    merge
      .saveToCassandra(cassandraKeyspace, cassandraInvIndexTables, SomeColumns("word", "docid", "occurences"))
     }

    sc.stop()
    println("Import Complete")
  }

  /*
   * Method that gets the Wikipedia-articles from the Wikippedia-articles MySQL Table
   *  and then builds an inverted index for them and stores them in a new MySQL Table in the Database
   * 
   * @param mySQLDBPath IP required to access MySQL
   * @param mySQLDBPort Port required to access MySQL
   * @param mySQLDBUser MySQL Database User
   * @param mySQLDBPW MySQL Database Password
   * @param mySQLDBDatabase MySQL Database name 
   * @param mySQLWikiTable Name of the MySQL Table containing the Wikipedia-Articles
   * @param mySQLIndexTable Name of the MySQL Table where the inverted Index should be stored
   * 
   * If the given mySQLIndexTable doesn't exist a table with the given name will be created. The Schema of the table will be 
   * sampled from the spark.dataframes schema. 
   * In This case that means a table with the following collumns will be created:
   *  word (indexed term) [TEXT]
   *  docID (Wikipedia-documentID) [BIGINT]
   *	occurence (single occurence of the term in the specified docID) [INT]
   * 
   * Note: 
   * 1. If a table with the matching name is created beforehand (recommended) different MySQL DATATYPES can be used, since Spark 
   * always seem to map Strings regardless of length to TEXT this could be a good thing to improve performance (e.g. use VARCHAR(x). 
   * 2. Spark will create a table without PRIMARY KEYs or INDEXes (if no table is created beforehand). This will slow down the retrival 
   * of specific indexes by a fair amount 
   * 
   */
  private def createInverseIndexMySQL(mySQLDBHost: String, mySQLDBPort: Int, mySQLDBUser: String, mySQLDBPW: String, mySQLDBDatabase: String, mySQLWikiTable: String, mySQLIndexTable: String) {
    println("Creating InverseIndex for MySQL")
    val mySQLClient = s"jdbc:mysql://${mySQLDBHost}:${mySQLDBPort}/${mySQLDBDatabase}"
    val conf = new SparkConf().setAppName("InvIndexBuilder")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", mySQLDBUser)
    prop.setProperty("password", mySQLDBPW)

        //Highest docID in the wikiarticles-collection
    val c = 9893064

    //just one document per loop recommenced has proven to be the fastest
    for (i <- 1 to c by 1) {

      println(i)

      val df = sqlContext.read.jdbc(mySQLClient, mySQLWikiTable, prop).select("dociD", "wikitext").where(s"docID = ${i}")
      val documents = df.rdd
        .map(x => (x.getLong(0), InverseIndexBuilderImpl.buildIndexKeys(x.getString(1))))

      val invIndexEntries = documents.map(x => InverseIndexBuilderImpl.buildInverseIndexEntry(x._1, x._2))
      val merge = invIndexEntries.flatMap(identity)
        .map(X => ((X._1, X._2._1), X._2._2))
        .flatMapValues(identity)
        .map(X => (X._1._1, X._1._2, X._2))

      val dfWithSchema = sqlContext.createDataFrame(merge).toDF("word", "docID", "occurence")
      dfWithSchema.write.mode("append").jdbc(mySQLClient, mySQLIndexTable, prop)
    }
    sc.stop()
    println("Import Complete")
  }

  /*
   * Method that gets the Wikipedia-articles from the Wikipedia-articles MongoDB Collection
   *  and then builds an inverted index for them and stores them in a new MongoDB Collection in the same Database
   * 
   * @param mongoDBPath IP required to access MongoDB
   * @param mongoDBPort Port required to access MongoDB
   * @param mongoDBUser MongoDB Database User
   * @param mongoDBPW MongoDB Database Password
   * @param mongoDBDatabase MongoDB Database name 
   * @param mongoDBWikiCollection Name of the MongoDB Collection containing the Wikipedia-Articles
   * @param mongoDBIndexCollection
   * 
   * If the given Collection doesn't exist a Collection with the given name will be created. The Schema of the Collection will be 
   * sampled from the spark.dataframes schema. 
   * In This case that means a table with the following fields and structure will be created:
   * 
   * 	{	_id (word) : String 
   * 		invIndex(indexStructure) : Array<Documents> [
   * 		{
   * 			_1 (docID) : NumberLong,
   * 			_2 (occurences of word): Array<Integer> [ (e.g. 1,5,7,233)
   * 					]
   * 				}
   * 			]
   * 	}
   * 
   * 
   * Note: 
   * 1. If a Collection with the matching name is created beforehand (not recommended) different MongoDB DATATYPES can be used, since Spark samples the
   * Collections Schema from the Dataframe
   * 2. Spark will create a collection without PRIMARY KEYs or INDEXes if you want those on your collections alter it afterwards
   * 3. In this case since we use the word/term  as "_id" field in MongoDB it will have an INDEX 
   * 
   */
  private def createInverseIndexMongo(mongoDBHost: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String, mongoDBDatabase: String, mongoDBWikiCollection: String, mongoDBIndexCollection: String) {
    println("Creating InverseIndex for MongoDB")
    val mongoClient = s"mongodb://${mongoDBUser}:${mongoDBPW}@${mongoDBHost}:${mongoDBPort}/${mongoDBDatabase}"
    val sc = SparkSession.builder().appName("WikiImporter")
      .config("spark.mongodb.input.uri", s"${mongoClient}.${mongoDBWikiCollection}")
      .config("spark.mongodb.output.uri", s"${mongoClient}.${mongoDBIndexCollection}")
      .getOrCreate()

    val df = MongoSpark.load(sc)
    //Highest docID in the wikiarticles-collection
    val c = 9893064
    //how many documents to fetch at once
    val x = 500
    
    for (i <- x to c by x) {
    println(x)
      val documents = df.select("_id", "wikitext").filter(df("_id") < i).filter(df("_id") > (i - x))
        .rdd
        .map(x => (x.getLong(0), InverseIndexBuilderImpl.buildIndexKeys(x.getString(1))))

      val invIndexEntries = documents.map(x => InverseIndexBuilderImpl.buildInverseIndexEntry(x._1, x._2))
      val merge = invIndexEntries.flatMap(identity).groupBy(_._1).map { case (k, v) => (k, v.map(_._2).toList) }
      val dfWithSchema = sc.createDataFrame(merge.toJavaRDD()).toDF("_id", "invIndex")

      MongoSpark.save(dfWithSchema.write.mode("append"))
    }

    sc.stop()
    println("Import Complete")
  }

}
