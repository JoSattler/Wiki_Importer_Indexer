

package de.htw.ai.Wiki_Importer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import com.mongodb.spark.config._
import com.mongodb.spark.sql._


/*
 * Created by Jörn Sattler on
 * 25.07.2017
 * 
 * Class used for Benchmarktesting of the databases. Reads in given files containing terms which are
 * then searched for while measuring the time. 
 */
object Benchmarktest {

  var titleSet: Set[String] = _
  var docIDSet: Set[Long] = _
  var wordSet: Set[String] = _

  val exactList = new ListBuffer[(String, Double)]()
  val containsList = new ListBuffer[(String, Double)]()
  val docwordList = new ListBuffer[(Long, Double)]()
  val worddocList = new ListBuffer[(String, Double)]()

  /*
   *  Reads required resources for the Benchmark Tests,
   *  e.g. terms which should be searched for
   *  
   *  @param titleSetfile name of the file for title search Benchmarks
   *  @param docIDSetFile name of the file for docID search Benchmarks
   *  @param wordIDSetFile name of the file for word search Benchmarks
   *  
   */
  def loadResources(titleSetFile: String = "titleset.txt", docIDSetFile: String = "docIDSet.txt", wordSetFile: String = "wordSet.txt") = {
    titleSet = Option(getClass.getClassLoader().getResourceAsStream(titleSetFile))
      .map(scala.io.Source.fromInputStream)
      .map(_.getLines.toSet)
      .getOrElse(scala.io.Source.fromFile(titleSetFile).getLines.toSet)
    docIDSet = Option(getClass.getClassLoader().getResourceAsStream(docIDSetFile))
      .map(scala.io.Source.fromInputStream)
      .map(_.getLines().toSet)
      .getOrElse(scala.io.Source.fromFile(docIDSetFile).getLines.toSet).map(_.toLong)

    wordSet = Option(getClass.getClassLoader().getResourceAsStream(wordSetFile))
      .map(scala.io.Source.fromInputStream)
      .map(_.getLines.toSet)
      .getOrElse(scala.io.Source.fromFile(wordSetFile).getLines.toSet)
  }


  /*
   * Tests a given mongoDB and its "wikiarticle" and "inverseIndezes" collections for querytime of specified queries. 
   * Also Prints the results in the end in form: (searchterm, query response time)
   * 
   * @param mongoDBPath IP required to access MongoDB
   * @param mongoDBPort Port required to access MongoDB
   * @param mongoDBUser MongoDB Database User
   * @param mongoDBPW MongoDB Database Password
   * @param mongoDBDatabase MongoDB Database name 
   * @param mongoDBWikiCollection Name of the MongoDB Collection containing the Wikipedia-Articles
   * @param mongoDBIndexCollection
   *  
   *  Queries: 
   *  1. Find the document in the "wikiarticles" collection with the exact given title.
   *  2. Find all documents in the "wikiarticles" whose title contains the given String
   *  3. Find all words of a given document (docID) in the "inverseIndezes" collection
   *  4. Find all documents containing the given word in the "inverseIndezes" collection
   */
  def testMongo(mongoDBHost: String, mongoDBPort: Int, mongoDBUser: String, mongoDBPW: String, mongoDBDatabase: String, mongoDBWikiCollection: String, mongoDBIndexCollection: String) {
    val mongoClient = s"mongodb://${mongoDBUser}:${mongoDBPW}@${mongoDBHost}:${mongoDBPort}/${mongoDBDatabase}"
    val sc = SparkSession.builder().appName("Database_Benchmark_MongoDB")
      .getOrCreate()
    val rconfig1 = ReadConfig(Map("uri" -> s"${mongoClient}.${mongoDBWikiCollection}"))
    val rconfig2 = ReadConfig(Map("uri" -> s"${mongoClient}.${mongoDBIndexCollection}"))
    if (titleSet == null || docIDSet == null || wordSet == null) { loadResources() }

    titleSet.foreach(f =>
      (exactList += testWikiMongoFindTitleexact(sc, rconfig1, f),
        containsList += testWikiMongoFindTitle(sc, rconfig1, f)))

    docIDSet.foreach(f =>
      docwordList += testIndexMongofindDocWords(sc, rconfig2, f))

    wordSet.foreach(f =>
      worddocList += testIndexMongofindDocWordDoc(sc, rconfig2, f))

    println("TEST MongoDB")
    println("----------------------------------------------------------")
    println("----------------------------------------------------------")
    println("TEST EXACT TITLE MATCHES MongoDB")
    println("----------------------------------------------------------")
    exactList.toList.foreach(println)
    println("----------------------------------------------------------")
    println("TEST EXACT TITLE MATCHES MongoDB")
    println("----------------------------------------------------------")
    containsList.toList.foreach(println)
    println("----------------------------------------------------------")
    println("TEST EXACT TITLE MATCHES MongoDB")
    println("----------------------------------------------------------")
    docwordList.toList.foreach(println)
    println("----------------------------------------------------------")
    println("TEST EXACT TITLE MATCHES MongoDB")
    println("----------------------------------------------------------")
    worddocList.toList.foreach(println)

    sc.stop()
  }

  /*
   * Tests a given MySQLDB and its "wikiarticle" and "inverseIndezes" Tables for querytime of specified queries. 
   * Also Prints the results in the end in form: (searchterm, query response time)
   * 
   * @param mySQLDBPath IP required to access MySQL
   * @param mySQLDBPort Port required to access MySQL
   * @param mySQLDBUser MySQL Database User
   * @param mySQLDBPW MySQL Database Password
   * @param mySQLDBDatabase MySQL Database name 
   * @param mySQLWikiTable Name of the MySQL Table containing the Wikipedia-Articles
   * @param mySQLIndexTable Name of the MySQL Table where the inverted Index should be stored
   *  
   *  Queries: 
   *  1. Find the document in the "wikiarticles" Table with the exact given title.
   *  2. Find all documents in the "wikiarticles" Table whose title contains the given String
   *  3. Find all words of a given document ("docID") in the "inverseIndezes" Table
   *  4. Find all documents containing the given word in the "inverseIndezes" Table
   */
  def testMySQl(mySQLDBHost: String, mySQLDBPort: Int, mySQLDBUser: String, mySQLDBPW: String, mySQLDBDatabase: String, mySQLWikiTable: String, mySQLInvIndexTable: String) {
    val mySQLClient = s"jdbc:mysql://${mySQLDBHost}:${mySQLDBPort}/${mySQLDBDatabase}"
    val conf = new SparkConf().setAppName("Database_Benchmark_MySQL")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    if (titleSet == null || docIDSet == null || wordSet == null) { loadResources() }
    //create Properties to be able to read/write to/from the MySQL Database
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", mySQLDBUser)
    prop.setProperty("password", mySQLDBPW)

    titleSet.foreach(f =>
      (exactList += testWikiMySQLFindTitleexact(sqlContext, mySQLClient, mySQLWikiTable, prop, f),
        containsList += testWikiMySQLFindTitle(sqlContext, mySQLClient, mySQLWikiTable, prop, f)))

    docIDSet.foreach(f =>
      docwordList += testIndexMySQLfindDocWords(sqlContext, mySQLClient, mySQLInvIndexTable, prop, f))

    wordSet.foreach(f =>
      worddocList += testIndexMySQLfindDocWordDoc(sqlContext, mySQLClient, mySQLInvIndexTable, prop, f))

    println("TEST MySQL")
    println("----------------------------------------------------------")
    println("----------------------------------------------------------")
    println("TEST EXACT TITLE MATCHES MySQL")
    println("----------------------------------------------------------")
    exactList.toList.foreach(println)
    println("----------------------------------------------------------")
    println("TEST TITLE CONTAINS MATCHES MySQL")
    println("----------------------------------------------------------")
    containsList.toList.foreach(println)
    println("----------------------------------------------------------")
    println("TEST GET ALL WORDS OF DOCUMENT MySQL")
    println("----------------------------------------------------------")
    docwordList.toList.foreach(println)
    println("----------------------------------------------------------")
    println("TEST GET ALL DOCS WITH WORD MySQL")
    println("----------------------------------------------------------")
    worddocList.toList.foreach(println)

    sc.stop()
  }

  /*
   * Tests a given CassandraDB and its "wikiarticle" and "inverseIndezes" Column Families for querytime of specified queries. 
   * Also Prints the results in the end in form: (searchterm, querytime)
   * 
   * @param cassandraPort Portnumber required to access Cassandra
   * @param cassandraDatabase IP required to access Cassandra
   * @param cassandraUser Cassandra Database User
   * @param cassandraPW Cassandra Database Password
   * @param cassandraKeyspace Cassandra Keyspace (analog with MySQL Database) name 
   * @param cassandraWikiTables Name of the Cassandra Collumn Family containing the Wikipedia-Articles
   *  
   *  Queries: 
   *  1. Find the document in the "wikiarticles" Collumn Family with the exact given title.
   *  2. Find all documents in the "wikiarticles" whose title contains the given String
   *  3. Find all words of a given document ("docID") in the "inverseIndezes" Column Family
   *  4. Find all documents containing the given word in the "inverseIndezes" Column Family
   */
  def testCassandra(cassandraHost: String, cassandraPort: Int, cassandraUser: String, cassandraPW: String, cassandraKeyspace: String, cassandraWikiTables: String, cassandraInvIndexTables: String) {
    val sparkConf = new SparkConf(true).setAppName("Database_Benchmark_Cassandra")
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.port", cassandraPort.toString())
      .set("spark.cassandra.auth.username", cassandraUser)
      .set("spark.cassandra.auth.password", cassandraPW)

    if (titleSet == null || docIDSet == null || wordSet == null) { loadResources() }

    val sc = new SparkContext(sparkConf)

    titleSet.foreach(f => (exactList += testWikiCassandraFindTitleexact(sc, cassandraKeyspace, cassandraWikiTables, f),
      containsList += testWikiCassandraFindTitle(sc, cassandraKeyspace, cassandraWikiTables, f)))

    docIDSet.foreach(f => docwordList += testIndexCassandraDocWords(sc, cassandraKeyspace, cassandraInvIndexTables, f))

    wordSet.foreach(f => worddocList += testIndexCassandraDocWordDoc(sc, cassandraKeyspace, cassandraInvIndexTables, f))

    println("TEST CASSANDRA")
    println("----------------------------------------------------------")
    println("----------------------------------------------------------")
    println("TEST EXACT TITLE MATCHES CASSANDRA")
    println("----------------------------------------------------------")
    exactList.toList.foreach(println)
    println("----------------------------------------------------------")
    println("TEST TITLE CONTAINS MATCHES CASSANDRA")
    println("----------------------------------------------------------")
    containsList.toList.foreach(println)
    println("----------------------------------------------------------")
    println("TEST GET ALL WORDS OF DOCUMENT CASSANDRA")
    println("----------------------------------------------------------")
    docwordList.toList.foreach(println)
    println("----------------------------------------------------------")
    println("TEST GET ALL DOCS WITH WORD CASSANDRA")
    println("----------------------------------------------------------")
    worddocList.toList.foreach(println)

    sc.stop
  }
/*
 * Finds document with exact matching title
 */
  def testWikiCassandraFindTitleexact(sc: SparkContext, cassandraKeyspace: String, cassandraWikiTables: String, search: String): (String, Double) = {
    val df = sc.cassandraTable(cassandraKeyspace, cassandraWikiTables)
    (search, time(df.filter(doc => doc.getString(1) == search)))
  }
  /*
   * Finds all documents containing the title
   */
  def testWikiCassandraFindTitle(sc: SparkContext, cassandraKeyspace: String, cassandraWikiTables: String, search: String): (String, Double) = {
    val df = sc.cassandraTable(cassandraKeyspace, cassandraWikiTables)
    (search, time(df.filter(doc => doc.getString(1).contains(search))))
  }
  /*
   * Finds all words for a given document
   */
  def testIndexCassandraDocWords(sc: SparkContext, cassandraKeyspace: String, cassandraInvIndexTables: String, search: Long): (Long, Double) = {
    val df = sc.cassandraTable(cassandraKeyspace, cassandraInvIndexTables)
    (search, time(df.filter(doc => doc.getLong(1)== search)))
  }
/*
 * Finds all documents containing the term
 */
  def testIndexCassandraDocWordDoc(sc: SparkContext, cassandraKeyspace: String, cassandraInvIndexTables: String, search: String): (String, Double) = {
    val df = sc.cassandraTable(cassandraKeyspace, cassandraInvIndexTables)
    (search, time(df.filter(doc => doc.getString(0) == search)))
  }
/*
 * Finds document with exact matching title
 */
  def testWikiMongoFindTitleexact(sc: SparkSession, rc: ReadConfig, search: String): (String, Double) = {
    val df = sc.loadFromMongoDB(rc)
    (search, time(df.filter(x => x.getString(1) == search)))
  }
  /*
   * Finds all documents containing the title
   */
  def testWikiMongoFindTitle(sc: SparkSession, rc: ReadConfig, search: String): (String, Double) = {
    val df = sc.loadFromMongoDB(rc)
    (search, time(df.filter(doc => doc.getString(1).contains(search))))
  }
  /*
   * Finds words and for a given document
   */
  def testIndexMongofindDocWords(sc: SparkSession, rc: ReadConfig, search: Long): (Long, Double) = {
    val df = sc.loadFromMongoDB(rc)
    (search, time(df.select("*").where(df("invIndex._1")(0).equalTo(search))))
  }
 /*
 * Finds all documents containing the term
 */
  def testIndexMongofindDocWordDoc(sc: SparkSession, rc: ReadConfig, search: String): (String, Double) = {
    val df = sc.loadFromMongoDB(rc)
    (search, time(df.filter(doc => doc.getString(0) == search)))
  }
/*
 * Finds document with exact matching title
 */
  def testWikiMySQLFindTitleexact(sqlContext: SQLContext, mySQLClient: String, mySQLWikiTable: String, prop: java.util.Properties, search: String): (String, Double) = {
    val df = sqlContext.read.jdbc(mySQLClient, mySQLWikiTable, prop)
    (search, time(df.filter(doc => doc.getString(1) == search)))
  }
    /*
   * Finds all documents containing the title
   */
  def testWikiMySQLFindTitle(sqlContext: SQLContext, mySQLClient: String, mySQLWikiTable: String, prop: java.util.Properties, search: String): (String, Double) = {
    val df = sqlContext.read.jdbc(mySQLClient, mySQLWikiTable, prop)
    (search, time(df.filter(doc => doc.getString(1).contains(search))))
  }
  /*
   * Finds words and for a given document
   */
  def testIndexMySQLfindDocWords(sqlContext: SQLContext, mySQLClient: String, mySQLInvIndexTable: String, prop: java.util.Properties, search: Long): (Long, Double) = {
    val df = sqlContext.read.jdbc(mySQLClient, mySQLInvIndexTable, prop)
    (search, time(df.filter(doc => doc.getLong(1) == search)))
  }
/*
 * Finds all documents containing the term
 */
  def testIndexMySQLfindDocWordDoc(sqlContext: SQLContext, mySQLClient: String, mySQLInvIndexTable: String, prop: java.util.Properties, search: String): (String, Double) = {
    val df = sqlContext.read.jdbc(mySQLClient, mySQLInvIndexTable, prop)
    (search, time(df.filter(doc => doc.getString(0) == search)))
  }

 /*
  * source: http://biercoff.com/easily-measuring-code-execution-time-in-scala/
  * Used for measuring codeexecution time.
  */
  def time[R](block: => R): Double = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    (t1 - t0) / 1e9d
  }

}