package de.htw.ai.Wiki_Importer


import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


@RunWith(classOf[JUnitRunner])
class BenchmarkTest_MySQLTest extends FunSuite with BeforeAndAfterAll {
  
   var sc: SparkContext = _
   var sparkConf: SparkConf = _
   var sqlContext: SQLContext =_
   var prop: java.util.Properties = _
   var mySQLClient:String = _
   var mySQLDBHost = "<DB_Host>"
   var mySQLDBPort = "<Port>"
   var mySQLDBDatabase = "<Database_Name>"
   var mySQLDBUser = "<User_Name>"
   var mySQLDBPW = "<DB_Password>"
   var mySQLWikiTable = "<Wiki_Table_Name>"
   var mySQLInvIndexTable = "<Inverse_Indezies_Table_Name>"
   
  
   
      override protected def beforeAll() { 
    mySQLClient = new String(s"jdbc:mysql://${mySQLDBHost}:${mySQLDBPort}/${mySQLDBDatabase}")
    sparkConf = new SparkConf().setAppName("InvIndexBuilder").setMaster("local[*]")
    sc = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sc)
    
    prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", mySQLDBUser)
    prop.setProperty("password", mySQLDBPW)
   }
   
   
   
   
     test("findexacttitle_MySQL") {
    val df = sqlContext.read.jdbc(mySQLClient, mySQLWikiTable, prop).select("*").where("title = 'Anime'")
    val article = df
    assert(article.count() == 1)
    assert(article.first().getString(1) == "Anime")
  }

     test("finddocID_MySQl") {
    val df = sqlContext.read.jdbc(mySQLClient, mySQLWikiTable, prop)
    val articles = df.select("*").where(s"docID = 1")  
    assert(articles.first().getLong(0) == 1)  
  }

    test("finddocwords_MySQL") {
    val df = sqlContext.read.jdbc(mySQLClient, mySQLInvIndexTable, prop).select("*").where("docID = 1")
    val articles = df
    assert(articles.count() == 405)
    assert(articles.first().getString(0) == "alan")
    
  }
  
   test("findworddocs_MySQL") {
    val df = sqlContext.read.jdbc(mySQLClient, mySQLInvIndexTable, prop).select("*").where("word = 'anime'")
    val articles = df
    assert(articles.first().getString(0) == "anime")
    
  }  
   
   
   
   
   
     override protected def afterAll() {

     if (sc!=null) {sc.stop; println("Spark stopped......")}
     else println("Cannot stop spark - reference lost!!!!")
  }
   
}
