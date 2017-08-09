package de.htw.ai.Wiki_Importer

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import com.mongodb.spark.config.ReadConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.mongodb.spark.config._
import com.mongodb.spark.sql._
import scala.collection.JavaConversions._

 case class ARSEGHHOLE(word: String, lsd: List[(Long, List[Int])])

@RunWith(classOf[JUnitRunner])
class BenchmarkTest_MongoDB extends FunSuite with BeforeAndAfterAll{
  
  var sc: SparkSession = _
  var mongoDBDatabase = "<DB Name>"
  var  mongoDBPort = "<Port>"
  var  mongoDBHost = "<Host>"
  var mongoDBPW = "<DB_Password>"
  var mongoDBUser = "<DB_User>"
  var mongoClient: String = _
  var mongoDBWikiCollection ="<Wiki_collection_name>"
  var mongoDBIndexCollection = "<Inverse_Indezes_collection_name>"
  var rconfic1: ReadConfig= _
  var rconfic2: ReadConfig= _
 
  
        override protected def beforeAll() { 
     mongoClient = new String( s"mongodb://${mongoDBUser}:${mongoDBPW}@${mongoDBHost}:${mongoDBPort}/${mongoDBDatabase}")
     sc = SparkSession.builder().appName("WikiImporter")
      .master("local")
      .getOrCreate()
     rconfic1 =  ReadConfig(Map("uri" -> s"${mongoClient}.${mongoDBWikiCollection}"))
     rconfic2 =  ReadConfig(Map("uri" -> s"${mongoClient}.${mongoDBIndexCollection}"))
   }
  
  
       test("findexacttitle_MongoDB") {
    val df = sc.loadFromMongoDB(rconfic1)
    val article = df.select("*").filter(df("title").equalTo("Anime"))
    assert(article.count() == 1)
    assert(article.first().getString(1) == "Anime")
  }

     test("finddocID_MongoDB") {
    val df = sc.loadFromMongoDB(rconfic1)
    val articles = df.select("*").filter(df("_id").equalTo(1))   
    assert(articles.first().getLong(0) == 1)  
  }

    test("finddocwords_MongoDB") {
    val df = sc.loadFromMongoDB(rconfic2)
  
    val articles = df.select("_id","invIndex._1" ).where(df("invIndex._1")(0).equalTo(1))
    articles.collect.foreach(println)
    assert(articles.count == 282)
    assert(articles.first().getString(0) == "1968")
    
  }
  
   test("findworddocs_MySQL") {
    val df = sc.loadFromMongoDB(rconfic2)
    val articles = df.select("*").filter(df("word").equalTo("anime"))
    assert(articles.first().getString(0) == "anime")
    
  }  
        
        
        
        
        
       
        
        
        
             override protected def afterAll() {

     if (sc!=null) {sc.stop; println("Spark stopped......")}
     else println("Cannot stop spark - reference lost!!!!")
  }
        
}
