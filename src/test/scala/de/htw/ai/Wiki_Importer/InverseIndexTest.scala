package de.htw.ai.Wiki_Importer

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner



/**
  * Created by chris on 06.11.2016.
  */
@RunWith(classOf[JUnitRunner])
class InverseIndexTest extends FunSuite {

  trait TestObject {
    val testObject = InverseIndexBuilderImpl
    testObject.loadStopWords()
  }

  test("testEmptyList") {


    new TestObject {
      val map_e1 = testObject.buildIndexKeys("")

      assert(map_e1.isEmpty)

    }
  }



  test("testBuildInverseIndexEntry(doc_id, pageWordsAsList)") {

    val doc_id = 13
    val temsInDocument1 = List("Ä", "Ü", "Ö", "Ελλάδα", "Elláda", "Ä", "Ü", "Ö", "Ελλάδα", "Elláda")

    new TestObject {
      val map_e1 = testObject.buildInverseIndexEntry(doc_id, temsInDocument1)

      assert(map_e1.size == 5)
      assert(map_e1.get("Ä").get._1 == doc_id)
      assert(map_e1.get("Ä").get._2.size == 2)
      assert(map_e1.get("Ä").get._2(1) == 5)

    }
  }



  test("testKeyBuilder") {
    val docText = "1997 kam die Parodie die kam Parodie An Alan Smithee Film: Burn Hollywood"

    new TestObject {
      val resultList = testObject.buildIndexKeys(docText)
      val resultSet = testObject.buildIndexKeySet(docText)

      assert(resultList.size == 13)
      println(resultList(1))
      assert(resultList.head.equals("1997"))
      assert(resultList(1).equals("kam"))
      assert(resultList(2).equals("die"))

      assert(resultSet.size == 10)
      assert(resultSet.contains("1997"))
      assert(resultSet.contains("kam"))
      assert(resultSet.contains("die"))

    }
  }
}