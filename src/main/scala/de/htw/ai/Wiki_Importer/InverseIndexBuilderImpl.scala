package de.htw.ai.Wiki_Importer

import java.util.Locale
import scala.annotation.tailrec

/**
 * Created by chris on 06.11.2016.
 * Modified by Jörn Sattler on 25.07.2017
 * Source: https://github.com/WikiPlag/wiki_data_fetcher
 *
 */
object InverseIndexBuilderImpl {

  var stopWords: Set[String] = _

  /**
   * Reads stop words from a file and stores them in the {@link InverseIndexBuilderImpl#stopWords} variable.
   *
   * @param stopWordsFile the file name.
   * Stopwords taken from: https://solariz.de/de/deutsche_stopwords.htm
   */
  def loadStopWords(stopWordsFile: String = "stopwords.txt") = {
    stopWords = Option(getClass.getClassLoader().getResourceAsStream(stopWordsFile))
      .map(scala.io.Source.fromInputStream)
      .map(_.getLines.toSet)
      .getOrElse(scala.io.Source.fromFile(stopWordsFile).getLines.toSet)
  }

  /**
   * Builds the inverse index for a certain document. There will be no normalization or stemming applied. You may use
   * {@link InverseIndexBuilderImpl#buildIndexKeys} beforehand to achieve this.
   *
   * <p>
   * <b>example:</b>
   * <br/>
   * {@code
   * doc_id = 100,
   *   }
   * <br/>
   * {@code
   * tokens = ["kam", "die", "Parodie", "An", "kam", "Alan", "die", "Smithee", "Film", "Burn", "Hollywood", "Film"],
   *   }
   * <br/>
   * {@code
   * return {
   *     "kam" : [(100, [0, 4])],
   *     "die" : [(100, [1, 6])],
   *     "Parodie" : [(100, [2])],
   *     "An" : [(100, [3])],
   *     "Alan" : [(100, [5])],
   *     "Smithee" : [(100, [7])],
   *     "Film" : [(100, [8, 11])],
   *     "Burn" : [(100, [9])],
   *     "Hollywood" : [(100, [10])],
   *   }
   * }
   * <p/>
   *
   * @param doc_id The document's identifier.
   * @param tokens The parsed and normalized words of the document.
   * @return The inverse index for the passed document
   *
   * Change by JörnSattler: Stopwords where filtered at a different method and thereby were producing wrong results
   */
  def buildInverseIndexEntry(doc_id: Long,
                             tokens: List[String]): Map[String, (Long, List[Int])] = {
    if (stopWords == null) { loadStopWords() }

    tokens.foldLeft((Map.empty[String, (Long, List[Int])], 0)) {
      (entry, x) =>
        {
          val docList = entry._1.getOrElse(x, (doc_id, List.empty[Int]))._2
          (entry._1.updated(x, (doc_id, docList :+ entry._2)), entry._2 + 1)
        }
      //Edit: Remove Stopwords here to produce keys matching to the actual wikitext.
    }._1.filterKeys(x => !stopWords.contains(x))

  }

  /**
   * Merges an array of different inverse indexes to get a single index. Actually this function works like a reducer
   * and is important when building a huge inverse index in a distributed system.
   *
   * @param entries The inverse indexes
   * @return a single inverse index based on the input indexes
   */
  def mergeInverseIndexEntries(entries: List[Map[String, (Long, List[Int])]]): Map[String, List[(Long, List[Int])]] = {
    entries.foldLeft(Map.empty[String, List[(Long, List[Int])]]) {
      (map, entryMap) =>
        {

          entryMap.foldLeft(map) {
            (map, x) =>
              {
                val docList = map.getOrElse(x._1, List.empty[(Long, List[Int])])
                map.updated(x._1, docList :+ x._2)
              }
          }
        }
    }
  }

  def buildIndexKeySet(documentText: String): Set[String] = buildIndexKeys(documentText).toSet

  /**
   * Parses a given input text into a collection of words and transforms these words into tokens to build an inverse
   * index.
   * <p>
   * The used parser extracts symbols and characters of the wikipedia markup language from the input text. The text
   * will then be normalized and certain stop words will be removed. Lastly, combinations of n tokens are used to build
   * compound token keys. (currently n = 1, meaning compound tokens are disabled)
   * </p>
   *
   * Change by Jörn: Changed function to retreive tokens without wikimarkup language instread of only words.
   * Change by Jörn:  Removing stopwords here produces incorrect tokens which don't match with the text.
   *
   * @param documentText the input text that should be parsed.
   * @return a collection of tokens already prepared to build an inverse index.
   */
  def buildIndexKeys(documentText: String): List[String] = {
    var tokens = WikiDumpParser.extractWikiDisplayText(documentText)
    normalize(tokens)
  }

  /**
   * Reduces the variety of word.
   *
   * <p>
   * <b>The following approaches are used so far:</b>
   * <ul>
   * <li>to lower case</li>
   * </ul>
   *
   * <b>Not implemented:</b>
   * <ul>
   * <li>stemming</il>
   * <li>synonyms</il>
   * </ul>
   * </p>
   *
   * @param rawWords words that are not yet normalized
   * @return normalized tokens
   */
  def normalize(rawWords: List[String]): List[String] = {
    rawWords.map(x => x.toLowerCase(Locale.ROOT))

    /**
     * ToDo:
     * - apply stemming: only store root versions of words
     * - merge synonyms of words to one term
     *
     * The latter could really influence our similarity results in a bad way. We should take care that this is not
     * going in the wrong direction and suddenly everything becomes a plagiarism.
     *
     * For more information on how to build an inverse index, look up the elastic search doc.
     * see: https://www.elastic.co/guide/en/elasticsearch/guide/current/inverted-index.html
     */
  }
  //Why is that even here?
  private def buildSingleTokenKeys(uniqueTokens: List[String]): List[String] = uniqueTokens

  @tailrec
  private def build2TokenKeys(uniqueTokens: List[String], agg: List[String]): List[String] = {
    if (uniqueTokens.isEmpty)
      return agg

    val head = uniqueTokens.head
    val tail = uniqueTokens.tail

    if (tail == null || tail.isEmpty)
      return agg

    val strings = List(head, tail.head).sortWith((e1, e2) => (e1 compareTo e2) < 0)

    build2TokenKeys(uniqueTokens.tail, agg :+ strings.mkString)
  }

}
