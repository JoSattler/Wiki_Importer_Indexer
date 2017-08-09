# Wiki_Importer_Indexer

Note this version required serious computing power.

* parses a given Wikipedia-articles.xml file ([download](https://dumps.wikimedia.org/dewiki/))
* fetches the Wikipedia articles
* stores it in either a MySQLDB, MongoDB or Apache Cassandra DB
* create an inverse index in either DB
* can run a benchmark test on each of the databases

## XML-Dump
To get an XML-Containing all aricles get https://dumps.wikimedia.org/dewiki/..........-pages-articles-multistream.xml.bz2
and unpack it.

## Build
```bash
sbt clean assembly
```
or without tests:

```bash
sbt "set test in assembly := {}" clean assembly
```

## Starting the application

```bash
spark-submit $SPARK wiki_importer_indexer.jar $OPTIONS   
```
```bash
$SPARK parameters are:
--executor-memory 20G --driver-memory 6G
```

```bash
$OPTIONS are: 
Following options are mandatory:
•	-dh database host
•	-dp database port
•	-du database user
•	-dpw database password
•	-dn database name e.g. "wiki"
•	-dt database type: MySQL =“mysql“, MongoDB=“mong“, Cassandra = „cass“
•	-wt table name of the wikipedia-articles table
•	-it name of the inverse-index table

Following options determine how the application will behave
•	-e parse wiki XML file and write the articles in the desired DB
•	-i build an inverse index for the desired db
•	-bt benchmark test the desired DB
•	-h for help
```
e.g.
```bash
wiki_importer_indexer.jar -e /home/user/downloads/wiki-articles.xml -dh databaseHost -dp databasePort -du databaseUser -dpw databasePassword -dn databaseName -dt mysql -wt wikitablename -it indextablename 
```
## Application Setup
If you want the application to handle the creation of the database tables you have to remove: .write.mode("append") in the SparkApp.scala. Then the framework will sample a table structure and create them accordingly. 

Note: Dont remove for mongoDB it will create the collection regardless 

If you want to create the tables first heres how you can do it:

### MySQL

WikiTable:
```bash
> CREATE TABLE <tableName> (docID BIGINT NOT NULL PRIMARY KEY, title VARCHAR(255),wikitext LONGTEXT);
```

inverse index Table: 
```bash
> CREATE TABLE <tableName>(word VARCHAR(65),docID BIGINT, occurence INT,PRIMARY KEY (word, docID, occurence));
```

### Apache Cassandra

WikiTable:
```bash
> CREATE TABLE <tablename>(
 		 docid bigint,
 		 title text,
 		 wikitext text,
 		 PRIMARY KEY(docid)
		 );
```

inverse index Table: 
```bash
> CREATE TABLE <tablename>(
		  word text,
		  docid bigint,
		  occurences List<int>,
		  PRIMARY KEY(word, docid)
		  );
```

### MongoDB

No Collection Setup needed
