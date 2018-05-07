import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.sql.{DataFrame, SQLContext}
import spray.json.RootJsonFormat

import scala.collection.mutable.ListBuffer

case class Search(columnName: String, operator:String, columnType:String, value:String)
case class Schema(separator:String, search:Array[Search])
case class Filter(id:Int, types:String, schema:Schema)

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val searchFormat = jsonFormat(Search, "columnName", "operator", "columnType", "value")
  implicit val schemaFormat = jsonFormat(Schema, "separator", "search")
  implicit val filterFormat = jsonFormat(Filter, "id", "types", "schema")
}
import MyJsonProtocol._


case class FilterManager() {
  def process(): Unit ={
    print("Processing")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("datasetCleaner")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    //val dataset = sparkContext.textFile("sparktest/dataset.csv")
    val dataset = sqlContext.read.format("csv")
      .option("header", "true")
      .option("ignoreLeadingWhiteSpace",false)
      .option("ignoreTrailingWhiteSpace",false)
      .load("sparktest/dataset.csv").cache()
    //dataset.printSchema()

    //dataset.take(2).foreach(println)
    val source = scala.io.Source.fromFile("sparktest/filterFile.json")
    val filterFile = try source.mkString finally source.close()
    val filterList = loadJsonFile(filterFile)
    var toDeleteBuffer = new ListBuffer[DataFrame]
    filterList.schema.search.foreach(x => toDeleteBuffer += returnMatchedLines(x.columnName,x.operator,x.value, dataset))
    val toDelete = toDeleteBuffer.toList
    val toDeleteDF = toDelete.reduce(_ union _)
    toDeleteDF.show()
    val finalDataframe = dataset.join(toDeleteDF,dataset("id")===toDeleteDF("id"),"leftanti")
      .write.format("com.databricks.spark.csv")
      .save("sparktest/cleanDataset")
  }

  def returnMatchedLines(columnName:String, operator:String, value:String, data: DataFrame): DataFrame ={
    val condition = columnName + " " + operator + " " + "'" + value + "'"
    //val condition = "types = 'FL'"
    val filteredDF = data.filter(condition).coalesce(1)
    filteredDF.write.format("com.databricks.spark.csv").save("sparktest/pending/toDelete_" + columnName)
    val result = filteredDF.select("id")
    result
  }

  def loadJsonFile(filterFile: String): Filter = {
    print("Load JSON")
    val convertedJson:Filter = filterFile.parseJson.convertTo[Filter]
    convertedJson
  }

  def cleanAfterProcess(pathToClean:String, pathToPending:String, sqlC: SQLContext): Unit ={
    val cleanDataset = sqlC.read.format("csv")
      .option("header", "true")
      .option("ignoreLeadingWhiteSpace",false)
      .option("ignoreTrailingWhiteSpace",false)
      .load("sparktest/cleanDataset")

  }
}
