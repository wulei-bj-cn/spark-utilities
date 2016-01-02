package sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import routines._

/**
 * Created by wulei7 on 2015/12/9.
 */
object query {

  def main(args: Array[String]):Unit = {

    if(args.length < 1) {
      println("Usage: sql.query Sql-query-statement [Path-to-store-query-results]\n")
      System.exit(1)
    }
    val queryString: String = args(0)
    var resultsPath: String = "show"
    if(args.length > 1) {
      resultsPath = args(1)
    }

    /** Parse the table-schema.xml file where tables are specified */
    val conf_dir: String = System.getenv("SPARK_HOME")
    val conf_xml: xml.Elem = xml.XML.loadFile(conf_dir + "/conf/table-schema.xml")
    val tables: mutable.HashMap[String, (Int, String, String, String, String)] = func.parseConfXML(conf_xml, queryString)

    /** Create Spark context as well as SQL context */
    val conf = new SparkConf().setAppName("Spark Utility [sql.query] - Query: " +
      {if(queryString.length > 50) queryString.substring(0,50) else queryString})
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /** Recursively register every table specified in the table-schema.xml file,
      * note that only tables that are referenced in the query are registered. */
    for (table <- tables) {
    //for (table <- tables.filter(t => queryString.toLowerCase().contains(t._1.toLowerCase()))) {
      val tableName: String = table._1.toString
      val storage: String = table._2._3.toString
      val store_type: String = table._2._4.toString
      if (store_type.equals("parquet")) {
        sqlContext.read.parquet(storage).registerTempTable(tableName)
      } else if (store_type.equals("txt")) {
        val columnNum: Int = table._2._1.toInt
        val columns: String = table._2._2.toString
        val del: String = table._2._5.toString
        val tableSchema: StructType = func.createSchema(columns.toLowerCase)
        val tableRDD = sc.textFile(storage).map(_.split(del)).filter(_.length.equals(columnNum)).map {
          case p: Array[String] =>
            val lb: ListBuffer[Any] = new ListBuffer[Any]
            for (i <- (0 until columnNum)) {
              lb += p(i)
              tableSchema.fields(i).dataType match {
                case IntegerType => lb(i) = p(i).toInt
                case FloatType => lb(i) = p(i).toFloat
                case DoubleType => lb(i) = p(i).toDouble
                case _ =>
              }
            }
            Row.fromSeq(lb)
          case _ => Row()
        }
        sqlContext.createDataFrame(tableRDD, tableSchema).registerTempTable(tableName)
      }
    }

    /** Submit query and process */
    if(resultsPath.equals("show")) {
      sqlContext.sql(queryString).show
    } else {
      sqlContext.sql(queryString).rdd.saveAsTextFile(resultsPath)
    }
  }
}
