package common

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import routines._

/**
 * Created by wulei7 on 2015/12/9.
 */
object toParquet {

  def main(args: Array[String]):Unit = {

    if(args.length < 2) {
      println("Usage: sql.query.toParquet [Table name] [Path to store the converted parquet file]\n")
      System.exit(1)
    }
    val tableName: String = args(0)
    val resultsPath: String = args(1)

    /** Parse the table-schema.xml file where tables are specified */
    val conf_dir: String = System.getenv("SPARK_HOME")
    val conf_xml: xml.Elem = xml.XML.loadFile(conf_dir + "/conf/table-schema.xml")
    val tables: mutable.HashMap[String, (Int, String, String, String, String)] = func.parseConfXML(conf_xml, tableName)

    /** Create Spark context as well as SQL context */
    val conf = new SparkConf().setAppName("Spark Utility [common.toParquet]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /** Search the required table in the table-schema.xml file,
      * note that only the table that is required will be parsed and processed. */
    for (table <- tables) {
    //for (table <- tables.filter(t => tableName.toLowerCase().equals(t._1.toLowerCase()))) {
      val columnNum: Int = table._2._1.toInt
      val columns: String = table._2._2.toString
      val storage: String = table._2._3.toString
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

      /**Convert the text file to parquet file based on the parsed table schema.*/
      sqlContext.createDataFrame(tableRDD, tableSchema).write.parquet(resultsPath)
      }
    }
}
