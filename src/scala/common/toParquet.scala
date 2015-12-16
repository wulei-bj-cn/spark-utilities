package common

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by wulei7 on 2015/12/9.
 */
object toParquet {

  def main(args: Array[String]):Unit = {

    if(args.length < 2) {
      println("Usage: sql.query.toParquet [Table name] [Path to store the converted parquet file]\n")
      System.exit(1)
    }
    val filterName: String = args(0)
    val resultsPath: String = args(1)

    /** Parse the table-schema.xml file where tables are specified */
    val conf_dir: String = System.getenv("SPARK_HOME")
    val conf_xml: xml.Elem = xml.XML.loadFile(conf_dir + "/conf/table-schema.xml")
    val tables: mutable.HashMap[String, (Int, String, String, String, String)] = parseConfXML(conf_xml, filterName)

    /** Create Spark context as well as SQL context */
    val conf = new SparkConf().setAppName("Spark Utility: sql.query.toParquet")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /** Recursively register every table specified in the table-schema.xml file,
      * note that only the table that is required will be registered. */
    for (table <- tables.filter(t => filterName.toLowerCase().equals(t._1.toLowerCase()))) {
      val columnNum: Int = table._2._1.toInt
      val columns: String = table._2._2.toString
      val storage: String = table._2._3.toString
      val del: String = table._2._5.toString

      val tableSchema: StructType = createSchema(columns.toLowerCase)
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

  /**Self-defined Functions*/
  /** Function name: parseConfXML
    *
    * @param conf_xml_file, which is of type xml.Elem
    * @return a hash map that contains
    *         key -> value
    *         tableName -> (columnNum,columns,storage,store_type,del)
    */
  def parseConfXML(conf_xml_file:xml.Elem, filterString:String):mutable.HashMap[String,(Int,String,String,String,String)] = {
    conf_xml_file match {
      case <tables>{tablesNode @ _*}</tables> =>
        var tables:mutable.HashMap[String,(Int,String,String,String,String)] = new mutable.HashMap[String,(Int,String,String,String,String)]()
        for (tableNode @ <table>{_*}</table> <- tablesNode.filter(t => filterString.equals(t.attribute("name").getOrElse("TableMiss").toString))) {
          var tableName:String = ""
          var columnNum:Int = 0
          var columns:String = ""
          var storage:String = ""
          var store_type:String = ""
          var del:String = ""
          tableName += tableNode.attribute("name").getOrElse("TableMiss").toString
          storage += (tableNode \ "storage").text
          for (store <- tableNode \ "storage") {
            del += store.attribute("del").getOrElse("\t").toString
            store_type += store.attribute("type").getOrElse("txt").toString
          }
          for (column <- tableNode \ "column") {
            columnNum += 1
            columns += column.text.trim + ":" + column.attribute("type").getOrElse("TypeMiss") + ","
          }
          /** Make sure the table has # of columns that's specified in the conf_xml file */
          if (!tableNode.attribute("columnNum").getOrElse("0").toString.equals(columnNum.toString)) {
            println("Table " + tableName + " column # does not match!")
            println("Actual column #: " + columnNum.toString)
            println("Expected column #: " + tableNode.attribute("columnNum").getOrElse("0").toString)
            System.exit(1)
          }
          tables.+=(tableName -> (columnNum,columns,storage,store_type,del))
        }
        tables
      case _ => new mutable.HashMap[String,(Int,String,String,String,String)]()
    }
  }

  /** Function name:createSchema
    *
    * @param schemaString
    * @return a table schema
    *         for now, only five data types are supported,
    *         i.e. Int, Double,Float,String,Date
    *         all other data types will be mapped to String type.
    */
  def createSchema(schemaString: String): StructType = StructType(
    schemaString.split(",").map {
      case fieldName_fieldType =>
        StructField(fieldName_fieldType.split(":").head,
          dataType = fieldName_fieldType.split(":").toSeq(1) match {
            case "int" => IntegerType
            case "double" => DoubleType
            case "float" => FloatType
            case "string" => StringType
            case "date" => DateType
            case _ => StringType
          },
          nullable = true)
    }
  )

}
