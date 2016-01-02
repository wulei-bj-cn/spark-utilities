package routines

import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * Created by wulei7 on 2016/1/1.
 * Self-defined Functions.
 */
object func {

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
        for (tableNode @ <table>{_*}</table> <- tablesNode.filter(t => filterString.contains(t.attribute("name").getOrElse("TableMiss").toString))) {
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
          if(store_type.equals("txt")) {
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
