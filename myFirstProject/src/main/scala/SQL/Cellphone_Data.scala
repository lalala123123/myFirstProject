package SQL

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Cellphone_Data {

  def insert_cellphone(sqlContext: SQLContext,prop:Properties,path:String): Unit ={
    var df = sqlContext.load("com.databricks.spark.csv", Map("path" -> path, "header" -> "false")).withColumnRenamed("_c0","brand").withColumnRenamed("_c1","model").withColumnRenamed("_c3","create_time")
    val schema = StructType(
      Seq(
        StructField("brand",StringType,true)
        ,StructField("model",StringType,true)
        ,StructField("create_time",StringType,true)
      )
    )

    JdbcUtils.jdbc("jdbc:mysql://localhost:3306/Terminal",df.select("brand","model","create_time"),"cell_phone",prop)
    //.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/Terminal","cell_phone",prop)

  }

  //爬取手机数据
  def insert_cellphone(sqlContext: SQLContext,prop:Properties,brand:String,model:String): Unit ={

  }
}
