package SQL
import java.io.File
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import Prediction.{CreateModel, Pre_Date}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.zookeeper.CreateMode
object Read_CSV {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder().master("local").appName("FirstSpark").getOrCreate()
    //val conf = new SparkConf().setAppName("FirstSpark").setMaster("local")
    //.setMaster("spark://hadoop:7077")//

    //val sc = new SparkContext(conf)
    var sqlContext: SQLContext = sparkSession.sqlContext//new SQLContext(sc)

    //手机数据存入mysql
/**
    val prop = new java.util.Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    var file:File = new File("./Cellphone")
    for(f <-file.listFiles()) {
      print(f.getAbsolutePath)
      Cellphone_Data.insert_cellphone(sqlContext, prop, f.getAbsolutePath)
    }
**/
    CreateModel.createModel(sparkSession,Pre_Date.get_date(sqlContext,List("201503")),Pre_Date.get_date(sqlContext,List("201504")))
    //用于读取csv

/**
    for(a <- 1 to 8) {

      print("./Data/20150"+(a+"")+"_1.csv")
      var df = sqlContext.load("com.databricks.spark.csv", Map("path" -> ("./Data/20150"+(a+"")+"_1.csv"), "header" -> "true"))
      df = df.withColumnRenamed("月份", "month").withColumnRenamed("网别", "net").withColumnRenamed("性别", "sex").withColumnRenamed("年龄值段", "age").withColumnRenamed("ARPU值段", "ARPU").withColumnRenamed("终端品牌", "brand").withColumnRenamed("终端型号", "model").withColumnRenamed("流量使用量", "date").withColumnRenamed("语音通话时长", "call").withColumnRenamed("短信条数", "msg")
      df.show()


      //修改数据
      //用于存储用户数据和和月数据
      val prop = new java.util.Properties()
      prop.setProperty("user", "root")
      prop.setProperty("password", "root")

      val pre_user_df = sqlContext.read.jdbc("jdbc:mysql://localhost:3306/Terminal", "user", prop)
      insert_user_month(df, pre_user_df, sqlContext, prop)
      insert_user(df, pre_user_df, sqlContext, prop)
      println(df.join(pre_user_df.select("IMSI", "brand", "model"), "IMSI").show())

    }
**/
    //System.out.println(rdd.first().getString(3))
    //print row1
    //row.show()
  }

  def insert_user(df:DataFrame,pre_user_df:DataFrame,sqlContext: SQLContext,prop:Properties): Unit ={

    val user_rdd = df.join(pre_user_df,df("IMSI")===pre_user_df("IMSI"),"left_outer").rdd.map(row=>user_info(row))
    println(user_rdd.count())
    val schema = StructType(
      Seq(
        StructField("IMSI",StringType,true)
        ,StructField("net",IntegerType,true)
        ,StructField("sex",IntegerType,true)
        ,StructField("age",IntegerType,true)
        ,StructField("brand",StringType,true)
        ,StructField("model",StringType,true)
        ,StructField("is_change",IntegerType,true)
      )
    )
    val user_df = sqlContext.createDataFrame(user_rdd,schema).where("is_change = 1").select("IMSI","net","sex","age","brand","model")
    user_df.show()
    JdbcUtils.jdbc("jdbc:mysql://localhost:3306/Terminal",user_df,"user",prop)
  }

  def insert_user_month(df:DataFrame,pre_user_df:DataFrame,sqlContext: SQLContext,prop:Properties): Unit ={
    val month_rdd = df.join(pre_user_df,df("IMSI")===pre_user_df("IMSI"),"left_outer").rdd.map(row=>month_info(row,pre_user_df))
    val schema_month = StructType(
      Seq(
        StructField("IMSI",StringType,true)
        ,StructField("month",StringType,true)
        ,StructField("brand",StringType,true)
        ,StructField("model",StringType,true)
        ,StructField("date",IntegerType,true)
        ,StructField("call_duration",IntegerType,true)
        ,StructField("msg_num",IntegerType,true)
        ,StructField("ARPU",IntegerType,true)
        //,StructField("is_change",IntegerType,true)
      )
    )
    val month_df = sqlContext.createDataFrame(month_rdd,schema_month)
    month_df.show()
    //JdbcUtils.jdbc("jdbc:mysql://localhost:3306/Terminal",month_df,"user",prop)
    val dataResult = month_df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/Terminal","user_month",prop)

  }


  def user_info(row:Row):Row= {
    var seq = row.toSeq.toList
    var month = seq(0).toString
    var IMSI = seq(1).toString
    var net = seq(2) match {
      case "2G" => 1
      case "3G" => 2
      case _ => 0
    }
    var sex = seq(3) match {
      case "男" => 0
      case "女" => 1
      case _ => 2
    }
    var age = seq(4) match {
      case "17岁以下" => 1
      case "18-22" => 2
      case "23-25" => 3
      case "26-29" => 4
      case "30-39" => 5
      case "40-49" => 6
      case "50-59" => 7
      case " 60以上" => 8
      case _ => 0
    }
    var brand = ""
    if(seq(6) != null){
      brand = seq(6).toString
    }
    var model = ""
    if(seq(7) != null){
      model = seq(7).toString
    }

    var brand1 = ""
    if(seq(14) != null){
      brand1 = seq(14).toString
    }
    var model1 = ""
    if(seq(15) != null){
      model1 = seq(15).toString
    }

    var is_change = 1
    if(brand1.equals("") && model1.equals("")) {
      is_change = 1
    }else if (brand1.equals(brand) && model1.equals(model) && seq(12) == net && seq(13) == sex && seq(16) == age){
      is_change = 0
    }

    return Row(IMSI, net, sex, age, brand, model, is_change)

  }

  def month_info(row:Row,pre_user_info:DataFrame):Row={
    var seq = row.toSeq.toList
    var month = seq(0).toString
    var IMSI = seq(1).toString
    var net = seq(2) match{
      case "2G" => 1
      case "3G" => 2
      case _ => 0
    }
    var sex = seq(3) match{
      case "男" => 0
      case "女" => 1
      case _ => 2
    }
    var age = seq(4) match{
      case "17岁以下" => 1
      case "18-22" => 2
      case "23-25" => 3
      case "26-29" => 4
      case "30-39" => 5
      case "40-49" => 6
      case "50-59" => 7
      case "60以上" => 8
      case _ => 0
    }
    var ARPU = seq(5) match{
      case "0-49" => 1
      case "50-99" => 2
      case "100-149" => 3
      case "150-199" => 4
      case "200-249" => 5
      case "250-299" => 6
      case "300及以上" => 7
      case _ => 0
    }
    var brand = ""
    if(seq(6) != null){
      brand = seq(6).toString
    }
    var model = ""
    if(seq(7) != null){
      model = seq(7).toString
    }

    var brand1 = ""
    if(seq(11) != null){
      brand1 = seq(11).toString
    }
    var model1 = ""
    if(seq(12) != null){
      model1 = seq(12).toString
    }
    var date = seq(8) match {
      case "0-499" => 1
      case "500-999" => 2
      case "1000-1499" => 3
      case "1500-1999" => 4
      case "2000-2499" => 5
      case "2500-2999" => 6
      case "3000-3499" => 7
      case "3500-3999" => 8
      case "4000-4499" => 9
      case "4500-4999" => 10
      case "5000以上" => 11
      case _ => 0
    }
    var call:Integer = Integer.parseInt(seq(9).toString())
    var msg:Integer = Integer.parseInt(seq(10).toString)
    var is_change = 0
    if(!(brand1.equals("") && model1.equals("")) && !(brand1.equals(brand) && model1.equals(model))){
      is_change = 1
    }


    return Row(IMSI,month,brand,model,date,call,msg,ARPU)

    //System.out.println(row.getString(1))
  }
}
