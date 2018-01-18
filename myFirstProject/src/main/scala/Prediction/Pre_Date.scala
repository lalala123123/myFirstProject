package Prediction

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object Pre_Date {

  def get_date(sqlContext:SQLContext,months:List[String]): DataFrame ={
    val prop = new java.util.Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")

    val user_df = sqlContext.read.jdbc("jdbc:mysql://localhost:3306/Terminal","user",prop)
    val month_df = sqlContext.read.jdbc("jdbc:mysql://localhost:3306/Terminal","user_month",prop)
    val cell_phone = sqlContext.read.jdbc("jdbc:mysql://localhost:3306/Terminal","cell_phone",prop)

    var train_data:RDD[Row] = null

    for(month <- months){
      val now_month_df = month_df.where("month = '"+month+"'")
      var last_month = month_df.select("IMSI","date","call_duration","msg_num","ARPU","brand","model").where("month = '"+month_del(month)+"'").withColumnRenamed("brand","pre_brand").withColumnRenamed("model","pre_model")
      var train_df = now_month_df.join(last_month.select("IMSI","date","call_duration","msg_num","pre_brand","pre_model","ARPU"),Seq("IMSI"),"left_outer")
      last_month = month_df.select("IMSI","date","call_duration","msg_num","ARPU").where("month = '"+month_del(month_del(month))+"'")
      train_df = train_df.join(last_month.select("IMSI","date","call_duration","msg_num","ARPU"),Seq("IMSI") ,"left_outer").join(user_df.select("IMSI","net","sex","age"),Seq("IMSI"),"left_outer")
      train_df = train_df.join(cell_phone.select("brand","model","create_time"),Seq("brand","model"),"left_outer")
      print(train_df.count())
      var rdd:RDD[Row] = train_df.rdd.map(row => create_Train_data(row,month))
      if(train_data == null){
        train_data = rdd
      }
      else {
        train_data = train_data ++ rdd
      }
    }
    val schema = StructType(
      Seq(
        StructField("month",StringType,true)
        ,StructField("IMSI",StringType,true)
        ,StructField("net",IntegerType,true)
        ,StructField("sex",IntegerType,true)
        ,StructField("age",IntegerType,true)
        ,StructField("data",IntegerType,true)
        ,StructField("call_duration",IntegerType,true)
        ,StructField("msg_num",IntegerType,true)
        ,StructField("ARPU",IntegerType,true)
        ,StructField("last_data",IntegerType,true)
        ,StructField("last_call_duration",IntegerType,true)
        ,StructField("last_msg_num",IntegerType,true)
        ,StructField("last_ARPU",IntegerType,true)
        ,StructField("pre_data",IntegerType,true)
        ,StructField("pre_call_duration",IntegerType,true)
        ,StructField("pre_msg_num",IntegerType,true)
        ,StructField("pre_ARPU",IntegerType,true)
        ,StructField("brand0",IntegerType,true)
        ,StructField("brand1",IntegerType,true)
        ,StructField("brand2",IntegerType,true)
        ,StructField("brand3",IntegerType,true)
        ,StructField("brand4",IntegerType,true)
        ,StructField("brand5",IntegerType,true)
        ,StructField("brand6",IntegerType,true)
        ,StructField("brand7",IntegerType,true)
        ,StructField("brand8",IntegerType,true)
        ,StructField("brand9",IntegerType,true)
        ,StructField("brand10",IntegerType,true)
        ,StructField("brand11",IntegerType,true)
        ,StructField("brand12",IntegerType,true)
        ,StructField("brand13",IntegerType,true)
        ,StructField("brand14",IntegerType,true)
        ,StructField("brand15",IntegerType,true)
        ,StructField("brand16",IntegerType,true)
        ,StructField("brand17",IntegerType,true)
        ,StructField("use_time",StringType,true)
        ,StructField("is_change",IntegerType,true)
      )
    )
    val data = sqlContext.createDataFrame(train_data,schema)
    data.show()
    return data
//    var train_df = month_df.join(user_df.select("IMSI","net","sex","age"),month_df("IMSI") === user_df("IMSI"),"left_outer")
// //   train_df = train_df.join(cell_phone.select("brand","model","create_time"),train_df("brand") === cell_phone("brand") && train_df("model") == cell_phone("model"),"left_outer")
//    create_Train_data(train_df.first(),"201502")
//    train_df.rdd.map(row => create_Train_data(row,month="201502"))
    //   print(train_df.collect(),train_df.count())
  }

  def create_Train_data(row:Row,month:String): Row ={
    print(row.get(0))
    var result = List[Any](month)

    var brand:Array[Int] = new Array[Int](18)
    val seq = row.toSeq.toList
    val IMSI = seq(2)
    result = result :+ IMSI

    val net = seq(19)
    val sex = seq(20)
    val age = seq(21)
    result = result ++ List(net,sex,age)

    val data = seq(5)
    val call_duration = seq(6)
    val msg_num = seq(7)

    result = result ++ List(data ,call_duration ,msg_num ,seq(8))

    val last_data = seq(9)
    val last_call_duration = seq(10)
    val last_msg_num = seq(11)
    result = result ++ List(last_data ,last_call_duration ,last_msg_num ,seq(14))

    val pre_data = seq(15)
    val pre_call_duration = seq(16)
    val pre_msg_num = seq(17)
    result = result ++ List(pre_data ,pre_call_duration ,pre_msg_num ,seq(18))

    var brand1 = ""
    if(seq(0) != null && seq(0)!= "**"){
      brand1 = seq(0).toString
    }
    brand1 match{
      case "Apple" => brand(0) = 1
      case "Samsung" => brand(1) = 1
      case "Nokia" => brand(2) = 1
      case "Xiaomi" => brand(3) = 1
      case "Lenovo" => brand(4) = 1
      case "HUAWEI" => brand(5) = 1
      case "HTC" => brand(6) = 1
      case "Coolpad" => brand(7) = 1
      case "OPPO" => brand(8) = 1
      case "BBK" => brand(9) = 1
      case "ZTE" => brand(10) = 1
      case "Gionee" => brand(11) = 1
      case "Motorola" => brand(12) = 1
      case "K-Touch" => brand(13) = 1
      case "Sony" => brand(14) = 1
      case "LG" => brand(15) = 1
      case "SonyEricsson" => brand(16) = 1
      case "Meizu" => brand(17) = 1
      case _ => 0
    }

    result = result ++ brand

    var createTime = ""
    if(seq(22) != null){
      createTime = seq(22).toString
    }
    val use_time = cal_month(createTime,month)
    result = result :+ use_time

    var is_change = 0
    if(seq(12).toString.length > 0 && seq(13).toString.length > 0 && (!seq(0).equals(seq(12)) || !seq(1).equals(seq(13)))){
      is_change = 1
    }
    result = result :+ is_change

    return Row.fromSeq(result)
    //return Row()//month,IMSI,net,age,ARPU,data,call_duration,msg_num,last_data,last_call_duration,last_msg_num,pre_data,pre_call_duration,pre_msg_num)
  }

  def month_del(month:String): String ={
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyyMM");
    val b = df.parse(month)
    cal.setTime(b)
    cal.add(Calendar.MONTH,-1)
    return df.format(cal.getTime)
  }

  def cal_month(product_month:String,now_month:String): String ={
    if(product_month.length < now_month.length){
      return ""
    }
    val sdf = new SimpleDateFormat("yyyyMM")
    print(sdf.parse(now_month).getTime,sdf.parse(product_month).getTime)
    val result = (sdf.parse(now_month).getTime/1000-sdf.parse(product_month).getTime/1000)/(3600*24*30)
    if(result < 0){
      return "";
    }
    return result.toString;
  }
}
