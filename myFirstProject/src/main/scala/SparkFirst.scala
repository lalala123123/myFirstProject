import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通话记录日志解析
  */
object SparkFirst {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FirstSpark").setMaster("local")//.setMaster("spark://hadoop:7077")//
    val sc = new SparkContext(conf)
    val call,sms = readFile("/home/hadoop/terminal",sc)

  }

  def readFile(str: String, sc: SparkContext){
    //val sc:SparkContext = session.sparkContext
    val text = sc.textFile(str)
    print(text.count())
    val call = text.map(line=>line.split('|')).filter(x => x(2) == "A1CALL").map(x=> new Terminal_Date(x(1),1,x(5),x(6),x(11),x(12)))
    val sms = text.map(line=>line.split('|')).filter(x => x(2) == "A1SMS").map(x=> new Terminal_Date(x(1),1,x(5),x(6),x(11),x(12)))
    return (call,sms)
  }
}
