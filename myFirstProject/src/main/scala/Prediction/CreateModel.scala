package Prediction

import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, Row}

object CreateModel {

  def createModel(train:DataFrame,test:DataFrame): Unit ={
    val numRound = 210

    //"objective" -> "reg:linear", //定义学习任务及相应的学习目标
    //"eval_metric" -> "rmse", //校验数据所需要的评价指标  用于做回归

    val train_data = train.map{ line =>
      val label = line(0).toString.toDouble
      val value0 = (1 to 16).map(i=>line(i).toString.toDouble)
      val featureVector = Vectors.dense(value0.toArray)
      LableledPo

    }

    val paramMap = List(
      "booster"->"gbtree",
    "objective"-> "rank:pairwise",
    "early_stopping_rounds"->210,
    "eval_metric"-> "auc",
    "max_depth"->3,
    "eta"-> 0.23 ,
    "seed"->10000,
    "nthread"->8    )
    val model = XGBoost.trainWithDataFrame(train_data, paramMap.toMap, numRound, 45)

    val predict = model.transform(test)

    val scoreAndLabels = predict.select(model.getPredictionCol, model.getLabelCol)
      .rdd
      .map { case Row(score: Double, label: Double) => (score, label) }

    //get the auc
    val metric = new BinaryClassificationMetrics(scoreAndLabels)
    val auc = metric.areaUnderROC()
    println("auc:" + auc)

  }

}
