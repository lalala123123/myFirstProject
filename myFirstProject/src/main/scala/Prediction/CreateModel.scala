package Prediction

import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object CreateModel {

  def createModel(sparkSession:SparkSession,train:DataFrame,test:DataFrame): Unit ={
    val numRound = 210

    //"objective" -> "reg:linear", //定义学习任务及相应的学习目标
    //"eval_metric" -> "rmse", //校验数据所需要的评价指标  用于做回归



    import sparkSession.implicits._
    import org.apache.spark.ml.linalg.Vectors
    train.show()
    val train_data = train.rdd.map{ line =>
      val label = line(36).toString.toDouble
      val value0 = (2 to 35).map(i=>line(i).toString.toDouble)
      val featureVector = Vectors.dense(value0.toArray)
      LabeledPoint(label,features = featureVector)
    }
    val test_data = test.rdd.map{ line =>
      val label = line(36).toString.toDouble
      val value0 = (2 to 35).map(i=>line(i).toString.toDouble)
      val featureVector = Vectors.dense(value0.toArray)
      new DenseVector(value0.toArray)
    }

    val paramMap = List(
      "booster"->"gbtree",
    "objective"-> "rank:pairwise",
    "early_stopping_rounds"->1000,
    "eval_metric"-> "auc",
    "max_depth"->10,
    "eta"-> 0.23 ,
    "seed"->10000,
    "nthread"->1    )
    val model = XGBoost.trainWithRDD(train_data, paramMap.toMap, numRound, 1,missing = -1)

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
