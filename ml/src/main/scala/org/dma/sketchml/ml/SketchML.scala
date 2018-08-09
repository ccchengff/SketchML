package org.dma.sketchml.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.dma.sketchml.ml.algorithm._
import org.dma.sketchml.ml.common.Constants
import org.dma.sketchml.ml.conf.MLConf

object SketchML {
  def main(args: Array[String]): Unit = {
    try {
      val sparkConf = new SparkConf().setAppName("SketchML")
      implicit val sc = SparkContext.getOrCreate(sparkConf)
      val mlConf = MLConf(sparkConf)
      val model = mlConf.algo match {
        case Constants.ML_LOGISTIC_REGRESSION => LRModel(mlConf)
        case Constants.ML_SUPPORT_VECTOR_MACHINE => SVMModel(mlConf)
        case Constants.ML_LINEAR_REGRESSION => LinearRegModel(mlConf)
        case _ => throw new UnknownError("Unsupported algorithm: " + mlConf.algo)
      }

      model.loadData()
      model.train()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      while (1 + 1 == 2) {}
    }

    // TODO: test data
  }

}
