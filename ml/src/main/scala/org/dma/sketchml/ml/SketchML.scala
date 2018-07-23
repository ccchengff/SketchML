package org.dma.sketchml.ml

import org.apache.spark.SparkConf
import org.dma.sketchml.ml.algorithm._
import org.dma.sketchml.ml.conf.MLConf

object SketchML {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SketchML")
    val mlConf = MLConf(sparkConf)
    val model = mlConf.algo.toUpperCase match {
      case "LR" => new LRModel(mlConf)
      case "SVM" => new SVMModel(mlConf)
      case "LinearReg" => new LinearRegModel(mlConf)
      case _ => throw new UnknownError("Unsupported algorithm: " + mlConf.algo)
    }

    model.loadData()
    model.train()

    // TODO: test data
  }

}
