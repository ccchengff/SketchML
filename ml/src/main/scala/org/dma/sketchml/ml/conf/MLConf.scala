package org.dma.sketchml.ml.conf

import org.apache.spark.SparkConf

object MLConf {
  val ML_ALGORITHM: String = "spark.sketchml.algo"
  val ML_INPUT_PATH: String = "spark.sketchml.input.path"
  val ML_INPUT_FORMAT: String = "spark.sketchml.input.format"
  //val ML_TEST_DATA_PATH: String = "spark.sketchml.test.path"
  //val ML_NUM_CLASS: String = "spark.sketchml.class.num"
  //val DEFAULT_ML_NUM_CLASS: Int = 2
  val ML_NUM_WORKER: String = "spark.sketchml.worker.num"
  val ML_NUM_FEATURE: String = "spark.sketchml.feature.num"
  val ML_VALID_RATIO: String = "spark.sketchml.valid.ratio"
  val DEFAULT_ML_VALID_RATIO: Double = 0.25
  val ML_EPOCH_NUM: String = "spark.sketchml.epoch.num"
  val DEFAULT_ML_EPOCH_NUM: Int = 100
  val ML_BATCH_SAMPLE_RATIO: String = "spark.sketchml.batch.sample.ratio"
  val DEFAULT_ML_BATCH_SAMPLE_RATIO: Double = 0.1
  val ML_LEARN_RATE: String = "spark.sketchml.learn.rate"
  val DEFAULT_ML_LEARN_RATE: Double = 0.1
  val ML_LEARN_DECAY: String = "spark.sketchml.learn.decay"
  val DEFAULT_ML_LEARN_DECAY: Double = 0.9
  val ML_REG_L1: String = "spark.sketchml.reg.l1"
  val DEFAULT_ML_REG_L1: Double = 0.1
  val ML_REG_L2: String = "spark.sketchml.reg.l2"
  val DEFAULT_ML_REG_L2: Double = 0.1

  def apply(sparkConf: SparkConf): MLConf = MLConf(
    sparkConf.get(ML_ALGORITHM),
    sparkConf.get(ML_INPUT_PATH),
    sparkConf.get(ML_INPUT_FORMAT),
    sparkConf.get(ML_NUM_WORKER).toInt,
    sparkConf.get(ML_NUM_FEATURE).toInt,
    sparkConf.getDouble(ML_VALID_RATIO, DEFAULT_ML_VALID_RATIO),
    sparkConf.getInt(ML_EPOCH_NUM, DEFAULT_ML_EPOCH_NUM),
    sparkConf.getDouble(ML_BATCH_SAMPLE_RATIO, DEFAULT_ML_BATCH_SAMPLE_RATIO),
    sparkConf.getDouble(ML_LEARN_RATE, DEFAULT_ML_LEARN_RATE),
    sparkConf.getDouble(ML_LEARN_DECAY, DEFAULT_ML_LEARN_DECAY),
    sparkConf.getDouble(ML_REG_L1, DEFAULT_ML_REG_L1),
    sparkConf.getDouble(ML_REG_L2, DEFAULT_ML_REG_L2)
  )

}

case class MLConf(algo: String, input: String, format: String, workerNum: Int,
                  featureNum: Int, validRatio: Double, epochNum: Int,batchSpRatio: Double,
                  learnRate: Double, learnDecay: Double, l1Reg: Double, l2Reg: Double) {

}

