package org.dma.sketchml.ml.algorithm

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkEnv}
import org.dma.sketchml.ml.data.{DataSet, Parser}
import org.dma.sketchml.ml.conf.MLConf
import org.dma.sketchml.ml.gradient.Gradient
import org.dma.sketchml.ml.objective.{GradientDescent, Loss}
import org.dma.sketchml.ml.util.ValidationUtil
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object GeneralizedLinearModel {
  private val logger: Logger = LoggerFactory.getLogger(GeneralizedLinearModel.getClass)

  object Model {
    var weights: DenseVector = _
    var optimizer: GradientDescent = _
    var loss: Loss = _
    var gradient: Gradient = _
  }

  object Data {
    var trainData: DataSet = _
    var validData: DataSet = _
  }

}

import GeneralizedLinearModel.Model._
import GeneralizedLinearModel.Data._

abstract class GeneralizedLinearModel(@transient protected val conf: MLConf) extends Serializable {
  @transient protected val logger: Logger = GeneralizedLinearModel.logger

  @transient protected implicit val sc: SparkContext = SparkContext.getOrCreate()
  @transient protected var executors: RDD[Int] = _
  protected val bcConf: Broadcast[MLConf] = sc.broadcast(conf)

  def loadData(): Unit = {
    val startTime = System.currentTimeMillis()
    val dataRdd = Parser.loadData(conf.input, conf.format, conf.featureNum, conf.workerNum)
      .persist(StorageLevel.MEMORY_AND_DISK)
    executors = dataRdd.mapPartitionsWithIndex((partId, _) => {
      val exeId = SparkEnv.get.executorId match {
        case "driver" => partId
        case exeStr => exeStr.toInt
      }
      Seq(exeId).iterator
    }, preservesPartitioning = true)
    val (trainDataNum, validDataNum) = dataRdd.mapPartitions(iterator => {
      trainData = new DataSet
      validData = new DataSet
      while (iterator.hasNext) {
        if (Random.nextDouble() > bcConf.value.validRatio)
          trainData += iterator.next()
        else
          validData += iterator.next()
      }
      Seq((trainData.size, validData.size)).iterator
    }, preservesPartitioning = true)
      .reduce((c1, c2) => (c1._1 + c2._1, c1._2 + c2._2))
    //val rdds = dataRdd.randomSplit(Array(1.0 - validRatio, validRatio))
    //val trainRdd = rdds(0).persist(StorageLevel.MEMORY_AND_DISK)
    //val validRdd = rdds(1).persist(StorageLevel.MEMORY_AND_DISK)
    //val trainDataNum = trainRdd.count().toInt
    //val validDataNum = validRdd.count().toInt
    dataRdd.unpersist()
    logger.info(s"Load data cost ${System.currentTimeMillis() - startTime} ms, " +
      s"$trainDataNum train data, $validDataNum valid data")
  }

  protected def initModel(): Unit

  def train(): Unit = {
    logger.info(s"Start to train a $getName model")
    logger.info(s"Configuration: $conf")
    val startTime = System.currentTimeMillis()
    initModel()

    val trainLosses = ArrayBuffer[Double](conf.epochNum)
    val validLosses = ArrayBuffer[Double](conf.epochNum)
    val timeElapsed = ArrayBuffer[Long](conf.epochNum)
    val batchNum = Math.ceil(1.0 / conf.batchSpRatio).toInt
    for (epoch <- 0 until conf.epochNum) {
      logger.info(s"Epoch[$epoch] start training")
      trainLosses += trainOneEpoch(epoch, batchNum)
      validLosses += validate(epoch)
      timeElapsed += System.currentTimeMillis() - startTime
      logger.info(s"Epoch[$epoch] done, ${timeElapsed.last} ms elapsed")
    }

    logger.info(s"Train done, total cost ${System.currentTimeMillis() - startTime} ms")
    logger.info(s"Train loss: [${trainLosses.mkString(", ")}]")
    logger.info(s"Valid loss: [${validLosses.mkString(", ")}]")
    logger.info(s"Time: [${timeElapsed.mkString(", ")}]")
  }

  protected def trainOneEpoch(epoch: Int, batchNum: Int): Double = {
    val epochStart = System.currentTimeMillis()
    var trainLoss = 0.0
    for (batch <- 0 until batchNum) {
      val batchLoss = trainOneIteration(epoch, batch)
      trainLoss += batchLoss
    }
    val epochCost = System.currentTimeMillis() - epochStart
    logger.info(s"Epoch[$epoch] train cost $epochCost ms, loss=${trainLoss / batchNum}")
    trainLoss / batchNum
  }

  protected def trainOneIteration(epoch: Int, batch: Int): Double = {
    val batchStart = System.currentTimeMillis()
    val batchLoss = computeGradient(epoch, batch)
    aggregateAndUpdate(epoch, batch)
    logger.info(s"Epoch[$epoch] batch $batch train cost "
      + s"${System.currentTimeMillis() - batchStart} ms")
    batchLoss
  }

  protected def computeGradient(epoch: Int, batch: Int): Double = {
    val miniBatchGDStart = System.currentTimeMillis()
    val (batchSize, objLoss, regLoss) = executors.aggregate(0, 0.0, 0.0)(
      seqOp = (_, _) => {
        val (grad, batchSize, objLoss ,regLoss) =
          optimizer.miniBatchGradientDescent(weights, trainData, loss)
        gradient = grad
        (batchSize, objLoss, regLoss)
      },
      combOp = (c1, c2) => (c1._1 + c2._1, c1._2 + c2._2, c1._3 + c2._3)
    )
    val batchLoss = objLoss / batchSize + regLoss / conf.workerNum
    logger.info(s"Epoch[$epoch] batch $batch compute gradient cost "
      + s"${System.currentTimeMillis() - miniBatchGDStart} ms, "
      + s"batch size=$batchSize, batch loss=$batchLoss")
    batchLoss
  }

  protected def aggregateAndUpdate(epoch: Int, batch: Int): Unit = {
    val aggrStart = System.currentTimeMillis()
    val sum = Gradient.sum(
      conf.featureNum,
      executors.map(_ => Gradient.compress(gradient, bcConf.value)).collect()
    )
    val grad = Gradient.compress(sum, conf)
    grad.timesBy(1.0 / conf.workerNum)
    logger.info(s"Epoch[$epoch] batch $batch aggregate gradients cost "
      + s"${System.currentTimeMillis() - aggrStart} ms")

    val updateStart = System.currentTimeMillis()
    val bcGrad = sc.broadcast(grad)
    executors.foreach(_ => optimizer.update(bcGrad.value, weights))
    logger.info(s"Epoch[$epoch] batch $batch update weights cost "
      + s"${System.currentTimeMillis() - updateStart} ms")
  }

  protected def validate(epoch: Int): Double = {
    val validStart = System.currentTimeMillis()
    val (sumLoss, truePos, trueNeg, falsePos, falseNeg, validNum) =
      executors.aggregate((0.0, 0, 0, 0, 0, 0))(
        seqOp = (_, _) => ValidationUtil.calLossPrecision(weights, validData, loss),
        combOp = (c1, c2) => (c1._1 + c2._1, c1._2 + c2._2, c1._3 + c2._3,
          c1._4 + c2._4, c1._5 + c2._5, c1._6 + c2._6)
      )
    val validLoss = sumLoss / validNum
    val precision = 1.0 * (truePos + trueNeg) / validNum
    val trueRecall = 1.0 * truePos / (truePos + falseNeg)
    val falseRecall = 1.0 * trueNeg / (trueNeg + falsePos)
    logger.info(s"Epoch[$epoch] validation cost ${System.currentTimeMillis() - validStart} ms, "
      + s"valid size=$validNum, loss=$validLoss, precision=$precision, "
      + s"trueRecall=$trueRecall, falseRecall=$falseRecall")
    validLoss
  }

  def getName: String

}


