package org.dma.sketchml.ml.algorithm

import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkEnv}
import org.dma.sketchml.ml.data.{DataSet, Parser}
import org.dma.sketchml.ml.common.Storage.Data._
import org.dma.sketchml.ml.common.Storage.Model._
import org.dma.sketchml.ml.conf.MLConf
import org.dma.sketchml.ml.gradient.{DenseDoubleGradient, Gradient, SparseDoubleGradient}
import org.dma.sketchml.ml.objective.{Adam, L2HingeLoss, L2LogLoss, L2SquareLoss}
import org.dma.sketchml.ml.util.ValidationUtil
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

abstract class GeneralizedLinearModel(@transient protected val conf: MLConf) extends Serializable {
  @transient protected val logger: Logger = LoggerFactory.getLogger(getName)

  @transient protected implicit val sc: SparkContext = SparkContext.getOrCreate()
  @transient protected var executors: RDD[Int] = _
  protected val bcConf = sc.broadcast(conf)

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
    val startTime = System.currentTimeMillis()
    initModel()

    val trainLosses = ArrayBuffer[Double](conf.epochNum)
    val validLosses = ArrayBuffer[Double](conf.epochNum)
    val timeElapsed = ArrayBuffer[Double](conf.epochNum)
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

    while (1 + 1 == 2) {}
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

  def computeGradient(epoch: Int, batch: Int): Double = {
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

  def aggregateAndUpdate(epoch: Int, batch: Int): Unit = {
    val aggrStart = System.currentTimeMillis()
    val grad = executors.aggregate(Gradient.zero.asInstanceOf[Gradient])(
      seqOp = (_, _) => Gradient.transform(gradient),
      combOp = (c1, c2) => c1 += c2
    )
    grad.timesBy(1.0 / conf.workerNum)
    logger.info(s"Epoch[$epoch] batch $batch aggregate gradients cost "
      + s"${System.currentTimeMillis() - aggrStart} ms")

    val updateStart = System.currentTimeMillis()
    val bcGrad = sc.broadcast(grad)
    executors.foreach(_ => optimizer.update(bcGrad.value, weights))
    logger.info(s"Epoch[$epoch] batch $batch update weights cost "
      + s"${System.currentTimeMillis() - updateStart} ms")
  }

  def validate(epoch: Int): Double = {
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


class LRModel(_conf: MLConf) extends GeneralizedLinearModel(_conf) {
  override protected def initModel(): Unit = {
    executors.foreach(_ => {
      dim = bcConf.value.featureNum
      weights = Vectors.dense(new Array[Double](dim)).asInstanceOf[DenseVector]
      optimizer = Adam(bcConf.value)
      loss = new L2LogLoss(bcConf.value.l2Reg)
    })
  }

  override def getName: String = "LRModel"
}


class SVMModel(_conf: MLConf) extends GeneralizedLinearModel(_conf) {
  override protected def initModel(): Unit = {
    executors.foreach(_ => {
      dim = bcConf.value.featureNum
      weights = Vectors.dense(new Array[Double](dim)).asInstanceOf[DenseVector]
      optimizer = Adam(bcConf.value)
      loss = new L2HingeLoss(bcConf.value.l2Reg)
    })
  }

  override def getName: String = "SVMModel"
}


class LinearRegModel(_conf: MLConf) extends GeneralizedLinearModel(_conf) {
  override protected def initModel(): Unit = {
    executors.foreach(_ => {
      dim = bcConf.value.featureNum
      weights = Vectors.dense(new Array[Double](dim)).asInstanceOf[DenseVector]
      optimizer = Adam(bcConf.value)
      loss = new L2SquareLoss(bcConf.value.l2Reg)
    })
  }

  override def getName: String = "LinearRegModel"
}

