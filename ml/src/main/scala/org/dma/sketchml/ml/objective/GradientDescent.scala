package org.dma.sketchml.ml.objective

import org.apache.spark.ml.linalg.DenseVector
import org.dma.sketchml.ml.conf.MLConf
import org.dma.sketchml.ml.data.DataSet
import org.dma.sketchml.ml.gradient._
import org.slf4j.{Logger, LoggerFactory}

object GradientDescent {
  private val logger: Logger = LoggerFactory.getLogger(GradientDescent.getClass)

  def apply(conf: MLConf): GradientDescent =
    new GradientDescent(conf.featureNum, conf.learnRate, conf.learnDecay, conf.batchSpRatio)
}

class GradientDescent(dim: Int, lr_0: Double, decay: Double, batchSpRatio: Double) {
  protected val logger = GradientDescent.logger

  var epoch: Int = 0
  var batch: Int = 0
  val batchNum: Double = Math.ceil(1.0 / batchSpRatio).toInt

  def miniBatchGradientDescent(weight: DenseVector, dataSet: DataSet, loss: Loss): (Gradient, Int, Double, Double) = {
    val startTime = System.currentTimeMillis()

    val denseGrad = new DenseDoubleGradient(dim)
    var objLoss = 0.0
    val batchSize = (dataSet.size * batchSpRatio).toInt
    for (i <- 0 until batchSize) {
      val ins = dataSet.loopingRead
      val pre = loss.predict(weight, ins.feature)
      val gradScala = loss.grad(pre, ins.label)
      denseGrad.plusBy(ins.feature, -1.0 * gradScala)
      objLoss += loss.loss(pre, ins.label)
    }
    val grad = denseGrad.toAuto
    grad.timesBy(1.0 / batchSize)

    if (loss.isL1Reg)
      l1Reg(grad, 0, loss.getRegParam)
    if (loss.isL2Reg)
      l2Reg(grad, weight, loss.getRegParam)
    val regLoss = loss.getReg(weight)

    logger.info(s"Epoch[$epoch] batch $batch gradient " +
      s"cost ${System.currentTimeMillis() - startTime} ms, "
      + s"batch size=$batchSize, obj loss=${objLoss / batchSize}, reg loss=$regLoss")
    batch += 1
    if (batch == batchNum) { epoch += 1; batch = 0 }
    (grad, batchSize, objLoss, regLoss)
  }

  private def l1Reg(grad: Gradient, alpha: Double, theta: Double): Unit = {
    val values = grad match {
      case dense: DenseDoubleGradient => dense.values
      case sparse: SparseDoubleGradient => sparse.values
      case _ => throw new UnsupportedOperationException(
        s"Cannot regularize ${grad.kind} kind of gradients")
    }
    if (values != null) {
      for (i <- values.indices) {
        if (values(i) >= 0 && values(i) <= theta)
          values(i) = (values(i) - alpha) max 0
        else if (values(i) < 0 && values(i) >= -theta)
          values(i) = (values(i) - alpha) min 0
      }
    }
  }

  private def l2Reg(grad: Gradient, weight: DenseVector, lambda: Double): Unit = {
    val w = weight.values
    grad match {
      case dense: DenseDoubleGradient => {
        val v = dense.values
        for (i <- v.indices)
          v(i) += w(i) * lambda
      }
      case sparse: SparseDoubleGradient => {
        val k = sparse.indices
        val v = sparse.values
        for (i <- k.indices)
          v(i) += w(k(i)) * lambda
      }
      case _ => throw new UnsupportedOperationException(
        s"Cannot regularize ${grad.kind} kind of gradients")
    }
  }

  def update(grad: Gradient, weight: DenseVector): Unit = {
    val lr = lr_0 / Math.sqrt(1.0 + decay * epoch)
    grad match {
      case dense: DenseDoubleGradient => update(dense, weight, lr)
      case sparse: SparseDoubleGradient => update(sparse, weight, lr)
      case dense: DenseFloatGradient => update(dense, weight, lr)
      case sparse: SparseFloatGradient => update(sparse, weight, lr)
      case sketchGrad: SketchGradient => update(sketchGrad.toAuto, weight)
    }
  }

  private def update(grad: DenseDoubleGradient, weight: DenseVector, lr: Double): Unit = {
    val g = grad.values
    val w = weight.values
    for (i <- w.indices)
      w(i) -= g(i) * lr
  }

  private def update(grad: SparseDoubleGradient, weight: DenseVector, lr: Double): Unit = {
    val k = grad.indices
    val v = grad.values
    val w = weight.values
    for (i <- k.indices)
      w(k(i)) -= v(i) * lr
  }

  private def update(grad: DenseFloatGradient, weight: DenseVector, lr: Double): Unit = {
    val g = grad.values
    val w = weight.values
    for (i <- w.indices)
      w(i) -= g(i) * lr
  }

  private def update(grad: SparseFloatGradient, weight: DenseVector, lr: Double): Unit = {
    val k = grad.indices
    val v = grad.values
    val w = weight.values
    for (i <- k.indices)
      w(k(i)) -= v(i) * lr
  }

}
