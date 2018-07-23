package org.dma.sketchml.ml.objective

import org.apache.spark.ml.linalg.DenseVector
import org.dma.sketchml.ml.conf.MLConf
import org.dma.sketchml.ml.gradient.{DenseDoubleGradient, Gradient, SparseDoubleGradient, SparseSortedDoubleGradient}
import org.dma.sketchml.ml.util.Maths
import org.slf4j.{Logger, LoggerFactory}

object Adam {
  private val logger: Logger = LoggerFactory.getLogger(Adam.getClass)

  def apply(conf: MLConf): GradientDescent =
    new Adam(conf.featureNum, conf.learnRate, conf.learnDecay, conf.batchSpRatio)
}

class Adam(dim: Int, lr_0: Double, decay: Double, batchSpRatio: Double)
  extends GradientDescent(dim, lr_0, decay, batchSpRatio) {

  override protected val logger = Adam.logger

  val beta1 = 0.9
  val beta2 = 0.999
  var beta1_t = 0.9
  var beta2_t = 0.999
  val m = new Array[Double](dim)
  val v = new Array[Double](dim)

  override def update(grad: Gradient, weight: DenseVector): Unit = {
    if (epoch > 0 && batch == 0) {
      beta1_t *= beta1
      beta2_t *= beta2
    }

    grad match {
      case dense: DenseDoubleGradient => update(dense, weight, lr_0)
      case sparse: SparseDoubleGradient => update(sparse, weight, lr_0)
      case sparseSorted: SparseSortedDoubleGradient => update(sparseSorted, weight, lr_0)
    }
  }

  private def update(grad: DenseDoubleGradient, weight: DenseVector, lr: Double): Unit = {
    val g = grad.values
    val w = weight.values
    for (i <- w.indices) {
      val m_t = beta1 * m(i) + (1 - beta1) * g(i)
      val v_t = beta2 * v(i) + (1 - beta2) * g(i) * g(i)
      val newGrad = (Math.sqrt(1 - beta2_t) * m_t) / ((1 - beta1_t) * (Math.sqrt(v_t) + Maths.EPS))
      w(i) -= newGrad * lr
      m(i) = m_t
      v(i) = v_t
    }
  }

  private def update(grad: SparseDoubleGradient, weight: DenseVector, lr: Double): Unit = {
    val iterator = grad.hashmap.int2DoubleEntrySet().fastIterator()
    val w = weight.values
    while (iterator.hasNext) {
      val entry = iterator.next()
      val dim  = entry.getIntKey
      val grad = entry.getDoubleValue
      val m_t = beta1 * m(dim) + (1 - beta1) * grad
      val v_t = beta2 * v(dim) + (1 - beta2) * grad * grad
      val newGrad = (Math.sqrt(1 - beta2_t) * m_t) / ((1 - beta1_t) * (Math.sqrt(v_t) + Maths.EPS))
      w(dim) -= newGrad * lr
      m(dim) = m_t
      v(dim) = v_t
    }
  }

  private def update(grad: SparseSortedDoubleGradient, weight: DenseVector, lr: Double): Unit = {
    val k = grad.indices
    val g = grad.values
    val w = weight.values
    for (i <- k.indices) {
      val m_t = beta1 * m(i) + (1 - beta1) * g(i)
      val v_t = beta2 * v(i) + (1 - beta2) * g(i) * g(i)
      val newGrad = (Math.sqrt(1 - beta2_t) * m_t) / ((1 - beta1_t) * (Math.sqrt(v_t) + Maths.EPS))
      w(i) -= newGrad * lr
      m(i) = m_t
      v(i) = v_t
    }
  }

}
