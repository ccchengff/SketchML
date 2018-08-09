package org.dma.sketchml.ml.gradient

import javax.inject.Singleton
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.dma.sketchml.ml.common.Constants
import org.dma.sketchml.ml.conf.MLConf
import org.dma.sketchml.ml.gradient.Kind.Kind
import org.dma.sketchml.sketch.base.SketchMLException
import org.slf4j.{Logger, LoggerFactory}

object Gradient {
  def zero: ZeroGradient = ZeroGradient.getInstance()

  private def logger: Logger = LoggerFactory.getLogger(Gradient.getClass)

  def compress(grad: Gradient, conf: MLConf): Gradient = {
    val startTime = System.currentTimeMillis()
    val res = conf.compressor match {
      case Constants.GRADIENT_COMPRESSOR_SKETCH =>
        new SketchGradient(grad, conf.quantBinNum, conf.sketchGroupNum,
          conf.sketchRowNum, conf.sketchColRatio)
      case Constants.GRADIENT_COMPRESSOR_FLOAT =>
        grad.kind match {
          case Kind.DenseDouble => new DenseFloatGradient(grad)
          case Kind.SparseDouble => SparseFloatGradient(grad)
        }
      case Constants.GRADIENT_COMPRESSOR_NONE => grad
      case _ => throw new SketchMLException(
        "Unrecognizable compressor: " + conf.compressor)
    }
    logger.info(s"Gradient compression from ${grad.kind} to ${res.kind} cost " +
      s"${System.currentTimeMillis() - startTime} ms")
    res
  }

  def sum(dim: Int, grads: Array[Gradient]): Gradient = {
    require(!grads.exists(_.dim != dim))
    val sum = new DenseDoubleGradient(dim)
    grads.foreach(sum.plusBy)
    sum.toAuto
  }
}

abstract class Gradient(val dim: Int) extends Serializable {
  require(dim > 0, s"Dimension is non-positive: $dim")

  def plusBy(o: Gradient): Gradient = {
    if (o.kind == Kind.ZeroGradient)
      this
    else {
      require(dim == o.dim, s"Adding gradients with " +
        s"different dimensions: $dim, ${o.dim}")
      o.kind match {
        case Kind.DenseDouble => plusBy(o.asInstanceOf[DenseDoubleGradient])
        case Kind.SparseDouble => plusBy(o.asInstanceOf[SparseDoubleGradient])
        case Kind.DenseFloat => plusBy(o.asInstanceOf[DenseFloatGradient])
        case Kind.SparseFloat => plusBy(o.asInstanceOf[SparseFloatGradient])
        case Kind.Sketch => plusBy(o.asInstanceOf[SketchGradient])
        case _ => throw new ClassNotFoundException(o.getClass.getName)
      }
    }
  }

  def plusBy(dense: DenseDoubleGradient): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add ${dense.kind} to ${this.kind}")

  def plusBy(sparse: SparseDoubleGradient): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add ${sparse.kind} to ${this.kind}")

  def plusBy(dense: DenseFloatGradient): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add ${dense.kind} to ${this.kind}")

  def plusBy(sparse: SparseFloatGradient): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add ${sparse.kind} to ${this.kind}")

  def plusBy(sketchGrad: SketchGradient): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add ${sketchGrad.kind} to ${this.kind}")

  def plusBy(v: Vector, x: Double): Gradient = {
    v match {
      case dense: DenseVector => plusBy(dense, x)
      case sparse: SparseVector => plusBy(sparse, x)
    }
  }

  def plusBy(dense: DenseVector, x: Double): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add DenseVector to ${this.kind}")

  def plusBy(sparse: SparseVector, x: Double): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add SparseVector to ${this.kind}")

  def timesBy(x: Double)

  def countNNZ: Int

  def toDense: DenseDoubleGradient

  def toSparse: SparseDoubleGradient

  def toAuto: Gradient

  def kind: Kind

  def +=(o: Gradient): Gradient = plusBy(o)

}

/**
  * Singleton object for zero value of gradients
  */
@Singleton
object ZeroGradient {
  private val instance = new ZeroGradient()

  def getInstance(): ZeroGradient = instance
}

class ZeroGradient private extends Gradient(1) {
  override def plusBy(o: Gradient): Gradient = o

  override def timesBy(x: Double): Unit = {}

  override def countNNZ: Int = 0

  override def toDense: DenseDoubleGradient = ???

  override def toSparse: SparseDoubleGradient = ???

  override def toAuto: Gradient = ???

  override def kind: Kind = Kind.ZeroGradient

  override def plusBy(dense: DenseDoubleGradient): Gradient = dense

  override def plusBy(sparse: SparseDoubleGradient): Gradient = sparse

  override def plusBy(dense: DenseFloatGradient): Gradient = dense

  override def plusBy(sparse: SparseFloatGradient): Gradient = sparse

  override def plusBy(sketchGrad: SketchGradient): Gradient = sketchGrad


}

