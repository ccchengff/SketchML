package org.dma.sketchml.ml.gradient

import javax.inject.Singleton
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.dma.sketchml.ml.common.Constants
import org.dma.sketchml.ml.conf.MLConf
import org.dma.sketchml.ml.gradient.Kind.Kind
import org.dma.sketchml.ml.util.Maths
import org.dma.sketchml.sketch.base.SketchMLException
import org.dma.sketchml.sketch.util.Utils
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
      case Constants.GRADIENT_COMPRESSOR_FIXED_POINT =>
        new FixedPointGradient(grad, conf.fixedPointBitNum)
      case Constants.GRADIENT_COMPRESSOR_ZIP =>
        new ZipGradient(grad, conf.quantBinNum)
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
    // uncomment to evaluate the performance of compression
    //evaluateCompression(grad, res)
    res
  }

  def sum(dim: Int, grads: Array[Gradient]): Gradient = {
    require(!grads.exists(_.dim != dim))
    val sum = new DenseDoubleGradient(dim)
    grads.foreach(sum.plusBy)
    sum.toAuto
  }

  def evaluateCompression(origin: Gradient, comp: Gradient): Unit = {
    logger.info(s"Evaluating compression from ${origin.kind} to ${comp.kind}, " +
      s"sparsity[${origin.countNNZ.toDouble / origin.dim}]")
    // distances
    val (vOrig, vComp) = origin.kind match {
      case Kind.DenseDouble => (origin.asInstanceOf[DenseDoubleGradient].values, comp.toDense.values)
      case Kind.SparseDouble => (origin.asInstanceOf[SparseDoubleGradient].values, comp.toSparse.values)
    }
    logger.info(s"Distances: euclidean[${Maths.euclidean(vOrig, vComp)}], " +
      s"cosine[${Maths.cosine(vOrig, vComp)}]")
    // size
    val sizeOrig = Utils.sizeof(origin)
    val sizeComp = Utils.sizeof(comp)
    val rate = 1.0 * sizeOrig / sizeComp
    logger.info(s"Sizeof gradients: nnz[${vOrig.length}], " +
      s"origin[$sizeOrig bytes], comp[$sizeComp bytes], rate[$rate]")
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
        case Kind.FixedPoint => plusBy(o.asInstanceOf[FixedPointGradient])
        case Kind.Zip => plusBy(o.asInstanceOf[ZipGradient])
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

  def plusBy(fpGrad: FixedPointGradient): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add ${fpGrad.kind} to ${this.kind}")

  def plusBy(zipGrad: ZipGradient): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add ${zipGrad.kind} to ${this.kind}")

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

  override def plusBy(fpGrad: FixedPointGradient): Gradient = fpGrad

  override def plusBy(zipGrad: ZipGradient): Gradient = zipGrad

}

