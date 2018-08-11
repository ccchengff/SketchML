package org.dma.sketchml.ml.gradient
import java.util

import breeze.stats.distributions.Bernoulli
import org.dma.sketchml.ml.gradient.Kind.Kind
import org.dma.sketchml.sketch.base.SketchMLException
import org.dma.sketchml.sketch.binary.BinaryUtils

object FixedPointGradient {
  private val bernoulli = new Bernoulli(0.5)
}

class FixedPointGradient(d: Int, val numBits: Int) extends Gradient(d) {
  import FixedPointGradient._

  require(numBits < 30, s"Bit num out of range: $numBits")

  def this(grad: Gradient, numBits: Int) {
    this(grad.dim, numBits)
    grad.kind match {
      case Kind.DenseDouble => fromDense(grad.asInstanceOf[DenseDoubleGradient])
      case Kind.SparseDouble => fromSparse(grad.asInstanceOf[SparseDoubleGradient])
      case _ => throw new SketchMLException(s"Cannot create ${this.kind} from ${grad.kind}")
    }
  }

  var size: Int = _
  var norm: Double = _
  var indices: Array[Int] = _
  var bitset: util.BitSet = _

  def fromDense(dense: DenseDoubleGradient): Unit = fromArray(dense.values)

  def fromSparse(sparse: SparseDoubleGradient): Unit = {
    indices = sparse.indices
    fromArray(sparse.values)
  }

  private def fromArray(values: Array[Double]): Unit = {
    size = values.length
    norm = 0.0
    values.foreach(v => norm += v * v)
    norm = Math.sqrt(norm)
    bitset = new util.BitSet(numBits * size)
    val max = (1 << (numBits - 1)) - 1
    val sign = 1 << (numBits - 1)
    for (i <- values.indices) {
      val sigma = if (bernoulli.draw()) 1 else 0
      var x = Math.floor(Math.abs(values(i)) / norm * max).toInt + sigma
      if (values(i) < 0) x |= sign
      BinaryUtils.setBits(bitset, i * numBits, x, numBits)
    }
  }

  override def timesBy(x: Double): Unit = norm *= x

  override def countNNZ: Int = size

  override def toDense: DenseDoubleGradient = new DenseDoubleGradient(dim, toArray)

  override def toSparse: SparseDoubleGradient = new SparseDoubleGradient(dim, indices, toArray)

  private def toArray: Array[Double] = {
    val values = new Array[Double](size)
    val max = (1 << (numBits - 1)) - 1
    val mask = max
    val sign = 1 << (numBits - 1)
    for (i <- 0 until size) {
      val x = BinaryUtils.getBits(bitset, i * numBits, numBits)
      var v = (x & mask).toDouble / max * norm
      if ((x & sign) != 0) v = -v
      values(i) = v
    }
    values
  }

  override def toAuto: Gradient = if (indices == null) toDense else toSparse

  override def kind: Kind = Kind.FixedPoint
}
