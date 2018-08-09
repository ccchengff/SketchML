package org.dma.sketchml.ml.gradient

import org.dma.sketchml.ml.gradient.Kind.Kind
import org.dma.sketchml.ml.util.Maths
import org.dma.sketchml.sketch.base.SketchMLException

object SparseFloatGradient {
  def apply(grad: Gradient): SparseFloatGradient = {
    val (indices, values) = grad.kind match {
      case Kind.DenseDouble => {
        val dense = grad.asInstanceOf[DenseDoubleGradient]
        ((0 until dense.dim).toArray, dense.values)
      }
      case Kind.SparseDouble => {
        val sparse = grad.asInstanceOf[SparseDoubleGradient]
        (sparse.indices, sparse.values)
      }
      case _ => throw new SketchMLException(s"Cannot create ${Kind.SparseFloat} from ${grad.kind}")
    }
    new SparseFloatGradient(grad.dim, indices, values.map(_.toFloat))
  }
}

class SparseFloatGradient(d: Int, val indices: Array[Int],
                          val values: Array[Float]) extends Gradient(d) {
  {
    require(indices.length == values.length,
      s"Sizes of indices and values not match: ${indices.length} & ${values.length}")
    require(indices.head >= 0, s"Negative index: ${indices.head}.")
    for (i <- 1 until indices.length)
      require(indices(i - 1) < indices(i), s"Indices are not strictly increasing")
    require(indices.last < dim, s"Index ${indices.last} out of bounds for gradient of dimension $dim")
  }

  override def timesBy(x: Double): Unit = {
    val x_ = x.toFloat
    for (i <- values.indices)
      values(i) *= x_
  }

  override def countNNZ: Int = {
    var nnz = 0
    for (i <- values.indices)
      if (Math.abs(values(i)) > Maths.EPS)
        nnz += 1
    nnz
  }

  override def toDense: DenseDoubleGradient = {
    val dense = new Array[Double](dim)
    for (i <- values.indices)
      if (Math.abs(values(i)) > Maths.EPS)
        dense(indices(i)) = values(i)
    new DenseDoubleGradient(dim, dense)
  }

  override def toSparse: SparseDoubleGradient =
    new SparseDoubleGradient(dim, indices, values.map(_.toDouble))

  override def toAuto: Gradient = {
    val nnz = countNNZ
    if (nnz > dim * 2 / 3) toDense else toSparse
  }

  override def kind: Kind = Kind.SparseFloat
}
