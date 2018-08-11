package org.dma.sketchml.ml.gradient

import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.dma.sketchml.ml.gradient.Kind.Kind
import org.dma.sketchml.ml.util.Maths

class DenseDoubleGradient(d: Int, val values: Array[Double]) extends Gradient(d) {
  def this(d: Int) = this(d, new Array[Double](d))

  override def plusBy(dense: DenseDoubleGradient): Gradient = {
    for (i <- 0 until dim)
      values(i) += dense.values(i)
    this
  }

  override def plusBy(sparse: SparseDoubleGradient): Gradient = {
    val k = sparse.indices
    val v = sparse.values
    for (i <- k.indices)
      values(k(i)) += v(i)
    this
  }

  override def plusBy(dense: DenseFloatGradient): Gradient = {
    for (i <- 0 until dim)
      values(i) += dense.values(i)
    this
  }

  override def plusBy(sparse: SparseFloatGradient): Gradient = {
    val k = sparse.indices
    val v = sparse.values
    for (i <- k.indices)
      values(k(i)) += v(i)
    this
  }

  override def plusBy(sketchGrad: SketchGradient): Gradient = plusBy(sketchGrad.toAuto)

  override def plusBy(fpGrad: FixedPointGradient): Gradient = plusBy(fpGrad.toAuto)

  override def plusBy(dense: DenseVector, x: Double): Gradient = {
    val v = dense.values
    for (i <- 0 until dim)
      values(i) += v(i) * x
    this
  }

  override def plusBy(sparse: SparseVector, x: Double): Gradient = {
    val k = sparse.indices
    val v = sparse.values
    for (i <- k.indices)
      values(k(i)) += v(i) * x
    this
  }

  override def timesBy(x: Double): Unit = {
    for (i <- 0 until dim)
      values(i) *= x
  }

  override def countNNZ: Int = {
    var nnz = 0
    for (i <- 0 until dim)
      if (Math.abs(values(i)) > Maths.EPS)
        nnz += 1
    nnz
  }

  override def toDense: DenseDoubleGradient = this

  override def toSparse: SparseDoubleGradient = toSparse(countNNZ)

  private def toSparse(nnz: Int): SparseDoubleGradient = {
    val k = new Array[Int](nnz)
    val v = new Array[Double](nnz)
    var i = 0
    var j = 0
    while (i < dim && j < nnz) {
      if (Math.abs(values(i)) > Maths.EPS) {
        k(j) = i
        v(j) = values(i)
        j += 1
      }
      i += 1
    }
    new SparseDoubleGradient(dim, k, v)
  }

  override def toAuto: Gradient = {
    val nnz = countNNZ
    if (nnz > dim * 2 / 3) toDense else toSparse(nnz)
  }

  override def kind: Kind = Kind.DenseDouble

}
