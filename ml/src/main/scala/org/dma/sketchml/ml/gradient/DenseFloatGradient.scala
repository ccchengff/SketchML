package org.dma.sketchml.ml.gradient

import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.dma.sketchml.ml.gradient.Kind.Kind
import org.dma.sketchml.ml.util.Maths
import org.dma.sketchml.sketch.base.SketchMLException

class DenseFloatGradient(d: Int, val values: Array[Float]) extends Gradient(d) {
  def this(d: Int) = this(d, new Array[Float](d))

  def this(grad: Gradient) {
    this(grad.dim, new Array[Float](grad.dim))
    grad.kind match {
      case Kind.DenseDouble => fromDense(grad.asInstanceOf[DenseDoubleGradient])
      case Kind.SparseDouble => fromSparse(grad.asInstanceOf[SparseDoubleGradient])
      case _ => throw new SketchMLException(s"Cannot create ${this.kind} from ${grad.kind}")
    }
  }

  def fromDense(dense: DenseDoubleGradient): Unit = {
    val dv = dense.values
    for (i <- 0 until dim)
      values(i) = dv(i).toFloat
  }

  def fromSparse(sparse: SparseDoubleGradient): Unit = {
    val k = sparse.indices
    val v = sparse.values
    for (i <- k.indices)
      values(k(i)) = v(i).toFloat
  }

  override def plusBy(dense: DenseDoubleGradient): Gradient = {
    for (i <- 0 until dim)
      values(i) += dense.values(i).toFloat
    this
  }

  override def plusBy(sparse: SparseDoubleGradient): Gradient = {
    val k = sparse.indices
    val v = sparse.values
    for (i <- k.indices)
      values(k(i)) += v(i).toFloat
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

  override def plusBy(dense: DenseVector, x: Double): Gradient = {
    val v = dense.values
    val x_ = x.toFloat
    for (i <- 0 until dim)
      values(i) += v(i).toFloat * x_
    this
  }

  override def plusBy(sparse: SparseVector, x: Double): Gradient = {
    val k = sparse.indices
    val v = sparse.values
    val x_ = x.toFloat
    for (i <- k.indices)
      values(k(i)) += v(i).toFloat * x_
    this
  }

  override def timesBy(x: Double): Unit = {
    val x_ = x.toFloat
    for (i <- 0 until dim)
      values(i) *= x_
  }

  override def countNNZ: Int = {
    var nnz = 0
    for (i <- 0 until dim)
      if (Math.abs(values(i)) > Maths.EPS)
        nnz += 1
    nnz
  }

  override def toDense: DenseDoubleGradient =
    new DenseDoubleGradient(dim, values.map(_.toDouble))

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

  override def kind: Kind = Kind.DenseFloat

}
