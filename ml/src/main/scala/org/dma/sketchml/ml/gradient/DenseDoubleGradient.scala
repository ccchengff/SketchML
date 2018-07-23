package org.dma.sketchml.ml.gradient

import org.apache.spark.ml.linalg
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
    val iterator = sparse.hashmap.int2DoubleEntrySet().fastIterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      values(entry.getIntKey) += entry.getDoubleValue
    }
    this
  }

  override def plusBy(sparseSorted: SparseSortedDoubleGradient): Gradient = {
    val k = sparseSorted.indices
    val v = sparseSorted.values
    for (i <- k.indices)
      values(k(i)) += v(i)
    this
  }

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

  override def toSparse: SparseDoubleGradient = {
    val sparse = new SparseDoubleGradient(dim)
    for (i <- 0 until dim)
      if (Math.abs(values(i)) > Maths.EPS)
        sparse.hashmap.put(i, values(i))
    sparse
  }

  override def toSparseSorted: SparseSortedDoubleGradient = toSparseSorted(countNNZ)

  private def toSparseSorted(nnz: Int): SparseSortedDoubleGradient = {
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
    new SparseSortedDoubleGradient(dim, k, v)
  }

  override def toAuto: Gradient = {
    val nnz = countNNZ
    if (nnz > dim * 2 / 3) toDense else toSparseSorted(nnz)
  }

  override def kind: Kind = Kind.DenseDouble

}
