package org.dma.sketchml.ml.gradient

import org.apache.spark.ml.linalg
import org.dma.sketchml.ml.gradient.Kind.Kind
import org.dma.sketchml.ml.util.Maths

class SparseSortedDoubleGradient(d: Int, val indices: Array[Int],
                                 val values: Array[Double]) extends Gradient(d) {
  {
    require(indices.length == values.length,
      s"Sizes of indices and values not match: ${indices.length} & ${values.length}")
    require(indices.head >= 0, s"Negative index: ${indices.head}.")
    for (i <- 1 until indices.length)
      require(indices(i - 1) < indices(i), s"Indices are not strictly increasing")
    require(indices.last < dim, s"Index ${indices.last} out of bounds for gradient of dimension $dim")
  }

  //override def plusBy(sparseSorted: SparseSortedDoubleGradient): Gradient = {
  //  val kv = Maths.add(this.indices, this.values, sparseSorted.indices, sparseSorted.values)
  //  new SparseSortedDoubleGradient(dim, kv._1, kv._2)
  //}

  override def timesBy(x: Double): Unit = {
    for (i <- values.indices)
      values(i) *= x
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
    new SparseDoubleGradient(dim, indices, values)

  override def toSparseSorted: SparseSortedDoubleGradient = this

  override def toAuto: Gradient = {
    val nnz = countNNZ
    if (nnz > dim * 2 / 3) toDense else toSparseSorted
  }

  override def kind: Kind = Kind.SparseSortedDouble
}
