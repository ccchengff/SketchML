package org.dma.sketchml.ml.gradient

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.dma.sketchml.ml.gradient.Kind.Kind
import org.dma.sketchml.ml.util.Maths
import org.dma.sketchml.sketch.util.Sort

class SparseDoubleGradient(d: Int, val hashmap: Int2DoubleOpenHashMap) extends Gradient(d) {
  def this(d: Int) = this(d, new Int2DoubleOpenHashMap())

  def this(d: Int, indices: Array[Int], values: Array[Double]) =
    this(d, new Int2DoubleOpenHashMap(indices, values))

  override def plusBy(sparse: SparseDoubleGradient): Gradient = {
    val iterator = sparse.hashmap.int2DoubleEntrySet().fastIterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      hashmap.addTo(entry.getIntKey, entry.getDoubleValue)
    }
    this
  }

  override def plusBy(sparseSorted: SparseSortedDoubleGradient): Gradient = {
    val k = sparseSorted.indices
    val v = sparseSorted.values
    for (i <- k.indices)
      hashmap.addTo(k(i), v(i))
    this
  }

  override def plusBy(sparse: SparseVector, x: Double): Gradient = {
    val k = sparse.indices
    val v = sparse.values
    for (i <- k.indices)
      hashmap.addTo(k(i), v(i) * x)
    this
  }

  override def timesBy(x: Double): Unit = {
    val iterator = hashmap.int2DoubleEntrySet().fastIterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      hashmap.put(entry.getIntKey, entry.getDoubleValue * x)
    }
  }

  override def countNNZ: Int = {
    var nnz = 0
    val iterator = hashmap.int2DoubleEntrySet().fastIterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      if (Math.abs(entry.getDoubleValue) > Maths.EPS)
        nnz += 1
    }
    nnz
  }

  override def toDense: DenseDoubleGradient = {
    val dense = new Array[Double](dim)
    val iterator = hashmap.int2DoubleEntrySet().fastIterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      if (Math.abs(entry.getDoubleValue) > Maths.EPS)
        dense(entry.getIntKey) = entry.getDoubleValue
    }
    new DenseDoubleGradient(dim, dense)
  }

  override def toSparse: SparseDoubleGradient = this

  override def toSparseSorted: SparseSortedDoubleGradient = toSparseSorted(countNNZ)

  private def toSparseSorted(nnz: Int): SparseSortedDoubleGradient = {
    val k = new Array[Int](nnz)
    val v = new Array[Double](nnz)
    var cnt = 0
    val iterator = hashmap.int2DoubleEntrySet().fastIterator()
    while (iterator.hasNext && cnt < nnz) {
      val entry = iterator.next()
      if (Math.abs(entry.getDoubleValue) > Maths.EPS) {
        k(cnt) = entry.getIntKey
        v(cnt) = entry.getDoubleValue
        cnt += 1
      }
    }
    Sort.quickSort(k, v, 0, nnz - 1)
    new SparseSortedDoubleGradient(dim, k, v)
  }

  override def toAuto: Gradient = {
    val nnz = countNNZ
    if (nnz > dim * 2 / 3) toDense else toSparse
  }

  override def kind: Kind = Kind.SparseDouble
}
