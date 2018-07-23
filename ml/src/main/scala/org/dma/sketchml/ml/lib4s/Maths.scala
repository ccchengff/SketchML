package org.dma.sketchml.ml.lib4s

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}

import scala.collection.mutable.ArrayBuffer

object Maths {
  val EPS = 1e-8

  def add(k1: Array[Int], v1: Array[Double], k2: Array[Int],
          v2: Array[Double]): (Array[Int], Array[Double]) = {
    val k = ArrayBuffer[Int]()
    val v = ArrayBuffer[Double]()
    var i = 0
    var j = 0
    while (i < k1.length && j < k2.length) {
      if (k1(i) < k2(j)) {
        k += k1(i)
        v += v1(i)
        i += 1
      } else if (k1(i) > k2(j)) {
        k += k2(j)
        v += v2(j)
        j += 1
      } else {
        k += k1(i)
        v += v1(i) + v2(j)
        i += 1
        j += 1
      }
    }
    (k.toArray, v.toArray)
  }

  def dot(a: Vector, b: Vector): Double = {
    (a, b) match {
      case (a: DenseVector, b: DenseVector) => dot(a, b)
      case (a: DenseVector, b: SparseVector) => dot(a, b)
      case (a: SparseVector, b: DenseVector) => dot(a, b)
      case (a: SparseVector, b: SparseVector) => dot(a, b)
    }
  }

  def dot(a: DenseVector, b: DenseVector): Double = {
    require(a.size == b.size, s"Dot between vectors of size ${a.size} and ${b.size}")
    //(a.values, b.values).zipped.map(_*_).sum
    val size = a.size
    val aValues = a.values
    val bValues = b.values
    var dot = 0.0
    for (i <- 0 until size) {
      dot += aValues(i) * bValues(i)
    }
    dot
  }

  def dot(a: DenseVector, b: SparseVector): Double = {
    require(a.size == b.size, s"Dot between vectors of size ${a.size} and ${b.size}")
    val aValues = a.values
    val bIndices = b.indices
    val bValues = b.values
    val size = b.numActives
    var dot = 0.0
    for (i <- 0 until size) {
      val ind = bIndices(i)
      dot += aValues(ind) * bValues(i)
    }
    dot
  }

  def dot(a: SparseVector, b: DenseVector): Double = dot(b, a)

  def dot(a: SparseVector, b: SparseVector): Double = {
    require(a.size == b.size, s"Dot between vectors of size ${a.size} and ${b.size}")
    val aIndices = a.indices
    val aValues = a.values
    val aNumActives = a.numActives
    val bIndices = b.indices
    val bValues = b.values
    val bNumActives = b.numActives
    var aOff = 0
    var bOff = 0
    var dot = 0.0
    while (aOff < aNumActives && bOff < bNumActives) {
      if (aIndices(aOff) < bIndices(bOff)) {
        aIndices(aOff) += 1
      } else if (aIndices(aOff) > bIndices(bOff)) {
        bOff += 1
      } else {
        dot += aValues(aOff) * bValues(bOff)
        aOff += 1
        bOff += 1
      }
    }
    dot
  }

}
