package org.dma.sketchml.ml.gradient

import javax.inject.Singleton
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.dma.sketchml.ml.gradient.Kind.Kind

object Gradient {
  def zero: ZeroGradient = ZeroGradient.getInstance()

  def transform(grad: Gradient): Gradient = {
    grad.toSparse
  }
}

abstract class Gradient(protected val dim: Int) extends Serializable {
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
        case Kind.SparseSortedDouble => plusBy(o.asInstanceOf[SparseSortedDoubleGradient])
        case _ => throw new ClassNotFoundException(o.getClass.getName)
      }
    }
  }

  def plusBy(dense: DenseDoubleGradient): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add ${dense.kind} to ${this.kind}")

  def plusBy(sparse: SparseDoubleGradient): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add ${sparse.kind} to ${this.kind}")

  def plusBy(sparseSorted: SparseSortedDoubleGradient): Gradient = throw new
      UnsupportedOperationException(s"Cannot to add ${sparseSorted.kind} to ${this.kind}")

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

  def toSparseSorted: SparseSortedDoubleGradient

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

  override def toSparseSorted: SparseSortedDoubleGradient = ???

  override def toAuto: Gradient = ???

  override def kind: Kind = Kind.ZeroGradient

  override def plusBy(dense: DenseDoubleGradient): Gradient = dense

  override def plusBy(sparse: SparseDoubleGradient): Gradient = sparse

  override def plusBy(sparseSorted: SparseSortedDoubleGradient): Gradient = sparseSorted
}

