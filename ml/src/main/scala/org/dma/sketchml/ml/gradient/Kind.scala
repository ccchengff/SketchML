package org.dma.sketchml.ml.gradient

object Kind extends Enumeration {
  type Kind = Value
  val ZeroGradient, DenseDouble, SparseDouble, SparseSortedDouble, DenseFloat, SparseFloat, Sketch, Zip = Value
}
