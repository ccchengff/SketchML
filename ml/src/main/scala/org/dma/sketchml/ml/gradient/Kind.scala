package org.dma.sketchml.ml.gradient

object Kind extends Enumeration {
  type Kind = Value
  val ZeroGradient, DenseDouble, SparseDouble, DenseFloat, SparseFloat, Sketch, FixedPoint = Value
}
