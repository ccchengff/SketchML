package org.dma.sketchml.ml.data

import org.apache.spark.ml.linalg.Vector

case class LabeledData(label: Double, feature: Vector) {

  override def toString: String = s"($label $feature)"
}
