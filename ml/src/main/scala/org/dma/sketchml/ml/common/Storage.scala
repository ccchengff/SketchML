package org.dma.sketchml.ml.common

import org.apache.spark.ml.linalg.DenseVector
import org.dma.sketchml.ml.data.DataSet
import org.dma.sketchml.ml.gradient.Gradient
import org.dma.sketchml.ml.objective.{GradientDescent, Loss}

object Storage {
  object Model {
    var dim: Int = _
    var weights: DenseVector = _
    var optimizer: GradientDescent = _
    var loss: Loss = _
    var gradient: Gradient = _
  }

  object Data {
    var trainData: DataSet = _
    var validData: DataSet = _
  }

}
