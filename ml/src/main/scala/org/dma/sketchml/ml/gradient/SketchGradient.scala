package org.dma.sketchml.ml.gradient

import org.dma.sketchml.ml.gradient.Kind.Kind
import org.dma.sketchml.sketch.base.SketchMLException
import org.dma.sketchml.sketch.quantization.QuantileQuantizer
import org.dma.sketchml.sketch.sketch.frequency.GroupedMinMaxSketch

class SketchGradient(d: Int, binNum: Int, groupNum: Int, rowNum: Int, colRatio: Double) extends Gradient(d) {

  def this(grad: Gradient, binNum: Int, groupNum: Int, rowNum: Int, colRatio: Double) {
    this(grad.dim, binNum, groupNum, rowNum, colRatio)
    grad.kind match {
      case Kind.DenseDouble => fromDense(grad.asInstanceOf[DenseDoubleGradient])
      case Kind.SparseDouble => fromSparse(grad.asInstanceOf[SparseDoubleGradient])
      case _ => throw new SketchMLException(s"Cannot create ${this.kind} from ${grad.kind}")
    }
  }

  private var nnz: Int = 0
  var bucketValues: Array[Double] = _
  var bins: Array[Int] = _
  var sketch: GroupedMinMaxSketch = _

  def fromDense(dense: DenseDoubleGradient): Unit = {
    val values = dense.values
    val quantizer = new QuantileQuantizer(binNum)
    quantizer.quantize(values)
    //quantizer.parallelQuantize(values)
    bucketValues = quantizer.getValues
    bins = quantizer.getBins
    sketch = null
    nnz = dim
  }

  def fromSparse(sparse: SparseDoubleGradient): Unit = {
    // 1. quantize into bin indexes
    val quantizer = new QuantileQuantizer(binNum)
    quantizer.quantize(sparse.values)
    //quantizer.parallelQuantize(sparse.values)
    bucketValues = quantizer.getValues
    // 2. encode bins and keys
    sketch = new GroupedMinMaxSketch(groupNum, rowNum, colRatio, binNum, quantizer.getZeroIdx)
    sketch.create(sparse.indices, quantizer.getBins)
    bins = null
    //sketch.parallelCreate(sparse.indices, quantizer.getBins)
    // 3. set nnz
    nnz = sparse.indices.length
  }

  override def timesBy(x: Double): Unit = {
    for (i <- bucketValues.indices)
      bucketValues(i) *= x
  }

  override def countNNZ: Int = nnz

  override def toDense: DenseDoubleGradient = {
    val values = bins.map(bin => bucketValues(bin))
    new DenseDoubleGradient(dim, values)
  }

  override def toSparse: SparseDoubleGradient = {
    val kb = sketch.restore()
    val indices = kb.getLeft
    val bins = kb.getRight
    val values = bins.map(bin => bucketValues(bin))
    new SparseDoubleGradient(dim, indices, values)
  }

  override def toAuto: Gradient = (if (bins != null) toDense else toSparse).toAuto

  override def kind: Kind = Kind.Sketch
}

