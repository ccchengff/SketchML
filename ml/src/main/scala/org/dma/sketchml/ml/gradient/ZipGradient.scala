package org.dma.sketchml.ml.gradient

import java.util
import java.util.concurrent.ExecutionException

import org.dma.sketchml.ml.gradient.Kind.Kind
import org.dma.sketchml.sketch.base.{Quantizer, SketchMLException}
import org.dma.sketchml.sketch.util.Sort
import org.slf4j.{Logger, LoggerFactory}

class ZipGradient(d: Int, binNum: Int) extends Gradient(d) {
  private var size: Int = 0
  var indices: Array[Int] = _
  var quantizer: ZipMLQuantizer = _

  def this(grad: Gradient, binNum: Int) {
    this(grad.dim, binNum)
    grad.kind match {
      case Kind.DenseDouble => fromDense(grad.asInstanceOf[DenseDoubleGradient])
      case Kind.SparseDouble => fromSparse(grad.asInstanceOf[SparseDoubleGradient])
      case _ => throw new SketchMLException(s"Cannot create ${this.kind} from ${grad.kind}")
    }
  }

  def fromDense(dense: DenseDoubleGradient): Unit = fromArray(dense.values)

  def fromSparse(sparse: SparseDoubleGradient): Unit = {
    indices = sparse.indices
    fromArray(sparse.values)
  }

  private def fromArray(values: Array[Double]): Unit = {
    size = values.length
    quantizer = new ZipMLQuantizer(binNum)
    quantizer.quantize(values)
    //quantizer.parallelQuantize(values)
  }

  override def timesBy(x: Double): Unit = quantizer.timesBy(x)

  override def countNNZ: Int = size

  override def toDense: DenseDoubleGradient = {
    val bucketValues = quantizer.getValues
    val values = quantizer.getBins.map(bin => bucketValues(bin))
    new DenseDoubleGradient(dim, values)
  }

  override def toSparse: SparseDoubleGradient = {
    val bucketValues = quantizer.getValues
    val values = quantizer.getBins.map(bin => bucketValues(bin))
    new SparseDoubleGradient(dim, indices, values)
  }

  override def toAuto: Gradient = (if (indices == null) toDense else toSparse).toAuto

  override def kind: Kind = Kind.Zip
}


object ZipMLQuantizer {
  private val logger: Logger = LoggerFactory.getLogger(classOf[ZipMLQuantizer])
}

class ZipMLQuantizer(b: Int) extends Quantizer(b) {
  import ZipMLQuantizer._

  def this() = this(Quantizer.DEFAULT_BIN_NUM)

  override def quantize(values: Array[Double]): Unit = {
    val startTime = System.currentTimeMillis
    n = values.length
    // 1. pre-compute the errors
    val sortedValues = values.clone
    util.Arrays.sort(sortedValues)
    val r = new Array[Double](n)
    val t = new Array[Double](n)
    r(0) = sortedValues(0)
    t(0) = sortedValues(0) * sortedValues(0)
    for (i <- 1 until n) {
      r(i) = r(i - 1) + sortedValues(i)
      t(i) = t(i - 1) + sortedValues(i) * sortedValues(i)
    }
    // 2. find split points
    var splitNum = n
    val splitIndex = (0 until n).toArray
    while (splitNum > binNum) {
      val errors = new Array[Double](splitNum)
      for (i <- 0 until splitNum / 2) {
        val L = splitIndex(2 * i)
        val R = (if (2 * i + 2 >= splitNum) n else splitIndex(2 * i + 2)) - 1
        val l1Sum = r(R) - (if (L == 0) 0 else r(L - 1))
        val l2Sum = t(R) - (if (L == 0) 0 else t(L - 1))
        val mean = l1Sum / (R - L + 1)
        errors(i) = l2Sum + mean * mean * (R - L + 1) - 2 * mean * l1Sum
      }

      val thrNum = binNum / 2 - (if (splitNum % 2 == 1) 1 else 0)
      val threshold = Sort.selectKthLargest(errors.clone, thrNum)

      var newSplitNum = 0
      for (i <- 0 until splitNum / 2) {
        if (errors(i) >= threshold) {
          splitIndex(newSplitNum) = splitIndex(2 * i); newSplitNum += 1
          splitIndex(newSplitNum) = splitIndex(2 * i + 1); newSplitNum += 1
        } else {
          splitIndex(newSplitNum) = splitIndex(2 * i); newSplitNum += 1
        }
      }

      if (splitNum % 2 == 1) {
        splitIndex(newSplitNum) = splitIndex(splitNum - 1); splitNum += 1
      }
      splitNum = newSplitNum
    }

    min = sortedValues(0)
    max = sortedValues(n - 1)
    binNum = splitNum
    splits = new Array[Double](binNum - 1)
    for (i <- 0 until binNum - 1)
      splits(i) = sortedValues(splitIndex(i + 1))
    // 2. find the zero index
    findZeroIdx()
    // 3.find index of each value
    quantizeToBins(values)
    logger.debug(s"ZipML quantization for $n items cost " +
      s"${System.currentTimeMillis - startTime} ms")
  }

  @throws[InterruptedException]
  @throws[ExecutionException]
  override def parallelQuantize(values: Array[Double]): Unit = {
    logger.warn(s"ZipML quantization should be sequential")
    quantize(values)
  }

  override def quantizationType: Quantizer.QuantizationType = ???
}