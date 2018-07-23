package org.dma.sketchml.ml.data

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.dma.sketchml.ml.util.Maths

object Parser {
  def loadData(input: String, format: String, maxDim: Int, numPartition: Int,
               negY: Boolean = true)(implicit sc: SparkContext): RDD[LabeledData] = {
    val parse: (String, Int, Boolean) => LabeledData = format.toLowerCase() match {
      case "libsvm" => Parser.parseLibSVM
      case "csv" => Parser.parseCSV
      case "dummy" => Parser.parseDummy
      case _ => throw new UnknownError("Unknown file format: " + format)
    }
    sc.textFile(input)
      .map(line => parse(line, maxDim, negY))
      .repartition(numPartition)
  }

  def parseLibSVM(line: String, maxDim: Int, negY: Boolean = true): LabeledData = {
    val splits = line.trim.split(" ")
    if (splits.length < 1)
      return null

    var y = splits(0).toDouble
    if (negY && Math.abs(y - 1) > Maths.EPS)
      y = -1

    val nnz = splits.length - 1
    val indices = new Array[Int](nnz)
    val values = new Array[Double](nnz)
    for (i <- 0 until nnz) {
      val kv = splits(i + 1).trim.split(":")
      indices(i) = kv(0).toInt
      values(i) = kv(1).toDouble
    }
    val x = Vectors.sparse(maxDim, indices, values)

    LabeledData(y, x)
  }

  def parseCSV(line: String, maxDim: Int, negY: Boolean = true): LabeledData = {
    val splits = line.trim.split(",")
    if (splits.length < 1)
      return null

    var y = splits(0).toDouble
    if (negY && Math.abs(y - 1) > Maths.EPS)
      y = -1

    val nnz = splits.length - 1
    val values = splits.slice(1, nnz + 1).map(_.trim.toDouble)
    val x = Vectors.dense(values)

    LabeledData(y, x)
  }

  def parseDummy(line: String, maxDim: Int, negY: Boolean = true): LabeledData = {
    val splits = line.trim.split(",")
    if (splits.length < 1)
      return null

    var y = splits(0).toDouble
    if (negY && Math.abs(y - 1) > Maths.EPS)
      y = -1

    val nnz = splits.length - 1
    val indices = splits.slice(1, nnz + 1).map(_.trim.toInt)
    val values = Array.fill(nnz)(1.0)
    val x = Vectors.sparse(maxDim, indices, values)

    LabeledData(y, x)
  }

}
