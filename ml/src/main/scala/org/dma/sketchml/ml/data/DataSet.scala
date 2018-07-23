package org.dma.sketchml.ml.data

import scala.collection.mutable.ArrayBuffer

class DataSet {
  private val data = ArrayBuffer[LabeledData]()
  private var readIndex = 0

  def size: Int = data.size

  def add(ins: LabeledData): Unit = data += ins

  def get(i: Int): LabeledData = data(i)

  def loopingRead: LabeledData = {
    if (readIndex >= size)
      readIndex = 0
    val ins = data(readIndex)
    readIndex += 1
    ins
  }

  def +=(ins: LabeledData): Unit = add(ins)

}
