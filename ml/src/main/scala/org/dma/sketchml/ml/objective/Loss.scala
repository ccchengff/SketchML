package org.dma.sketchml.ml.objective

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.dma.sketchml.ml.util.Maths

trait Loss extends Serializable {
  def loss(pre: Double, y: Double): Double

  def grad(pre: Double, y: Double): Double

  def predict(w: Vector, x: Vector): Double

  def isL1Reg: Boolean

  def isL2Reg: Boolean

  def getRegParam: Double

  def getReg(w: Vector): Double
}

abstract class L1Loss extends Loss {
  protected var lambda: Double

  override def isL1Reg: Boolean = this.lambda > Maths.EPS

  override def isL2Reg: Boolean = false

  override def getRegParam: Double = lambda

  override def getReg(w: Vector): Double = {
    if (isL1Reg)
      Vectors.norm(w, 1) * lambda
    else
      0.0
  }
}

abstract class L2Loss extends Loss {
  protected var lambda: Double

  def isL1Reg: Boolean = false

  def isL2Reg: Boolean = lambda > Maths.EPS

  override def getRegParam: Double = lambda

  override def getReg(w: Vector): Double = {
    if (isL2Reg)
      Vectors.norm(w, 2) * lambda
    else
      0.0
  }
}

class L1LogLoss(l: Double) extends L1Loss {
  override protected var lambda: Double = l

  override def loss(pre: Double, y: Double): Double = {
    val z = pre * y
    if (z > 18)
      Math.exp(-z)
    else if (z < -18)
      -z
    else
      Math.log(1 + Math.exp(-z))
  }

  override def grad(pre: Double, y: Double): Double = {
    val z = pre * y
    if (z > 18)
      y * Math.exp(-z)
    else if (z < -18)
      y
    else
      y / (1.0 + Math.exp(z))
  }

  override def predict(w: Vector, x: Vector): Double = Maths.dot(w, x)
}

class L2HingeLoss(l: Double) extends L2Loss {
  override protected var lambda: Double = l

  override def loss(pre: Double, y: Double): Double = {
    val z = pre * y
    if (z < 1.0)
      1.0 - z
    else
      0.0
  }

  override def grad(pre: Double, y: Double): Double = {
    if (pre * y <= 1.0)
      y
    else
      0.0
  }

  override def predict(w: Vector, x: Vector): Double = Maths.dot(w, x)
}

class L2LogLoss(l: Double) extends L2Loss {
  override protected var lambda: Double = l

  override def loss(pre: Double, y: Double): Double = {
    val z = pre * y
    if (z > 18)
      Math.exp(-z)
    else if (z < -18)
      -z
    else
      Math.log(1.0 + Math.exp(-z))
  }

  override def grad(pre: Double, y: Double): Double = {
    val z = pre * y
    if (z > 18)
      y * Math.exp(-z)
    else if (z < -18)
      y
    else
      y / (1.0 + Math.exp(z))
  }

  override def predict(w: Vector, x: Vector): Double = Maths.dot(w, x)
}

class L2SquareLoss(l: Double) extends L2Loss {
  override protected var lambda: Double = l

  override def loss(pre: Double, y: Double): Double = 0.5 * (pre - y) * (pre - y)

  override def grad(pre: Double, y: Double): Double = y - pre

  override def predict(w: Vector, x: Vector): Double = Maths.dot(w, x)
}