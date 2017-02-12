package com.sumanth.practice.spark.loadingAndSaving

/**
  * Created by sumanthdommaraju on 1/29/17.
  */
class Matrix(var name: String, var matrix: Seq[Double], var timeBuckets: Int, var paths:Int) extends java.io.Serializable {

  override def toString = s"PFEMatrix($name, $matrix, $timeBuckets, $paths)"
}
