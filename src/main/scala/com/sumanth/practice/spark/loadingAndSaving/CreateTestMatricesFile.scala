package com.sumanth.practice.spark.loadingAndSaving

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sumanthdommaraju on 1/29/17.
  */
object CreateTestMatricesFile {

  def main(args: Array[String]): Unit = {
    val matricesOutputFile = args(0)
    //"/src/main/resources/pfeMatrices.parquet"
    val conf = new SparkConf().setAppName("PartitioningTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //sc.setLogLevel("DEBUG")
    val hc = new HiveContext(sc)
    val matricSchema = StructType(
      StructField("dealId", StringType)
        ::StructField("matrix", ArrayType(DoubleType))
        ::StructField("timeBuckets", IntegerType)
        ::StructField("paths", IntegerType)
        :: Nil
    )
    val t1 = new Matrix("t1",Array(1.0, 3.0 ,7.0 ,2.2 ,4.4 ,7.7 ,2.6 ,2.1 ,9.0 ,3.3 ,12.9 ,9.23), 4, 3)//IMA123
    val t2 = new Matrix("t2",Array(2.0, 4.0 ,5.0 ,1.2 ,3.4 ,0.7 ,6.6 ,8.1 ,9.9 ,2.3 ,2.9 ,19.23), 4, 3)//IMA342
    val t3 = new Matrix("t3",Array(3.0, 4.0 ,5.0 ,7.2 ,3.8 ,7.3 ,6.6 ,6.3 ,12.9 ,2.9 ,6.9 ,9.73, 12.6, 5.3, 0.9), 3, 5)//END901
    val t4 = new Matrix("t4",Array(2.0, 6.0 ,3.0 ,1.2 ,7.8 ,2.3 ,6.5 ,2.3 ,10.2 ,5.6 ,7.9 ,19.73, 2.6, 3.3, 1.9), 3, 5)//END912
    val t5 = new Matrix("t5",Array(4.0, 7.0 ,2.0 ,3.2 ,6.8 ,8.3 ,3.4 ,2.4 ,2.9 ,2.9 ,6.9 ,9.3, 1.6, 5.3, 0.9), 3, 5)//END123
    val rdd = sc.parallelize(Seq(t1, t2, t3, t4, t5)).map(matrix => Row(matrix.name, matrix.matrix, matrix.timeBuckets, matrix.paths))
    hc.createDataFrame(rdd, matricSchema).write.parquet(matricesOutputFile)
  }
}
