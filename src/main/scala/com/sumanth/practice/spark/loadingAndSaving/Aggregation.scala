package com.sumanth.practice.spark.loadingAndSaving


import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by sumanthdommaraju on 1/29/17.
  */
object Aggregation {

    //format of MatrixAgg file.
    /**

      P001=t1,t2,t3,t4,t5
      p002=t6,t7,t8
      p003=t9,t10,t11,t12

    each line represents matrices to be aggregated
    */

    /**
        Ex:pfeMatrices.parquet
        dealId: string|timeBuckets: int|paths: int|matrix: double array
        t1|3|6|[1,2,3,4,5,6  ,7,8,9,10,12,13,  14,15,16,17,18,19]
        t2|2|3|[3,4,5  ,10,12,13]
        ...
        ...
    */

  def main(args: Array[String]) {
    val portfolioAndMatrixNamesFile = args(0)
      //"Spark-Scala/src/main/resources/MatrixAgg.txt"
    val matricesInputFile = args(1)
      //"Spark-Scala/src/main/resources/Matrices.parquet"
    val aggregatedMatricesOutputFile = args(2)
      //"Spark-Scala/src/main/resources/aggregatedMatrices.parquet"
    val conf = new SparkConf().setAppName("Aggregation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val hiveContext = new HiveContext(sc)
    // assuming that file MatrixAgg.txt will be less that 50mb( 50 mb can store approx 700,000 tradeIds)
    val matrixNames = sc.textFile(portfolioAndMatrixNamesFile).filter(x => !x.equals("")).map(line => {
      val keyVals = line.split("=")
      (keyVals(0), keyVals(1))
    }).flatMapValues(x => x.split(",")).map(x => Row(x._1, x._2))
    val matricNamesSchema = StructType(
      StructField("portfolioId", StringType) ::
        StructField("dealId", StringType)
        :: Nil
    )
    val matrixNamesDF = hiveContext.createDataFrame(matrixNames, matricNamesSchema)
    matrixNamesDF.registerTempTable("matrixNames")

    val matricesDF = hiveContext.read.parquet(matricesInputFile)
    val matricesRDD = matricesDF.map(row =>Row(row.getAs[String]("dealId"),row.getAs[Seq[Double]]("matrix"),
                                      row.getAs[Int]("timeBuckets"), row.getAs[Int]("paths")))
    val matricSchema = StructType(
      StructField("dealId", StringType)
        ::StructField("matrix", ArrayType(DoubleType))
        ::StructField("timeBuckets", IntegerType)
        ::StructField("paths", IntegerType)
        :: Nil
    )
    val matrixDF = hiveContext.createDataFrame(matricesRDD, matricSchema)
    matrixDF.registerTempTable("matrices")
    val resultDF = hiveContext.sql("select a.portfolioId, a.dealId, b.matrix, b.timeBuckets, b.paths from matrixNames a, matrices b where a.dealId = b.dealId")
    val aggregatedMatrices = resultDF.map(row => (row.getAs[String]("portfolioId"), new Matrix(row.getAs[String]("dealId"),row.getAs[Seq[Double]]("matrix"),
                                   row.getAs[Int]("timeBuckets"), row.getAs[Int]("paths"))))
                                    .reduceByKey((x, y) => aggregate(x, y, AggregationMethod.Net))
                                    .map(x => {x._2.name = x._1; x}).values.map(matrix => Row(matrix.name, matrix.matrix, matrix.timeBuckets, matrix.paths))

    val aggregationMatricesDF = hiveContext.createDataFrame(aggregatedMatrices, matricSchema)
    aggregationMatricesDF.write.parquet(aggregatedMatricesOutputFile)
    //    aggregationMatricesDF.foreach(row => {
//      println(row.getAs[String]("dealId") + "  "+ row.getAs[Seq[Double]]("matrix")+ "  "+
//        row.getAs[Int]("timeBuckets")+ "  "+  row.getAs[Int]("paths"))
//    })
//    println("done")
//    Thread.sleep(200000)
  }

  def aggregate(x: Matrix, y: Matrix, aggregationMethod: AggregationMethod): Matrix = {
        val timeBuckets = math.max(x.timeBuckets, y.timeBuckets)
        val paths = math.max(x.paths, y.paths)
        val resultMatrix = if(x.matrix.length > y.matrix.length) add(x.matrix, y.matrix, aggregationMethod) else add(y.matrix, x.matrix, aggregationMethod)
        new Matrix(y.name, resultMatrix, timeBuckets, paths)
  }

  def add(biggerMatrix: Seq[Double], smallerMatrix: Seq[Double], aggregationMethod: AggregationMethod): Array[Double] = {
    val resultMatrix = Array.fill[Double](biggerMatrix.length)(0)
    if(AggregationMethod.Gross.equals(aggregationMethod)) {
      for(i <- 0 until biggerMatrix.length) {
        resultMatrix(i) = biggerMatrix(i) + (if(smallerMatrix.length > i) smallerMatrix(i) else 0.0)
      }
    } else {
      for(i <- 0 until biggerMatrix.length) {
        resultMatrix(i) = {
          var bVal = biggerMatrix(i)
          bVal = if(bVal < 0.0) 0.0 else bVal
          var sVal = if(smallerMatrix.length > i) smallerMatrix(i) else 0.0
          sVal = if(sVal < 0.0) 0.0 else sVal
          bVal + sVal
        }
      }
    }
    resultMatrix
  }

  /**
        how it works:
        example:
                matrixAgg.txt
                            P001=t1,t2,t3,t4,t5
                            p002=t6,t7,t8
                            p003=t9,t10,t11,t12

                each line in the above file represents matrices to be aggregated.(string before '=' is portfolioId, string after '=' is comma seperated dealId)

                Matrices.parquet   --parquet files that contain all Matrices (stored in s3 or hdfs)
                       tradeId: string|timeBuckets: int|paths: int|matrix: double array
                       t1|3|6|[1,2,3,4,5,6  ,7,8,9,10,12,13,  14,15,16,17,18,19]
                       t2|2|3|[3,4,5  ,10,12,13]
                        ...

                Both files are converted to dataframes
                MatrixAgg.txt will converted to dataframe in below format
                (portfolioId, dealId)
                (p001,t1)
                (p001,t2)
                (p001,t3)
                ...
                Matrices.parquet will be coverted ton dataframe using below schema
                StructType(
                  StructField("dealId", StringType)
                    ::StructField("matrix", ArrayType(DoubleType))
                    ::StructField("timeBuckets", IntegerType)
                    ::StructField("paths", IntegerType)
                    :: Nil
                )
                then dataframes are joined and required fields are selected, then marices are reduced by key.
                aggregated matrices are saved in aggregatedMatrices.parquet using above mentioned schema.



              //Partitioning:
                dataframes will be in the below format
                dataframe1: (portfolioId, dealId)
                dataframe2: (dealId, matrix)
                So partitioning based on key will not make much difference, because we need trades of same portfolio in one node.
                And we only scan the files one time, so there wont be much shuffling involved in process.
                shuffling will happen twice once during the join(this is because we don't know which matrix belongs to which portfolio),
                                                      and once during reduceByKey action.
                default partitioning should be enough in this scenario.

    */
}