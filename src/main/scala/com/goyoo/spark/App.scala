package com.goyoo.spark

import org.apache.spark.sql.SparkSession

/**
  * Hello world!
  *
  */
object App {
  def main(args: Array[String]) {
    println("hello world")


    val spark = SparkSession
      .builder
      .appName("WordCount").master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    val textFile = sc.textFile("hdfs://hadoop:9000/words.txt")
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCounts.collect.foreach(println)
    spark.stop()
  }
}
