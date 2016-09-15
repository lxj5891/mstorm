package com.goyoo.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Hello world!
  *
  */
object App {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val textFile = sc.textFile("hdfs://hadoop:9000/words.txt")
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCounts.collect.foreach(println)
    sc.stop()
  }
}
