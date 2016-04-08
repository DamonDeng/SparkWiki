package com.damondeng.spark.wiki

/**
 * Created by mingxuan on 16/4/8.
 * Runner to call WikiOperator
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WikiOperatorRunner {

  def main(args: Array[String]) {
    println("testing in the runner")

    val conf = new SparkConf().setAppName("HelloWorld")
    val sc = new SparkContext(conf)

    sc.stop()
  }


}
