package com.ddorong.spark.chapter04App

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SimpleApp")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext;

    val tranFile =sc.textFile("../../sia/first-edition/ch04/"+"ch04_data_transactions.txt")
    val tranData = tranFile.map(_.split("#"))
    println(tranData)
    var transByCust = tranData.map(tran => (tran(2).toInt, tran))

    println(transByCust.countByKey())
    println(transByCust.countByKey().values.sum)

    val (cid, purch) =transByCust.countByKey().toSeq.sortBy(_._2).last
    println(cid, purch)

    var complTrans = Array(Array("2015-03-30", "11:59 PM", "53", "4", "1", "0.00"))

//    println(transByCust.keys.count())
//    println(transByCust.keys.distinct().count())

  }
}
