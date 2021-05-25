package com.ddorong.spark.chapter03App

import org.apache.spark.sql.SparkSession
import scala.io.Source.fromFile

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleApp")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext;

    val homeDir = System.getenv("HOME")
    val inputPath = homeDir + "/sia/github-archive/2015-03-01-0.json"
    val ghLog = spark.read.json(inputPath)

    val pushes = ghLog.filter("type = 'PushEvent'")
    pushes.printSchema
    println("all events: " + ghLog.count)
    println("only pushes: " + pushes.count)
    pushes.show(5)

    val grouped = pushes.groupBy("actor.login").count
    grouped.show(5)

    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)

    val empPath = homeDir + "/sia/first-edition/ch03/ghEmployees.txt"
    val employees = Set() ++ (
      for {
        line <- fromFile(empPath).getLines
      } yield line.trim
      )

    val bcEmployees = sc.broadcast(employees)

    val isEmp = user => bcEmployees.value.contains(user) //    val isEmp: (String => Boolean) = (arg: String) => employees.contains(arg)
    val isEmployee = spark.udf.register("isEmpUdf", isEmp)

    import spark.implicits._
    val filtered = ordered.filter(isEmployee($"login"))
    filtered.show()

  }
}
