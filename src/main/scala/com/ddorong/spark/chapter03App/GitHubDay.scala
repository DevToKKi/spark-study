package com.ddorong.spark.chapter03App

import org.apache.spark.sql.SparkSession

import scala.io.Source.fromFile

object GitHubDay {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val sc = spark.sparkContext;
    val ghLog = spark.read.json(args(0))

    val pushes = ghLog.filter("type = 'PushEvent'")
    val grouped = pushes.groupBy("actor.login").count
    val ordered = grouped.orderBy(grouped("count").desc)

    val employees = Set() ++ (
      for {
        line <- fromFile(args(1)).getLines
      } yield line.trim
      )

    /**
     * 공유 변수 설정 : 그냥 할 경우 필터링 작업을 수행 할 태스크 수가 200개 정도 가까이 반복적으로 네트워크에 전송 하게 됨.
     */
    val bcEmployees = sc.broadcast(employees)

    import spark.implicits._
    /**
     * 각 login 컬럼 값이 직원 Set에 존재하는지 검사하는 일반적인 필터링 함수
     */
    val isEmp = user => bcEmployees.value.contains(user) //    val isEmp: (String => Boolean) = (arg: String) => employees.contains(arg)

    /**
     * Regist User-define function
     */
    val sqlFunc = spark.udf.register("setContinsUdf", isEmp)

    val filtered = ordered.filter(sqlFunc($"login"))

    filtered.write.format(args(3)).save(args(2))
  }
}
