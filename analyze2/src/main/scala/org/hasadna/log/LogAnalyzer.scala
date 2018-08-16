package org.hasadna.log

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.collection.immutable.ListMap
import scala.io.Source
import scala.util.Try

/**
  * This class calculates the average response time for every minute
  * by reading the debug log file of bus application (Java siri client)
  */
class LogAnalyzer {

  val logFileFullPath = "/home/evyatar/logs/work/debug.2018-07-27.0.log";

  def average(xs:List[Int]):Float={
    val (sum,length)=xs.foldLeft((0,0))( { case ((s,l),x)=> (x+s,1+l) })
    sum/length
  }

  def start(args: Array[String]): Unit = {
    println(s"reading log file $logFileFullPath ...")
    val list = Source.fromFile(logFileFullPath)
      .getLines()
      .filter(line => line.contains("latency"))
      .flatMap(line =>
        Try {
          val a = line.split("latency ")(1).split(" ")(0)
          val b = "2018" + line.split("2018")(1).split("\\]")(0)
          (a, b)
        }.toOption
      )
      .flatMap(x => toOption(toNumber(x._1), toDateTime(x._2)))
      .toList   // list of (a, b) where a is Int and b is timestamp
    val list2 =
    for ((ms, d) <- list ) yield {
      (ms, d, d.toEpochSecond(ZoneOffset.UTC) / 60)
    }
    val myMap = list2.groupBy(tuple => tuple._3)
    val result1 =
    for ((key, value) <- myMap) yield {
      val ms = value.map(v => v._1) // list of ints, each int is the responseTime
      (key * 60, average(ms))
    }
    def f(x: (Long, Float)) : (LocalDateTime, Float) = {
      x match {
        case (lon, flo) => (LocalDateTime.ofEpochSecond(lon, 0, ZoneOffset.UTC), flo)
        case _ => (LocalDateTime.now(), 0.toFloat)
      }
    }
    val result2 = ListMap(result1.toSeq.sortBy(_._1):_*)
    val result = result2.toList.map(f)

    result.foreach(println)
  }

  def toNumber(s : String) : Option[Int] = {
    Try{ s.toInt }.toOption
  }

  val dateTimeFormatter : DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def toDateTime (date : String) : Option[LocalDateTime] = {
    Try {
      LocalDateTime.parse(date, dateTimeFormatter);
    }.toOption
  }

  def toOption(op1 : Option[Int], op2 : Option[LocalDateTime]) : Option[(Int, LocalDateTime)] = {
    for (a <- op1; b <-op2) yield (a,b)
  }
}
