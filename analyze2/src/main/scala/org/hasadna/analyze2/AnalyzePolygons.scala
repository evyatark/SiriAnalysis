package org.hasadna.analyze2

import scala.io.Source
import scala.util.Try

class AnalyzePolygons {

  def start(): Unit = {
    val fileName = "/home/evyatar/logs/routes_from_polygons_180808_1.csv"
    val lines = Source.fromFile(fileName).getLines().toList


    val csvData = lines.map(line => parseLine(line))

    val allMakats = csvData.map(csvLine => csvLine.makat).toSet
    val allRoutes = csvData.map(csvLine => csvLine.routeId).toSet
    val makatsAndRoutes = csvData.map(csvLine => (csvLine.makat, csvLine.routeId)).groupBy(_._1)

    println(s"${allMakats.size} makats: $allMakats")
    println(s"${allRoutes.size} routes: $allRoutes")
    println(s"makats and routes: $makatsAndRoutes")
  }

  def parseLine(line: String): CsvLine = {
    try {
      val values = line.split(",")
      val makat = values(5).split("-")(0)
      val direction = values(5).split("-")(1)
      val alternative = values(5).split("-")(2)
      CsvLine(values(1), values(2).toInt, values(3), values(4), makat, direction, alternative)
    }
    catch {
      case _ => CsvLine("",0,"","","","","")
    }
  }

}

//object AnalyzePolygons {
//  val x = new AnalyzePolygons()
//  x.start()
//  println(s"done!")
//}

case class CsvLine(val routeId : String,
                   val agency : Int,
                   val shortName : String,
                   val longName : String,
                   val makat : String,
                   val direction : String,
                   val alternative : String)