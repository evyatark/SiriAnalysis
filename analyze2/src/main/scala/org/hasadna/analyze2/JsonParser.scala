package org.hasadna.analyze2

import java.time.DayOfWeek

import com.fasterxml.jackson.databind.{DeserializationFeature, MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.io.Source
import scala.util.Try

class JsonParser(val filename: String) {

  def f ( line : String) : String = {
    val ret = if (!line.contains("activeRanges")) line
    else if (line.contains("\"activeRanges\" : null,")) {
      val x = line.replaceAllLiterally("\"activeRanges\" : null,", "")
      x
    }
    else line
    ret
  }

  def parseJson() : List[BusLineData] = {

    // read
    println(s"Reading $filename ...")
    val json = Source.fromFile(filename).getLines().map(line => f(line)).mkString("\n")
    //val json = Source.fromFile(filename)
    // parse
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val parsedJson = mapper.readValue[Map[String, List[Map[String, Any]]]](json)

    //scala.collection.immutable.Map.Map1
    val data = parsedJson("data") // List of Map Objects

    val x = data.map(item => convertToBusLineData(item))
    val allLines : List[BusLineData] = x.flatten    //convert returns Option[BusLineData], so we use flatten to get the BusLineData itself


    //allLines.foreach(println)
    allLines
  }

  def convertToBusLineData(item : Map[String, Any]) : Option[BusLineData] = {
      val myItem =
        if (!item.contains("lineShortName")) {

          val desc = item("description").toString.split(" ")
          val lineShortName =
              if (desc.size < 2) {
                desc(0)
              }
              else if (desc(0) == "קו") {
                desc(1)
              }
              else if (desc.length > 2 && desc(2) == "קו") {
                desc(3)
              }
              else if ( Try(desc(0).toInt).isSuccess ) {
                desc(0)
              }
//      val longDescription = routes(0).description.split("---")(1)
// might match "קו 82 -- ירושלים "
          item + ("lineShortName" -> lineShortName)
          //aMap + ("lineShortName" -> aMap("description").split(" ")(0)) // in old siri.schedule
          //aMap + ("lineShortName" -> aMap("description").split(" ")(3)) // in newer siri.schedule
        }
        else item

    // assuming myItem is a map with the following keys:
//    activeRanges : String,
//    makat: String,
//    lineShortName : String,
//    description: String,
//    executeEvery: String,
//    lineRef: String,
//    previewInterval: String,
//    stopCode: String,
//    weeklyDepartureTimes : Map[String, List[String]],
//    lastArrivalTimes : Map[String, String],
//    maxStopVisits: String
    val busData = BusLineData(
      activeRanges = myItem.get("activeRanges").orElse(Some("")).get.toString,
      makat = myItem("makat").toString,
      lineShortName = myItem("lineShortName").toString,
      description = myItem("description").toString,
      executeEvery = myItem("executeEvery").toString,
      lineRef = myItem("lineRef").toString,
      previewInterval = myItem("previewInterval").toString,
      stopCode = myItem("stopCode").toString,
      weeklyDepartureTimes = myItem("weeklyDepartureTimes").asInstanceOf[Map[String, List[String]]],
      lastArrivalTimes = myItem("lastArrivalTimes").asInstanceOf[Map[String, String]],
      maxStopVisits = myItem.get("maxStopVisits").orElse(Some("")).get.toString
    )
    Some(busData)

//      // see https://stackoverflow.com/a/20684836
//      val params = Some(myMap.map(_._2).toList).flatMap{
//        case List(makat:String,x:Map[String, List[String]],desc:String,shortName:String,c:String,
//                  d:String,y:Map[String, String],e:String,f:String,g:String) =>
//
//          Some((makat,shortName, desc,c,d,e,f,x,y,g))
//
//        case List(z:Any, makat:String,x:Map[String, List[String]],desc:String,shortName:String,c:String,
//                  d:String,y:Map[String, String],e:String,f:String,g:String) =>
//
//          Some((makat,shortName, desc,c,d,e,f,x,y,g))
//
//        case List(a:String,x:Map[String, List[String]],shortName:String,c:String,
//                  d:String,y:Map[String, String],e:String,f:String,g:String) =>
//
//          Some(("unknown",shortName,a, c,d,e,f,x,y,g))
//
//        case other =>
//          None
//      }
//      val myCaseClass = params.map(BusLineData.tupled(_))
//      myCaseClass
  }

  def toJson(obj : Any) : String = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.enable(SerializationFeature.INDENT_OUTPUT)
    val json = mapper.writeValueAsString(obj)
    println(json)
    json
  }

  def writeToFile(json : String) = {
    reflect.io.File(filename).writeAll(json)
  }

  def writeToFile(obj : Any) = {
    reflect.io.File(filename).writeAll(toJson(obj))
  }

}
