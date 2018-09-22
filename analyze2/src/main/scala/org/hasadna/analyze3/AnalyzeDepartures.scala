package org.hasadna.analyze3

import java.io.File
import java.nio.file.{Files, Paths}
import java.time.temporal.ChronoField
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.stream.Collectors

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.hasadna.analyze2._
import org.hasadna.analyze3.AnalyzeDepartures.{Departure, RouteId}
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

@Component
class AnalyzeDepartures() {

  @Autowired
  val ctx : ApplicationContext = null

  val LATE_INTERVAL = 8;  // minutes

  val logger : Logger = LoggerFactory.getLogger(getClass.getName)

  @Value("${siri.month.analysis:08}")
  val month : String = null

  var makatsDescription : Map[String, String] = Map.empty[String, String]   // key is makat (e.g "13324", value is description (e.g " --- קו 324 מבית שמש אל קרית גת  --- Makat 13324  ---" )
  var routesDescription : Map[String, String] = Map.empty[String, String]   // key is routeId (e.g "20846", value is description (e.g " --- קו 324 מבית שמש אל קרית גת  --- Makat 13324  --- Direction 1  --- Alternative 0  " )
  val trace = false
  var readMakat : ReadMakatFileImpl = null

  var filesLocation : String = ""
  //val month : String
  var ignoreTheseTimes : List[String] = List.empty

  //var allLinesOfDay: List[String] = List.empty[String]

  def init(filesLocation : String, ignoreTheseTimes : List[String]) = {
    this.filesLocation = filesLocation
    this.ignoreTheseTimes = ignoreTheseTimes
//"2018-MM"
    // read makat file
//    readMakat = new ReadMakatFileImpl()
//    readMakat.init(month.toInt)

  }

  //def getReadingsForDate(month: String, day: Int, filesLocation: String, route: Any): List[Reading] = ???

  case class DeparturesOfRoute(val date : LocalDate, val departures : List[DeparturesRouteDate])

  def findAllDepartures(allBusLines : List[BusLineData])
                    : Map[RouteId, List[DeparturesRouteDate]] = {
    var depOfRoute : Map[RouteId, List[DeparturesRouteDate]] = Map.empty[RouteId, List[DeparturesRouteDate]]
    allBusLines.foreach(busLine => depOfRoute += (busLine.lineRef -> List.empty[DeparturesRouteDate]))

    for (day <- (1 to LocalDate.of(LocalDate.now().getYear, month.toInt,1).range(ChronoField.DAY_OF_MONTH).getMaximum.toInt)) {
      val date = LocalDate.of(2018, month.toInt, day)

      val readMakat : ReadMakatFileImpl = new ReadMakatFileImpl();
      readMakat.init(month.toInt, day)

      val x : Option[List[DeparturesRouteDate]] =
        findDeparturesOfAllRoutesInDay(date, allBusLines)
      x match {
        case None => DeparturesOfRoute(date, List.empty)
        case Some(list) => {
          for (item: DeparturesRouteDate <- list) {
            val routeId = item.routeId
            if (depOfRoute.contains(routeId)) {
              val listBefore : List[DeparturesRouteDate] = depOfRoute.get(routeId).get
              var listAfter = item :: listBefore
              depOfRoute += (routeId -> listAfter)
            }
          }
        }
      }
    }
    depOfRoute
  }


  // process all bus lines, but first loop on days, and inside it loop on routes
  // this way, we call getReadingsForDate() only once for each day!
  // here allBusLines has only several routes of one makat
  // (we need to get a list of all routes in all makats!)
  def processAllByDay(allBusLines : List[BusLineData]) : List[DeparturesAnalysisResult] = {
    val departuresOfRoute : Map[RouteId, List[DeparturesRouteDate]] = findAllDepartures(allBusLines)
    // create a map with key=routeId, and value is BusLine (actually a list with a single BusLine)
    // we need this to quickly reach from a routeId to its published name
    val mapBusLinesByRouteId = allBusLines.groupBy(_.lineRef)
    logger.info(s"all departures in $month : $departuresOfRoute")

    val allRouteIds : List[RouteId] = departuresOfRoute.keySet.toList
    val lastDayOfMonth : Int = LocalDate.of(LocalDate.now().getYear, month.toInt,1).range(ChronoField.DAY_OF_MONTH).getMaximum.toInt
    val z : ListBuffer[RouteDailyResults] = ListBuffer.empty
    for (day <- (1 to lastDayOfMonth)) {
      allReadingsGroupedByRoute = Map.empty
      readDayDone = false
      logger.warn(s"process day $day")
      readAllForDate(month, day, filesLocation)  // will read from files of that day
      logger.info(s"read from files for day $day - done")
      var count = 1
      val listOfDailyResults : List[RouteDailyResults] =
        allRouteIds.flatMap(routeId => {
          val departuresOfRouteInDay : List[DeparturesRouteDate] = departuresOfRoute.get(routeId).get.sortBy(p => p.date.toString)
          val x = departuresOfRouteInDay.filter(p => p.date.getDayOfMonth.equals(day))
          val busLine = mapBusLinesByRouteId(routeId).head.lineShortName
          logger.warn(s"process route $routeId (day $day) - route ${count} out of ${allRouteIds.size}")
          count = count + 1
          val results =
            processDay(busLine,
              routeId,
              day,
              readingsForDate(month, day, filesLocation, routeId),    // does not actually read from file - only from List[String] that is in memory
              x(0), //departuresOfRouteInDay,
              LocalDate.of(2018, month.toInt, day))
          logger.info(s"results of day $day route $routeId : $results")
          results
          })
      logger.info(s"results of day $day (for all routes): $listOfDailyResults")
      listOfDailyResults  ++=: z
    }
    // z is a list of all RouteDailyResults from all days in month
    val listOfAllDailyResults : List[RouteDailyResults] = z.toList
    val listOfDepartureAnalysisResults =
      for (routeId <- allRouteIds) yield {
        val routeResults : List[RouteDailyResults] =
          listOfAllDailyResults.
            filter(routeDailyResults => routeDailyResults.routeId.equals(routeId)).
            sortBy(routeDailyResults => routeDailyResults.date)
        // TODO calc the totals
        val routeTotals : RouteTotals = calcTotals(routeResults)
        val shortName = mapBusLinesByRouteId(routeId).head.lineShortName
        val makat = mapBusLinesByRouteId(routeId).head.makat
        val longDesc = makatsDescription.getOrElse(makat, "line-desc not found")
        val routeAnalysisResult = RouteAnalysisResult(makat, routeId, longDesc, routeTotals, routeResults)  // RouteTotals(0,0,"",0,"")
        DeparturesAnalysisResult(makat, shortName, longDesc, routeAnalysisResult)
      }
    logger.info(s"returning: $listOfDepartureAnalysisResults")
    // this is what we want to return:
    listOfDepartureAnalysisResults

    // temporary
    // generateDummyResults()
  }

  def calcTotals(allResultsOfRoute: List[RouteDailyResults]): RouteTotals = {
    val aimedTotal = allResultsOfRoute.map(b => b.aimed.size).sum
    val missingTotal = allResultsOfRoute.map(b => b.missing.size).sum
    val lateTotal = allResultsOfRoute.map(b => b.late.size).sum
    val percentMissing = if (aimedTotal == 0) 0 else (missingTotal*100/aimedTotal).toInt
    val percentLate = if (aimedTotal == 0) 0 else  (lateTotal*100/aimedTotal).toInt
    val routeTotals = RouteTotals(aimedTotal, missingTotal, s"$percentMissing%", lateTotal, s"$percentLate%")
    routeTotals
  }



  def readAllLinesOfDay(month: String, day: Int, fileLocation: String): List[String] = {
    val files : List[String] = findFiles(day, month, fileLocation)
    var all : List[String] = List.empty
    for (file <- files) {
      val content: List[String] = readAllLinesOfResultsFile(file)  // also filters just lines of this route
      all = all ++ content
    }
    all
  }

  var readDayDone = false
  var allReadingsGroupedByRoute : Map[RouteId, List[Reading]] = Map.empty

  def readOnce(month: String, day: Int, fileLocation: String): String = {
    if (!readDayDone) {
      val start = System.nanoTime()
      this.synchronized[String] {
        if (!readDayDone) {
          val allLinesOfDay = readAllLinesOfDay(month, day, fileLocation)
          logger.info(s"create a map of readings from ${allLinesOfDay.size} lines of day $day ...")
          allReadingsGroupedByRoute = allLinesOfDay
            .filter(line => !line.isEmpty)
            .map(line => parseLine(line))
             .groupBy(_.routeId)
          logger.info(s"... Done!");
          readDayDone = true
        }
        logger.info(s"all files of day $day: ${(System.nanoTime() - start) / 1000000000} seconds")

        "done"
      } // end sychronnized


    }
    else "no read"
  }

  // read from file all Readings for that day
  def readAllForDate(month: String, day: Int, fileLocation: String): Unit = {
    readOnce(month, day, fileLocation)
  }

  def readingsForDate(month: String, day: Int, fileLocation: String, routeId: RouteId): List[Reading] = {
    /////////////////
    // only once:
    readOnce(month, day, fileLocation)  // will not read from file if readingsForDate already executed for that day
    ///////////////


    val x = allReadingsGroupedByRoute.get(routeId)
    if (x.isEmpty) {
      List.empty[Reading]
    }
    else {
      x.get.filter(p => p.localDateTime.toLocalTime.isAfter(LocalTime.parse("03:00")))
    }
  }

  // called by processBusLine when departures from makat file
  def processDay(line: String,
                 route: String,
                 day: Int,
                 readingsOfThatDay : List[Reading],
                 departures : List[Departure],
                 dateOfThisDay : LocalDate)      : Option[RouteDailyResults] = {
    if (readingsOfThatDay.isEmpty) return None
    val aimedDeparturesFromMakatFile : List[String] = departures
      .filter(dep => shouldNotBeIgnored(dateOfThisDay.toString + "T" + dep))
    if (departures.size != aimedDeparturesFromMakatFile.size) {
      logger.warn(s"route $route date $dateOfThisDay, ${departures.size} departures before filter, ${aimedDeparturesFromMakatFile.size} after")
    }
    logger.trace(s"route $route day $day , get lines for this date and route")
    logger.trace(s"find missing departures for day $day route $route")
    val (aimedDepartures, missingDepartures, lateDepartures) = findMissingDeparturesForLine(line, route, readingsOfThatDay, aimedDeparturesFromMakatFile)
    logger.trace("find missing departures completed");

    logger.trace("create report...")
    val date : String = f"2018-${month}-" + f"${day}%02d"
    val header = (  f"${dateOfThisDay.getDayOfWeek}%10s  $date" + "\t\t")
    val body = if (!aimedDepartures.isEmpty) {
      Some(displayProblems(line, route, aimedDepartures, missingDepartures, lateDepartures))
    } else None
    logger.trace("create report...completed")
    var latePercent = 0
    var missingPercent = 0
    if (!aimedDepartures.isEmpty) {
      latePercent = (lateDepartures.size * 100 / aimedDepartures.size).toInt
      missingPercent = (missingDepartures.size * 100 / aimedDepartures.size).toInt
    }
    val mp = s"${missingDepartures.size}/${aimedDepartures.size} = $missingPercent%"
    val routeResults = RouteDailyResults("", route, dateOfThisDay.getDayOfWeek.toString, date, lateDepartures,latePercent, missingDepartures,mp, aimedDepartures)
    val ret = body match {
      case Some(body) => Some(routeResults)
      case None => None
    }
    ret
  }

  def displayProblems(lineShortName: String, routeId : String,
                      aimedDepartures: List[String], missingDepartures: List[String], lateDepartures: List[String]): String = {
    if (missingDepartures.isEmpty)
      s"OK (all ${aimedDepartures.size} departures)"
    else
      s"${missingDepartures.size} missing departures (out of ${aimedDepartures.size} aimed departures)  ---  $missingDepartures"
  }

  def shouldNotBeIgnored(localDateTime: String ): Boolean = {
    true   // temporary - do not ignore
    //ignoreTheseTimes.filter(timeRange => contains(timeRange, localDateTime)).toList.isEmpty
  }



  // 2018-07-05T17:36:16.669,[line 18 v 7768269 oad 16:20 ea 17:43],3,10797,18,26206752,16:20,7768269,17:43,17:35:33,31.75551986694336,35.19329833984375
  def parseLine(line: String): Reading = {
    val values = line.split(",")
    Reading (LocalDateTime.parse(values(0)),
      values(4),
      values(3),
      values(1),
      values(6),
      values(10),
      values(11)
    )
  }

  def readAllLinesOfResultsFile(fileFullPath : String, searchForRoute: String) : List[String] = {
    val st = System.nanoTime()
    val javaList = Files.lines(Paths.get(fileFullPath))
      .filter(line => !line.isEmpty)
      .filter(line => line.contains(searchForRoute))
      .collect(Collectors.toList())
    val list = convertToScalaList(javaList)

    val durationMs = (System.nanoTime() - st) / 1000000
    logger.info(s"read ${list.size} lines from file=$fileFullPath, duration=$durationMs ms")
    list
  }

  def readAllLinesOfResultsFile(fileFullPath : String) : List[String] = {
    val st = System.nanoTime()
    val javaList = Files.lines(Paths.get(fileFullPath))
      .filter(line => !line.isEmpty)
      .collect(Collectors.toList())
    val list = convertToScalaList(javaList)

    val durationMs = (System.nanoTime() - st) / 1000000
    logger.info(s"read ${list.size} lines from file=$fileFullPath, duration=$durationMs ms")
    list
  }

  def convertToScalaList(jlist : java.util.List[String] ) : List[String] = {
    toScalaList(jlist)
  }

  def processDay(line: String,
                 route: RouteId,
                 day: Int,
                 readingsOfThatDay : List[Reading],
                 departures : DeparturesRouteDate,
                 dateOfThisDay : LocalDate)      : Option[RouteDailyResults] = {
    logger.warn(s"readings of route $route day $day: ${readingsOfThatDay.size}")
    if (readingsOfThatDay.isEmpty) return None
    val aimedDeparturesFromMakatFile : List[String] = departures.departures
      .filter(dep => shouldNotBeIgnored(dateOfThisDay.toString + "T" + dep))
    if (departures.departures.size != aimedDeparturesFromMakatFile.size) {
      logger.warn(s"route $route date $dateOfThisDay, ${departures.departures.size} departures before filter, ${aimedDeparturesFromMakatFile.size} after")
    }
    logger.trace(s"route $route day $day , get lines for this date and route")
    logger.trace(s"find missing departures for day $day route $route")
    val (aimedDepartures, missingDepartures, lateDepartures) = findMissingDeparturesForLine(line, route, readingsOfThatDay, aimedDeparturesFromMakatFile)
    logger.trace("find missing departures completed");

    logger.trace("create report...")
    val date : String = f"2018-${month}-" + f"${day}%02d"
    val header = (  f"${dateOfThisDay.getDayOfWeek}%10s  $date" + "\t\t")
    val body = if (!aimedDepartures.isEmpty) {
      Some(displayProblems(line, route, aimedDepartures, missingDepartures, lateDepartures))
    } else None
    logger.trace("create report...completed")
    var latePercent = 0
    var missingPercent = 0
    if (!aimedDepartures.isEmpty) {
      latePercent = (lateDepartures.size * 100 / aimedDepartures.size).toInt
      missingPercent = (missingDepartures.size * 100 / aimedDepartures.size).toInt
    }
    val mp = s"${missingDepartures.size}/${aimedDepartures.size} = $missingPercent%"
    val routeResults = RouteDailyResults("", route, dateOfThisDay.getDayOfWeek.toString, date, lateDepartures,latePercent, missingDepartures,mp, aimedDepartures)
    val ret = body match {
      case Some(body) => Some(routeResults)
      case None => None
    }
    ret
  }





  def departuresFromSchedule() = {
    // departures from Schedule file
    //==================================
    //val departuresFromSchedule = findAimedDeparturesFromScheduleFile(busLine, route, date)
    //processDay(busLine, route, day, getReadingsForDate(month, day, filesLocation, route), departuresFromSchedule, dayOfWeek, date)
    //println(s"day $day route $route - found ${departures(dayOfWeek).size()} departures")
    //compareDepartures(departuresFromGtfs, departuresFromSiri, date)

  }

  case class DeparturesRouteDate(val routeId : RouteId, val date : LocalDate, departures: List[Departure])

  def departuresFromMakatFile(routeId : RouteId, date: LocalDate) : DeparturesRouteDate = {   // : List[Departure] = {
    // departures from Makat file
    //==================================
    val departures: java.util.List[Departure] = readMakat.findDeparturesByRouteIdAndDate(routeId, date)
    val departuresFromMakatFile = if (departures == null) List[Departure]() else toScalaList(departures)
    DeparturesRouteDate(routeId, date, departuresFromMakatFile)
  }

  def departuresFromSiri() = {
    // departures from Siri results
    //==================================
    // results from Siri
    //val departuresFromSiri = findAimedDeparturesFromSiriResults(busLine, route, date)
    //logger.debug(s"$dayOfWeek $date route $route - found ${departuresFromSiri.size} departures")
    //processDay(busLine, route, day, getReadingsForDate(month, day, filesLocation, route), Map((dayOfWeek, departuresFromSiri)), dayOfWeek, date)

  }

  def siriDataFilesExist(date: LocalDate, filesLocation: String): Boolean = {
    var month = date.getMonthValue.toString  //"08"
    if (month.length == 1) month = "0" + month
    val day = date.getDayOfMonth
    val result = !findFiles(day, month, filesLocation).isEmpty
    result
  }

  def findFiles(day: Int, month: String, fileLocation: String): List[String] = {
    var sDay : String = day.toString
    sDay = f"${day}%02d"
    val list : ListBuffer[String] = ListBuffer.empty
    var start = 0
    val inc = 10
    var more = true
    while (more) {
      val listInThisCycle : ListBuffer[String] = ListBuffer.empty
      for (i <- (start to start + inc)) {
        val fullPath = fileLocation + s"siri_rt_data.2018-${month}-${sDay}.$i.log"
        val file = new File(fullPath)
        val fileOK = file.exists() && file.canRead
        //println(s"$fullPath - $fileOK")
        val r = if (fileOK) Some(fullPath) else None
        listInThisCycle ++= r
      }
      list ++= listInThisCycle
      start = start + inc
      if (listInThisCycle.size < inc) more = false
    }
    list.toList
      //.take(1)  // temporary - take only one file (there might be 10-12 files)
  }

  case class DeparturesOfRouteAllMonth(val departuresInDay : Map[Int, DeparturesRouteDate])

  /**
    *
    * @param date
    * @param busLines
    * @return Optional List of all departures of all routes on the specified day
    *         List[  // for each routeId
    *           List[String]   // list of departue times of that routeId in the specified day
    *
    */
  def findDeparturesOfAllRoutesInDay(date: LocalDate, busLines : List[BusLineData])
                                : Option[List[DeparturesRouteDate]] = {
    if (siriDataFilesExist(date, filesLocation)) {
      val dayOfWeek = date.getDayOfWeek

      logger.info(s"start departures for all days and routes")
      val depAllRoutes : List[DeparturesRouteDate] =
        busLines.map(busLine =>
          //departuresFromMakatFile =
            departuresFromMakatFile(busLine.lineRef, date)  // List[Departure]
      )
      logger.info(s"complete departures for all days and routes")

      Some(depAllRoutes)

    }
    else None
  }


  def findMissingDeparturesForLine(lineShortName : String,
                                   routeId : String,
                                   all : List[Reading],
                                   departures : List[String],
                                   intervalForLateInMinutes : Int = 5
                                  ) : (List[String], List[String], List[String]) = {

    val allForLine : List[Reading] = all.filter(r => r.lineShortName.equals(lineShortName) && r.routeId.equals(routeId))

    val aimedDepartures = departures.sortBy(s => s)
    val missingDepartures : ListBuffer[String] = ListBuffer.empty[String]
    val lateDepartures : ListBuffer[String] = ListBuffer.empty[String]
    for (dep <- aimedDepartures) {
      //println(s"investigating departure $dep")
      if (dep.substring(0, 2).toInt > 23) {
        logger.trace(s"currently ignoring departure $dep")
      }
      else {
        val depTime = LocalTime.parse(dep)
        //println(s"dep time=$depTime")
        val allReadingInMotion: List[Reading] = allForLine.filter(!_.latitude.equals("0")).filter(r => r.aimedDeparture.equals(dep))
        //println(allReadingInMotion.mkString("\n"))
        val firstReadingInMotion: List[Reading] = allForLine.filter(!_.latitude.equals("0")).filter(r => r.aimedDeparture.equals(dep)).take(1)
        //println(s"all Readings per line per day=${allForLine.size}")
        //println(s"first reading in motion $firstReadingInMotion")
        if (firstReadingInMotion.isEmpty) {
          //println(s"departure $dep does not exist in Readings")
          missingDepartures += dep;
        }
        //else println(firstReadingInMotion)
        else {
          //println(s"first reading in motion ${firstReadingInMotion.head}")
          val time = firstReadingInMotion.head.localDateTime.toLocalTime
          if (time.minusMinutes(LATE_INTERVAL).isAfter(depTime)) {
            //println(s"\n =====> $dep $depTime $time   ")
            lateDepartures += dep
          }
          else {
            //print(s"$dep $depTime $time   ")
          }
        }
      }
    }

    (aimedDepartures, missingDepartures.toList, lateDepartures.toList)

  }




  def generateDummyResults() : List[DeparturesAnalysisResult] = {
    List(
      DeparturesAnalysisResult("makat", "shortName", "description",
        RouteAnalysisResult("makat", "routeId", "description",
          RouteTotals(1,1,"0%", 1, "0%"),
          List(
            RouteDailyResults("", "", "Sunday","2018-09-09",
              List("10:00"),0,List("10:00"),"0%", List("10:00", "11:00")
            )
          )
        )
      )
    )
  }

  def toJson(obj : Any) : String = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.enable(SerializationFeature.INDENT_OUTPUT)
    val json = mapper.writeValueAsString(obj)
    //println(json)
    json
  }

  def toScalaList(value: java.util.List[String]) : List[String] = {
    scala.collection.JavaConverters.asScalaBuffer( value).toList
  }


}

case class BusLine(val shortName : String, val routeId : RouteId, val makat : String) {}

object AnalyzeDepartures {
  type RouteId = String
  type Departure = String

}