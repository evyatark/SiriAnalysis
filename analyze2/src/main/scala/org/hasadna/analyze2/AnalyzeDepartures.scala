package org.hasadna.analyze2

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate, LocalDateTime, LocalTime}
import java.util.concurrent.Executors

import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.collection.JavaConverters._

class AnalyzeDepartures(val filesLocation : String, val month : String, ignoreTheseTimes : List[String]) {

  val logger : Logger = LoggerFactory.getLogger(getClass.getName)

  var makatsDescription : Map[String, String] = Map.empty[String, String]   // key is makat (e.g "13324", value is description (e.g " --- קו 324 מבית שמש אל קרית גת  --- Makat 13324  ---" )
  var routesDescription : Map[String, String] = Map.empty[String, String]   // key is routeId (e.g "20846", value is description (e.g " --- קו 324 מבית שמש אל קרית גת  --- Makat 13324  --- Direction 1  --- Alternative 0  " )
  val trace = false


  // we limit to X concurrent threads, because each thread reads a whole file to memory
  // more threads cause OutOfMemory error or Heap error
  // in general, each thread needs about 2GB memory so for 2 threads
  // you should add -Xmx2g to the java command line
  val executor = Executors.newFixedThreadPool(4)  // 4
  implicit val ec = ExecutionContext.fromExecutor(executor)

  def shutdownThreads(): Unit = {
    executor.shutdown()
  }

  var readMakat : ReadMakatFileImpl = null

  def init() = {
    // read makat file
    readMakat = new ReadMakatFileImpl()
    readMakat.init();

  }

  // allBusLines is list of tuples(lineShortName, routeId, makat)
  def processAll(allBusLines : List[(String, String, String)]) : List[DeparturesAnalysisResult] = {    //List[String] = {


    def produce() : Future[List[DeparturesAnalysisResult]] = {
      val results: List[Future[DeparturesAnalysisResult]] =
        for ((line, route, makat) <- allBusLines) yield {
          Future {
            processBusLine(line, route, makat, readMakat)
          }
        }
      Future.sequence(results)
    }


    logger.debug(s"processing these routes: $allBusLines ...")

    val result = Await.result(produce(), Duration.Inf)

    logger.debug(s"processing routes ... completed")

    result
  }


  def toScalaList(value: java.util.List[String]) : List[String] = {
    scala.collection.JavaConverters.asScalaBuffer( value).toList
  }


  def toScala(x : java.util.Map[DayOfWeek, java.util.List[String]]) : Map[DayOfWeek, List[String]] = {
    val jMap = new java.util.HashMap[DayOfWeek, List[String]]()

//    for ( entry : Entry[DayOfWeek, java.util.List[String]] <- x.entrySet) {
//        jMap.put(entry.getKey, toScalaList(entry.getValue) )
//    }
    for (item <- x.keySet().toArray()) {
      val y : List[String] = toScalaList(x.get(item))
      val key = item.asInstanceOf[DayOfWeek]
      jMap.put(key, y)
    }
    jMap.asScala.toMap
  }


  def compareDepartures(departuresFromGtfs: Map[DayOfWeek, List[String]], departuresFromSiri: List[String], date: LocalDate) = {
    val gtfsDepartures = departuresFromGtfs.get(date.getDayOfWeek).get

    val diff = gtfsDepartures.diff(departuresFromSiri)
    println("==============================================================")
    println(s"date $date")
    println("gtfs: " + gtfsDepartures)
    println("siri: " + departuresFromSiri)
    println()
    println("diff (gtfs has): " + diff)
    println("diff (siri has): " + departuresFromSiri.diff(gtfsDepartures))
    println()

  }

  def siriDataFilesExist(date: LocalDate, filesLocation: String): Boolean = {
    var month = date.getMonthValue.toString  //"08"
    if (month.length == 1) month = "0" + month
    val day = date.getDayOfMonth
    val result = !findFiles(day, month, filesLocation).isEmpty
    result
  }

  def findSchedulesFile(filesLocation: String, agency: String, date: LocalDate) : String = {
    val dateAsStr = date.format(DateTimeFormatter.ISO_DATE)
    val f = new File(filesLocation)
    if (f.isDirectory) {
      val allScheduleFiles = f.listFiles().filter(file => file.getName.startsWith(s"siri.schedule.$agency")).toList
      var dateOfFile = date
      var exactDate = allScheduleFiles.filter(file => file.getName.endsWith(dateOfFile.format(DateTimeFormatter.ISO_DATE)))
      while (exactDate.isEmpty) {
        dateOfFile = dateOfFile.plusDays(1)
        exactDate = allScheduleFiles.filter(file => file.getName.endsWith(dateOfFile.format(DateTimeFormatter.ISO_DATE)))
        if (dateOfFile.isAfter(date.plusDays(31))) {
          exactDate = List(new File("/tmp/"))
        }
      }
      exactDate.head.getAbsolutePath
    } else ""
  }

  def findAimedDeparturesFromScheduleFile(busLine: String, route: String, date: LocalDate) : Map[DayOfWeek, List[String]] = {
    // read the siri.schedule.nn.DAY_OF_WEEK.json.yyyy-MM-dd file, take aimed departures from there
    val fileName = findSchedulesFile(filesLocation, "16", date)
    //Source.fromFile(fileName).getLines();
    val x1 = new JsonParser(fileName).retrieveDepartureTimes()
    val x2 = x1.filter(item => item._1.equals(route))
    if (x2.isEmpty) {
      Map[DayOfWeek, List[String]]()
    }
    else if (x2.head._2.equals(None)) {
      Map[DayOfWeek, List[String]]()
    }
    else {
      val x3: (String, Option[Any]) = x2.head
      val x4 = x3._2
      val key: String = x4.asInstanceOf[Map.Map1[String, Any]].keySet.head.toString
      val list = x4.asInstanceOf[Map.Map1[String, Any]].values.head
      Map[DayOfWeek, List[String]](DayOfWeek.valueOf(key) -> list.asInstanceOf[List[String]])
    }
    //val dayOfWeekMap = for (item <- x2) yield (DayOfWeek.valueOf("SUNDAY") -> item._2.asInstanceOf[List[String]])
    //dayOfWeekMap.toMap
    //x4
  }

  def processBusLine(busLine: String, route: String, makat : String, makatFile : ReadMakatFileImpl) : DeparturesAnalysisResult = {
    logger.info(s"$busLine\t\t($route) ")
    //val departures = makatFile.findDeparturesByRouteId(route)

    val resultsForAllDays = (1 to 31).map( day => {
        val year = 2018
        val date = LocalDate.of(year, month.toInt, day)
        if (siriDataFilesExist(date, filesLocation)) {
          val dayOfWeek = date.getDayOfWeek

          // departures from Schedule file
          //==================================
          //val departuresFromSchedule = findAimedDeparturesFromScheduleFile(busLine, route, date)
          //processDay(busLine, route, day, getReadingsForDate(month, day, filesLocation, route), departuresFromSchedule, dayOfWeek, date)
          //println(s"day $day route $route - found ${departures(dayOfWeek).size()} departures")
          //compareDepartures(departuresFromGtfs, departuresFromSiri, date)

          // departures from Makat file
          //==================================
          //val departures: java.util.Map[DayOfWeek, java.util.List[String]] = makatFile.findDeparturesByRouteIdAndDate(route, date)
          //val departuresFromGtfs = if (departures == null) Map[DayOfWeek, List[String]]() else toScala(departures)
          //processDay(busLine, route, day, getReadingsForDate(month, day, filesLocation, route), departuresFromGtfs, dayOfWeek, date)

          // departures from Siri results
          //==================================
          // results from Siri
          val departuresFromSiri = findAimedDeparturesFromSiriResults(busLine, route, date)
          logger.debug(s"$dayOfWeek $date route $route - found ${departuresFromSiri.size} departures")
          processDay(busLine, route, day, getReadingsForDate(month, day, filesLocation, route), Map((dayOfWeek, departuresFromSiri)), dayOfWeek, date)
        }
        else None
      }
    )
    println("")
    val ret = s"\n\nLine $busLine (route $route)\n" :: resultsForAllDays.flatten.toList
    //ret.foreach(println)
    val aimedTotal = resultsForAllDays.flatten.map(b => b.aimed.size).sum
    val missingTotal = resultsForAllDays.flatten.map(b => b.missing.size).sum
    val lateTotal = resultsForAllDays.flatten.map(b => b.late.size).sum
    val percentMissing = if (aimedTotal == 0) 0 else (missingTotal*100/aimedTotal).toInt
    val percentLate = if (aimedTotal == 0) 0 else  (lateTotal*100/aimedTotal).toInt

    val departuresAnalysisResult = DeparturesAnalysisResult(makat = makat, lineShortName = busLine, lineDescription = makatsDescription(makat),
      routes = RouteAnalysisResult(
        routeId=route, routeDescription = routesDescription(route),    // alternative="", direction="",
        routeTotals =RouteTotals(aimedTotal, missingTotal, s"$percentMissing%", lateTotal, s"$percentLate%"),
        routeResults = resultsForAllDays.flatten.toList  // RouteResults(dayOfWeek = day, date = "", late = List(), missing = List(), aimed = List(),

      )
    )

    logger.debug(new JsonParser("dummy").toJson(departuresAnalysisResult.routes.routeTotals)) ;

    //ret
    departuresAnalysisResult
  }


//  def findAllLinesOnDay(day : Int) : Set[(String, String)] = {
//    val all: List[Reading] = getLinesForDate(month, day, filesLocation)
//    all.map(r => (r.lineShortName, r.routeId)).toSet
//  }


  def processDay(line: String,
                 route: String,
                 day: Int,
                 readingsOfThatDay : List[Reading],
                  departures : Map[DayOfWeek, List[String]],
                 dayOfWeek : DayOfWeek,
                 dateOfThisDay : LocalDate)      : Option[RouteDailyResults] = {
    //print(".")
    if (readingsOfThatDay.isEmpty) return None
    val dayName = createDayName(day)
    //dayName.flatMap( dayName => {
    val aimedDeparturesFromMakatFile : List[String] =
        (if (departures.contains(dayOfWeek)) departures(dayOfWeek) else List.empty)
          .filter(dep => shouldNotBeIgnored(dateOfThisDay.toString + "T" + dep))
    logger.trace(s"route $route day $day , get lines for this date and route")
    logger.trace(s"find missing departures for day $day route $route")
    val (aimedDepartures, missingDepartures, lateDepartures) = findMissingDeparturesForLine(line, route, readingsOfThatDay, aimedDeparturesFromMakatFile)
    logger.trace("find missing departures completed");

    logger.trace("create report...")
    val date = f"2018-${month}-" + f"${day}%02d"
    val header = (  f"$dayName%10s  $date" + "\t\t")
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
    val routeResults = RouteDailyResults(dayName, date, lateDepartures,latePercent, missingDepartures,mp, aimedDepartures)
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

  def createDayName(day: Int) : String = {
//   Try(
      LocalDate.parse(s"2018-${month}-" + f"${day}%02d").getDayOfWeek.toString
//    ).toOption
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



  def displayProblems2(lineShortName: String, routeId : String, aimedDepartures: List[String], missingDepartures: List[String], lateDepartures: List[String]): Unit = {

    //println(s"Line $lineShortName")
    //println(s"-------------------")
    for (dep <- aimedDepartures) {
      if (missingDepartures.contains(dep)) {
        logger.trace(s"\n\n$dep\t" + s"======> missing!" )
      }
      else if (lateDepartures.contains(dep)) {
        logger.trace(s"\n$dep\t" + s"================> late (by more than 3 minutes)" )
      }
      else {
        logger.trace(s"$dep\t")
      }
    }
    //println("")
  }

  def findFiles(day: Int, month: String, fileLocation: String): List[String] = {
    var sDay : String = day.toString
    sDay = f"${day}%02d"
    val list : ListBuffer[String] = ListBuffer.empty
    val start = 0
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
      if (listInThisCycle.size < inc) more = false
    }
    list.toList
  }

  def getReadingsForDate(month : String, day : Int, fileLocation : String, route : String) : List[Reading] = {
    logger.trace(s"retrieving lines for day $day for route $route")
    val files : List[String] = findFiles(day, month, fileLocation)
    var all : ListBuffer[Reading] = ListBuffer.empty
    for (file <- files) {
      val content = Source.fromFile(file).getLines().filter(logLine => logLine.contains(s",$route,")).toList
      val allInFile: List[Reading] = content
        .filter(line => !line.isEmpty)
        .map(line => parseLine(line))
        .filter(re => re.localDateTime.toLocalTime.isAfter(LocalTime.parse("03:00")))
      all ++= allInFile
    }
    all.toList
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
          if (depTime.minusMinutes(3).isAfter(time)) {
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



  def filterOnlyReadingsOfDeparture(all : List[Reading], departureTime : String) : List[Reading] = {
    all.filter(_.aimedDeparture.equals(departureTime)).sortBy(_.localDateTime.toString)
  }

  def shouldNotBeIgnored(localDateTime: String ): Boolean = {
    val result = ignoreTheseTimes.filter(timeRange => contains(timeRange, localDateTime)).toList.isEmpty
    //if (!result) println(localDateTime)
    //if (result) {println(s" dateTime $localDateTime should NOT be ignored")}
    //else {println(s" dateTime $localDateTime should be IGNORED")}
    result
  }

  def contains(timeRange: String, time: String) : Boolean = {
    val times = timeRange.split(" to ")
    val start = times(0)
    val end = times(1)
    //    if (time.getDayOfMonth==2 && time.getHour==20) {
    //      println (time)
    //    }
    if ((start < time) && (end > time)) {
      //println (time)
      true
    }
    else false
  }




//  def collectReadingsForDate(monthToAnalyze: String, filesLocation: String, route : String): Map[Int, List[Reading]] = {
//    println("collecting readings...")
//    val ret = (1 to 31).map(day => (day, getLinesForDate(monthToAnalyze, day, filesLocation, route))).toMap
//    println("collecting readings...completed")
//    ret
//  }


  def findAimedDeparturesFromSiriResults(lineShortName : String, routeId : String, date: LocalDate) : List[String] = {

    var month = date.getMonthValue.toString  //"08"
    if (month.length == 1) month = "0" + month
    val day = date.getDayOfMonth


    findAimedDeparturesFromSiriResults(lineShortName,
                                      routeId,
                                      getReadingsForDate(month, day, filesLocation, routeId))
  }


  def findAimedDeparturesFromSiriResults(lineShortName : String, routeId : String, all : List[Reading]): List[String] = {

    val allForLine: List[Reading] = all.filter(r => r.lineShortName.equals(lineShortName) && r.routeId.equals(routeId))

    val aimedDepartures = findAllAimedDepartures(allForLine).toList.sortBy(s => s)
    aimedDepartures
  }

  def findAllAimedDepartures(all : List[Reading]) : Set[String] =  {
    val result = all
      .filter(r => shouldNotBeIgnored(r.localDateTime.toLocalDate.atTime(LocalTime.parse(r.aimedDeparture)).toString))
      .map(_.aimedDeparture)
      .toSet
      .filter(dep => isAfter(dep, "03:00"))
    //println(result)
    result
  }

  def isAfter(s : String, after : String) : Boolean = {
    LocalTime.parse(s).isAfter(LocalTime.parse(after))
  }


//  def findMissingDeparturesForLine(lineShortName : String, routeId : String, all : List[Reading], intervalForLateInMinutes : Int = 5): (List[String], List[String], List[String]) = {
//
//    val allForLine : List[Reading] = all.filter(r => r.lineShortName.equals(lineShortName) && r.routeId.equals(routeId))
//
//    val aimedDepartures = findAllAimedDepartures(allForLine).toList.sortBy(s => s)
//    val missingDepartures : ListBuffer[String] = ListBuffer.empty[String]
//    val lateDepartures : ListBuffer[String] = ListBuffer.empty[String]
//    for (dep <- aimedDepartures) {
//      val depTime = LocalTime.parse(dep)
//      val firstReadingInMotion : List[Reading] = allForLine.filter(!_.latitude.equals("0")).filter(r => r.aimedDeparture.equals(dep)).take(1)
//      if (firstReadingInMotion.isEmpty) {
//        //println(s"\ndeparture $dep does not exist!")
//        missingDepartures += dep ;
//      }
//      //else println(firstReadingInMotion)
//      else {
//        val time = firstReadingInMotion.head.localDateTime.toLocalTime
//        if (depTime.minusMinutes(3).isAfter(time)) {
//          //println(s"\n =====> $dep $depTime $time   ")
//          lateDepartures += dep
//        }
//        else {
//          //print(s"$dep $depTime $time   ")
//        }
//      }
//    }
//
//    (aimedDepartures, missingDepartures.toList, lateDepartures.toList)
//
//  }

}

case class Reading (
                     localDateTime: LocalDateTime,  // 2018-06-28T23:59:33.366
                     lineShortName : String,  // 415
                     routeId : String,
                     description : String,    // [line 415 v 9764032 oad 23:15 ea 00:00]
                     aimedDeparture : String, // 20:30
                     latitude : String,
                     longitude : String
                   )


/*
JSON I want to create:
[{
	"makat": 11141,
	"lineShortName": "14",
	"lineDescription": "קו 14 -- בית שמש",
	"routes": [{
		"routeId": "15494",
		"alternative": "#",
		"direction": "1",
		"routeDescription": "",
		"results": [{
			"dayOfWeek": "Sunday",
			"date": "2018-07-01",
			"late": [],
			"missing": ["07:40", "09:50", "12:10", "18:10", "20:10", "21:45"],
			"aimed": ["06:25", "06:35", "06:50", "06:58", "07:06", "07:14", "07:22", "07:30", "07:40", "07:55", "08:10", "08:30", "08:50", "09:10", "09:30",
				"09:50", "10:10", "10:30", "10:50", "11:10", "11:30", "11:50", "12:10", "12:25", "12:35", "12:45", "12:55", "13:10", "13:25", "13:40", "13:55",
				"14:10", "14:25", "14:40", "14:55", "15:10", "15:25", "15:40", "15:55", "16:10", "16:25", "16:40", "16:55", "17:10", "17:25",
				"17:40", "17:55", "18:10", "18:25", "18:40", "18:55", "19:10", "19:25", "19:40", "19:55", "20:10", "20:25", "20:45", "21:10", "21:25",
				"21:45", "22:10", "22:25", "22:45", "23:30"
			],

			"totals": {
				"aimed": "65",
				"missing": "6",
				"percentMissing": "9.2%",
				"late": "0",
				"percentLate": ""
			}
		}, {
			"dayOfWeek": "Monday",
			"date": "2018-07-02",
			"late": [],
			"missing": ["15:40", "16:40", "18:25"],
			"aimed": ["06:25", "06:35", "06:50", "06:58", "07:06", "07:14", "07:22", "07:30", "07:40", "07:55", "08:10", "08:30", "08:50", "09:10", "09:30", "09:50", "10:10", "10:30",
				"10:50", "11:10", "11:30", "11:50", "12:10", "12:25", "12:35", "12:45", "12:55", "13:10", "13:25", "13:40", "13:55", "14:10", "14:25", "14:40", "14:55", "15:10", "15:25",
				"15:40", "15:55", "16:10", "16:25", "16:40", "16:55", "17:10", "17:25", "17:40", "17:55", "18:10", "18:25", "18:40",
				"18:55", "19:10", "19:25", "19:40", "19:55", "20:10", "20:25", "20:45", "21:10", "22:10", "22:25", "22:45"
			],
			"totals": {
				"aimed": "58",
				"missing": "3",
				"percentMissing": "5.1%",
				"late": "0",
				"percentLate": ""
			}
		}]
	}]
}]

 */