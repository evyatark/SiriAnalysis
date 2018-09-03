package org.hasadna.analyze2

import java.io._
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate, LocalDateTime, LocalTime}
import java.util.concurrent.Executors
import java.util.stream.Collectors

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BetterAnalyzeDepartures(val filesLocation : String, val month : String, ignoreTheseTimes : List[String]) {

  val LATE_INTERVAL = 8;  // minutes

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
    readMakat.init(month.toInt);

  }


  // +
  def doProcess(//ap : AnalyzeDepartures,
                allLinesGroupedByMakat : Map[String, List[BusLineData]]) : List[RouteDailyResults] = {
    val x : List[RouteDailyResults] = process( //ap,
      getAllMakatsAndRoutes(allLinesGroupedByMakat))
    x
  }

  // +
  // returns list of (shortName, routeId, makat)
  def getAllMakatsAndRoutes(allLinesGroupedByMakat : Map[String, List[BusLineData]]) : List[(String, String, String)] = {

    val listOfTuples : List[(String, String, String)] =
      allLinesGroupedByMakat.flatMap(makatAndItsRoutes => touplesForMakat(makatAndItsRoutes)).toList
    listOfTuples
  }

  // +
  def touplesForMakat(makatAndItsRoutes : (String, List[BusLineData])) : List[(String, String, String)] = {
    val (makat, routes) = makatAndItsRoutes
    val lineShortName = routes(0).lineShortName
    val listOfTuples = routes.map(route =>  (lineShortName, route.lineRef, makat))
    listOfTuples
  }

  // +
  def process(//ap : AnalyzeDepartures,
              allBusLines : List[(String, String, String)]  // (shortName, routeId, makat)
             ) : List[RouteDailyResults] = {
    val x : List[Option[RouteDailyResults]] =
      (1 to 31).flatMap( day => dailyResultsForAllRoutes(day,
                                                        //ap,
                                                        allBusLines) ).toList
    x.flatten
  }

  var readingsForDay : Map[Int, List[Reading]] = Map.empty

  // +
  def readForDay(day : Int) : List[Reading] = {

      logger.info(s"readForDay $day")
      if (readingsForDay.contains(day)) {
        readingsForDay(day)
      }
      else {
        this.synchronized {
          if (readingsForDay.contains(day)) {
            readingsForDay(day)
          }
          else {
            readingsForDay = readingsForDay - (day - 1)
            readingsForDay = readingsForDay +(day -> reallyRead(day))
            logger.info(s"removed ${day - 1}, added $day. size of map is ${readingsForDay.size}")
            readingsForDay(day)
          }
        }
      }

  }

  // +
  def reallyRead(day : Int) : List[Reading] = {
    logger.info(s"start really reading $day")
    val files : List[String] = findFiles(day, month, filesLocation)
    var all : ListBuffer[Reading] = ListBuffer.empty
    for (file <- files) {
      val content = readAllLinesOfResultsFile(file)
      val allInFile: List[Reading] = content
        .filter(line => !line.isEmpty)
        .map(line => parseLine(line))
        .filter(re => re.localDateTime.toLocalTime.isAfter(LocalTime.parse("03:00")))
      all ++= allInFile
    }
    logger.info(s"completed reading and parsing ${all.size} lines from all ${files.size} files")
    all.toList
  }

  // +
  def dailyResultsForAllRoutes(day : Int,
                               //ap : AnalyzeDepartures,
                               allBusLines : List[(String, String, String)]) : List[Option[RouteDailyResults]]  = {
    readForDay(day);  // this will cache all readings of that day, so next reads of that day will be from memory

    def parallellize() : Future[List[Option[RouteDailyResults]]] = {
      val pp : List[Future[Option[RouteDailyResults]]] =
        allBusLines.map(x  => {
          Future(processBusLine(day, //ap,
                                x._1, x._2, x._3))
        })
      Future.sequence(pp)
    }

    val result = Await.result(parallellize(), Duration.Inf)
    result
  }


  // +
  def processBusLine(day : Int, //ap: AnalyzeDepartures,
                     line : String, route : String, makat : String) : Option[RouteDailyResults] = {
    val results : Option[RouteDailyResults] = processBusLineOneDay(day, line, route, makat, new ReadMakatFileImpl())
    results
  }







  // ================================================================================
  // ================================================================================
  // ================================================================================







  def toScalaList(value: java.util.List[String]) : List[String] = {
    scala.collection.JavaConverters.asScalaBuffer( value).toList
  }


  def toScala(x : java.util.Map[DayOfWeek, java.util.List[String]]) : Map[DayOfWeek, List[String]] = {
    val jMap = new java.util.HashMap[DayOfWeek, List[String]]()
    for (item <- x.keySet().toArray()) {
      val y : List[String] = toScalaList(x.get(item))
      val key = item.asInstanceOf[DayOfWeek]
      jMap.put(key, y)
    }
    jMap.asScala.toMap
  }



  //+
  def siriDataFilesExist(date: LocalDate, filesLocation: String): Boolean = {
    var month = date.getMonthValue.toString  //"08"
    if (month.length == 1) month = "0" + month
    val day = date.getDayOfMonth
    val result = !findFiles(day, month, filesLocation).isEmpty
    result
  }



  // +
  def processBusLineOneDay(day : Int,
                           busLine: String,
                           route: String,
                           makat : String,
                           makatFile : ReadMakatFileImpl) : Option[RouteDailyResults] = {
    logger.info(s"$busLine\t\t($route)\t\t$day ")
    //val departures = makatFile.findDeparturesByRouteId(route)
    val year = 2018
    val date = LocalDate.of(year, month.toInt, day)
    if (siriDataFilesExist(date, filesLocation)) {
      val dayOfWeek = date.getDayOfWeek

      // ========== departures from Makat File ===
//      val departures: java.util.List[String] = makatFile.findDeparturesByRouteIdAndDate(route, date)
//      val departuresFromMakatFile = if (departures == null) List[String]() else toScalaList(departures)
//      processDay(busLine, route, day, getReadingsForDate(month, day, filesLocation, route, busLine), departuresFromMakatFile, date)

      // departures from Siri results
      //==================================
      // results from Siri
      val departuresFromSiri = findAimedDeparturesFromSiriResults(busLine, route, date)
      logger.debug(s"$dayOfWeek $date route $route - found ${departuresFromSiri.size} departures")
      //val results : Option[RouteDailyResults] = processDay(busLine, route, day, getReadingsForDate(month, day, filesLocation, route, busLine), Map((dayOfWeek, departuresFromSiri)), dayOfWeek, date)
      val results : Option[RouteDailyResults] = processDay(busLine, route, day, getReadingsForDate(day, route, busLine), Map((dayOfWeek, departuresFromSiri)), dayOfWeek, date)
      results.map(x => RouteDailyResults(makat, x.routeId, x.dayOfWeek, x.date, x.late, x.latePercent, x.missing, x.missingPercent, x.aimed))
    } else {
      None
    }
  }



  // + called by processBusLine when departuresFromSiri
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
    val routeResults = RouteDailyResults("", route, dayName, date, lateDepartures,latePercent, missingDepartures,mp, aimedDepartures)
    val ret = body match {
        case Some(body) => Some(routeResults)
        case None => None
    }
    ret
  }

  // +
  def displayProblems(lineShortName: String, routeId : String,
                      aimedDepartures: List[String], missingDepartures: List[String], lateDepartures: List[String]): String = {
    if (missingDepartures.isEmpty)
      s"OK (all ${aimedDepartures.size} departures)"
    else
      s"${missingDepartures.size} missing departures (out of ${aimedDepartures.size} aimed departures)  ---  $missingDepartures"
  }

  // +
  def createDayName(day: Int) : String = {
      LocalDate.parse(s"2018-${month}-" + f"${day}%02d").getDayOfWeek.toString
  }


  // +
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




  // +
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
  }

  // +
  def getReadingsForDate(day : Int, route : String, shortName : String) : List[Reading] = {
    logger.trace(s"retrieving lines for day $day for route $route")
    val ret = readForDay(day)
      .filter(_.routeId.equals(route))
      .filter(re => re.localDateTime.toLocalTime.isAfter(LocalTime.parse("03:00")))
    logger.trace("retrieved")
    ret
  }

  // +
  def getReadingsForDate(month : String, day : Int, fileLocation : String, route : String, shortName : String) : List[Reading] = {
    logger.trace(s"retrieving lines for day $day for route $route")
    val files : List[String] = findFiles(day, month, fileLocation)
    var all : ListBuffer[Reading] = ListBuffer.empty
    val searchForRoute : String = "," + route + "," + shortName + ","
    for (file <- files) {
      val content = readAllLinesOfResultsFile(file, searchForRoute)  // also filters just lines of this route
      //val content = Source.fromFile(file).getLines().filter(logLine => logLine.contains(searchFor)).toList
      val allInFile: List[Reading] = content
        .filter(line => !line.isEmpty)
        .map(line => parseLine(line))
        .filter(re => re.localDateTime.toLocalTime.isAfter(LocalTime.parse("03:00")))
      all ++= allInFile
    }
    all.toList
  }



  // +
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



  // +
  def shouldNotBeIgnored(localDateTime: String ): Boolean = {
    val result = ignoreTheseTimes.filter(timeRange => contains(timeRange, localDateTime)).toList.isEmpty
    //if (!result) println(localDateTime)
    //if (result) {println(s" dateTime $localDateTime should NOT be ignored")}
    //else {println(s" dateTime $localDateTime should be IGNORED")}
    result
  }

  // +
  def contains(timeRange: String, time: String) : Boolean = {
    val times = timeRange.split(" to ")
    val start = times(0)
    val end = times(1)
    if ((start < time) && (end > time)) {
      true
    }
    else false
  }



  // +
  def findAimedDeparturesFromSiriResults(lineShortName : String, routeId : String, date: LocalDate) : List[String] = {

    var month = date.getMonthValue.toString  //"08"
    if (month.length == 1) month = "0" + month
    val day = date.getDayOfMonth


    findAimedDeparturesFromSiriResults(lineShortName,
                                      routeId,
                                      getReadingsForDate(month, day, filesLocation, routeId, lineShortName))
  }

  // +
  def findAimedDeparturesFromSiriResults(lineShortName : String, routeId : String, all : List[Reading]): List[String] = {

    val allForLine: List[Reading] = all.filter(r => r.lineShortName.equals(lineShortName) && r.routeId.equals(routeId))

    val aimedDepartures = findAllAimedDepartures(allForLine).toList.sortBy(s => s)
    aimedDepartures
  }

  // +
  def findAllAimedDepartures(all : List[Reading]) : Set[String] =  {
    val result = all
      .filter(r => shouldNotBeIgnored(r.localDateTime.toLocalDate.atTime(LocalTime.parse(r.aimedDeparture)).toString))
      .map(_.aimedDeparture)
      .toSet
      .filter(dep => isAfter(dep, "03:00"))
    //println(result)
    result
  }

  // +
  def isAfter(s : String, after : String) : Boolean = {
    LocalTime.parse(s).isAfter(LocalTime.parse(after))
  }


  // +
  def convertToScalaList(jlist : java.util.List[String] ) : List[String] = {
    toScalaList(jlist)
  }

  // +
  def readAllLinesOfResultsFile(fileFullPath : String) : List[String] = {
    val st = System.nanoTime()
    val x = Files.lines(Paths.get(fileFullPath))
      .collect(Collectors.toList())
    val durationMs = (System.nanoTime() - st) / 1000000
    logger.error(s"file=$fileFullPath, duration=$durationMs ms")
    convertToScalaList(x)
  }

  // +
  def readAllLinesOfResultsFile(fileFullPath : String, justThisRoute : String) : List[String] = {
    val st = System.nanoTime()
    val x = Files.lines(Paths.get(fileFullPath))
      .filter(line => line.contains(justThisRoute))
      .collect(Collectors.toList())
    val durationMs = (System.nanoTime() - st) / 1000000
    logger.info(s"read ${x.size()} lines from file=$fileFullPath, route=$justThisRoute, duration=$durationMs ms")
    convertToScalaList(x)

  }

}



/*
case class Reading (
                     localDateTime: LocalDateTime,  // 2018-06-28T23:59:33.366
                     lineShortName : String,  // 415
                     routeId : String,
                     description : String,    // [line 415 v 9764032 oad 23:15 ea 00:00]
                     aimedDeparture : String, // 20:30
                     latitude : String,
                     longitude : String
                   )
*/
