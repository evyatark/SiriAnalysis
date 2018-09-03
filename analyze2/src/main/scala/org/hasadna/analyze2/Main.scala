package org.hasadna.analyze2

import java.io.File
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import org.slf4j.{Logger, LoggerFactory}


class Main {

  val logger : Logger = LoggerFactory.getLogger(getClass.getName)

  val filesLocation = System.getProperty("siri.results.dir", "/home/evyatar/logs/work/" );
  val monthToAnalyze = System.getProperty("siri.month.analysis", "08");
  //val filterBusLines = List("130", "110", "111", "150")
  val filterMakats = List.empty
  //val filterMakats = List("12150", "15130", "12111", "10110")
  val filterBusLines = List("420", "412","411","420","415","414","416","417","419")
  //val filterBusLines = List()
  //val filterBusLines = List("414");    // 160

  /*
  3,אגד
4,אגד תעבורה
5,דן
6,ש.א.מ
7,נסיעות ותיירות
8,גי.בי.טורס
10,מועצה אזורית אילות
14,נתיב אקספרס
15,מטרופולין
16,סופרבוס
18,קווים
19,מטרודן
23,גלים
24,מועצה אזורית גולן
25,אפיקים
30,דן צפון
31,דן בדרום
32,דן באר שבע

   */
  val allAgencies = List(16) //List(3,4,5,7,8,14,15,16,18,19,25,30,31,32)



  val ignoreTheseTimes = List("2018-07-02T20:01 to 2018-07-02T22:59",
                              "2018-07-24T16:00 to 2018-07-24T23:59", // suspicious behavior in that interval

                              "2018-08-02T09:30 to 2018-08-02T23:59",  // some results show problems here. but e.g.150 is OK till midnight
                              "2018-08-05T00:01 to 2018-08-05T09:30",  // bug in postponing of nextExecution (maybe also some hours from 04 night)
                              "2018-08-13T00:01 to 2018-08-13T08:00",
                              "2018-08-20T15:01 to 2018-08-20T23:59"
                              )


  def findDescriptions(allLinesGroupedByMakat: Map[String, List[BusLineData]]) : Map[String, String] = {
    for ((makat, routes) <- allLinesGroupedByMakat) yield {
      val longDescription = routes(0).description.split("Direction")(0)
      (makat, longDescription)
    }
  }

  def parseSchedules(lastDate : String, agencies : List[String]) : List[BusLineData] = {
    val dir = "/home/evyatar/logs/schedules/"
    val prefix = "siri.schedule."
//    val lastDate = "2018-08-22"
//    val agencies = List(18, 5)
    val allFiles : List[String] =
      agencies.flatMap(agency => {
        val fileName = prefix + agency.toString + ".*" + ".json." + lastDate
        val files = new File(dir).listFiles().map(file => file.getName)
          .filter(name => name.startsWith(prefix + agency.toString + ".") && name.endsWith(".json." + lastDate))
          .map(name => dir + name).toList
        files
      })
    allFiles.flatMap(fileName => (new JsonParser(fileName)).parseJson())
  }

  def start(args: Array[String]): Unit = {
    logger.info("reading json...")

    val allLines0 : List[BusLineData] = parseSchedules("2018-08-21", allAgencies.map(_.toString))
    logger.info("reading json... completed")



    val allLines = allLines0.map(busLine => busLine match {
        case BusLineData(activeRange, "unknown", lineShortName, description, executeEvery,
                        lineRef, previewInterval, stopCode, weeklyDepartureTimes, lastArrivalTimes, maxStopVisits) =>
            BusLineData(activeRange, lineShortName,lineShortName ,description,executeEvery,lineRef,previewInterval,stopCode,weeklyDepartureTimes, lastArrivalTimes, maxStopVisits)
        case _ => busLine
      }
    )
      .filter(b => (filterBusLines.contains( b.lineShortName ) || filterBusLines.isEmpty))
      .filter(b => (filterMakats.contains(b.makat) || filterMakats.isEmpty))

    val lineNames : List[String] = allLines.map(b => b.lineShortName).distinct

    val allLinesGroupedByMakat : Map[String, List[BusLineData]] = allLines.groupBy(_.makat)

    val ap = new AnalyzeDepartures(filesLocation , monthToAnalyze, ignoreTheseTimes)
    ap.init();
    ap.routesDescription = allLines.map(busLine => (busLine.lineRef -> busLine.description)).toMap

    logger.info(s"processing ${allLinesGroupedByMakat.keySet.size} bus lines ($lineNames)")

    ap.makatsDescription = findDescriptions(allLinesGroupedByMakat)
    var counter = 0 ;
    val total = allLinesGroupedByMakat.keySet.size
    val resultsByMakat =
      for ((makat, routes) <- allLinesGroupedByMakat) yield {
        val lineShortName = routes(0).lineShortName
        counter = counter + 1
        logger.info(s"makat $makat  $lineShortName ${ap.makatsDescription(makat)}")

        // listOfTuples: each tuple is (lineShortName, routeId, makat),
        val listOfTuples = routes.map(route =>
                                        (lineShortName, route.lineRef, makat))


        val results : List[DeparturesAnalysisResult] = ap.processAll(listOfTuples)


        // these are results of all routes of one makat
        //results.foreach(println)
        logger.info(s"${counter*100/total}% completed")

        (makat, results)
      }
    val unique = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC).toString
    val date = LocalDate.now().toString
    new JsonParser(s"/tmp/busLinesAnalysis.$date.$unique.json").writeToFile(resultsByMakat)

    ap.shutdownThreads()
  }



  def start1(args: Array[String]): Unit = {
    val startedAt = System.nanoTime()
    logger.info("reading Schedules...")

    val allLines0: List[BusLineData] = parseSchedules("2018-08-21", allAgencies.map(_.toString))
    logger.info("reading Schedules... completed")

    val allLines = allLines0.map(busLine => busLine match {
      case BusLineData(activeRange, "unknown", lineShortName, description, executeEvery,
      lineRef, previewInterval, stopCode, weeklyDepartureTimes, lastArrivalTimes, maxStopVisits) =>
        BusLineData(activeRange, lineShortName, lineShortName, description, executeEvery, lineRef, previewInterval, stopCode, weeklyDepartureTimes, lastArrivalTimes, maxStopVisits)
      case _ => busLine
    }).filter(b => (filterBusLines.contains(b.lineShortName) || filterBusLines.isEmpty))
      .filter(b => (filterMakats.contains(b.makat) || filterMakats.isEmpty))

    val lineNames: List[String] = allLines.map(b => b.lineShortName).distinct

    val allLinesGroupedByMakat: Map[String, List[BusLineData]] = allLines.groupBy(_.makat)

    val ap = new BetterAnalyzeDepartures(filesLocation, monthToAnalyze, ignoreTheseTimes)
    ap.init();
    ap.routesDescription = allLines.map(busLine => (busLine.lineRef -> busLine.description)).toMap
    ap.makatsDescription = findDescriptions(allLinesGroupedByMakat)

    //logger.info(s"processing ${allLinesGroupedByMakat.keySet.size} bus lines ($lineNames)")

    val x : List[RouteDailyResults] = ap.doProcess(allLinesGroupedByMakat)
    val results : List[DeparturesAnalysisResult] = combine(x)
    println(results.toString())

    val unique = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC).toString
    val date = LocalDate.now().toString
    new JsonParser(s"/tmp/busLinesAnalysis.$date.$unique.json").writeToFile(results)

    ap.shutdownThreads()

    val timeElapsedInSeconds = (System.nanoTime() - startedAt)/1000000000
    val timeElapsedMinutes = timeElapsedInSeconds/60
    println(s"done in $timeElapsedMinutes minutes (which is $timeElapsedInSeconds seconds)")

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

  // combine results from all routes
  def combine(x: List[RouteDailyResults]): List[DeparturesAnalysisResult] = {
    val groups = x.groupBy(_.routeId)
    val routeResults =
      for (route <- groups.keys) yield {
        val allResultsOfRoute : List[RouteDailyResults] = groups(route).sortBy(_.date)
        val routeTotals : RouteTotals = calcTotals(allResultsOfRoute)
        RouteAnalysisResult(allResultsOfRoute(0).makat, route, "desc", routeTotals, allResultsOfRoute)
      }
    val routeAnalysisResults : List[RouteAnalysisResult] = routeResults.toList
    val y : List[(String, List[RouteAnalysisResult])] = routeAnalysisResults.groupBy(_.makat).toList
    val z : List[List[RouteAnalysisResult]] = y.map(tup => tup._2)
    val departuresAnalysisResults : List[DeparturesAnalysisResult] = z.flatMap( list   => {
      val routeAnalysisResults : List[RouteAnalysisResult] = list
      val makat = routeAnalysisResults(0).makat
      val mapFromRouteIdToRouteAnalysisResultsOfThatRouteId = routeAnalysisResults.groupBy(_.routeId)
      val listOfDep : List[DeparturesAnalysisResult] =
        mapFromRouteIdToRouteAnalysisResultsOfThatRouteId
          .keySet
            .map(routeId =>
              DeparturesAnalysisResult(makat, "shortNameOf " + routeId, "desc",mapFromRouteIdToRouteAnalysisResultsOfThatRouteId(routeId)(0) )
            ).toList
      listOfDep
    })
    departuresAnalysisResults
    //List.empty
  }


//  def doProcess(ap : AnalyzeDepartures,
//                allLinesGroupedByMakat : Map[String, List[BusLineData]]) : List[RouteDailyResults] = {
//    val x : List[RouteDailyResults] = process(ap, getAllMakatsAndRoutes(allLinesGroupedByMakat))
//    x
//  }

//  // returns list of (shortName, routeId, makat)
//  def getAllMakatsAndRoutes(allLinesGroupedByMakat : Map[String, List[BusLineData]]) : List[(String, String, String)] = {
//
//    val listOfTuples : List[(String, String, String)] =
//      allLinesGroupedByMakat.flatMap(makatAndItsRoutes => touplesForMakat(makatAndItsRoutes)).toList
//    listOfTuples
//  }
//
//  def touplesForMakat(makatAndItsRoutes : (String, List[BusLineData])) : List[(String, String, String)] = {
//    val (makat, routes) = makatAndItsRoutes
//    val lineShortName = routes(0).lineShortName
//    val listOfTuples = routes.map(route =>  (lineShortName, route.lineRef, makat))
//    listOfTuples
//  }
//
//  def process(ap : AnalyzeDepartures,
//              allBusLines : List[(String, String, String)]  // (shortName, routeId, makat)
//             ) : List[RouteDailyResults] = {
//    val x : List[Option[RouteDailyResults]] =
//      (1 to 31).flatMap( day => dailyResultsForAllRoutes(day, ap, allBusLines) ).toList
//    x.flatten
//  }
//
//  def dailyResultsForAllRoutes(day : Int,
//                               ap : AnalyzeDepartures,
//                               allBusLines : List[(String, String, String)]) : List[Option[RouteDailyResults]]  = {
//
//    def parallellize() : Future[List[Option[RouteDailyResults]]] = {
//      val pp : List[Future[Option[RouteDailyResults]]] =
//        allBusLines.map(x  => {
//          Future(processBusLine(day, ap, x._1, x._2, x._3))
//        })
//      Future.sequence(pp)
//    }
//
//    val result = Await.result(parallellize(), Duration.Inf)
//    result
//  }
//
//  def processBusLine(day : Int,ap: AnalyzeDepartures, line : String, route : String, makat : String) = {
//    val results : Option[RouteDailyResults] = ap.processBusLineOneDay(day, line, route, makat, new ReadMakatFileImpl())
//    results
//  }


  def allLines1() : List[(String, String)] = {
    List(
      //63
      //("63", "2517"),("63", "2519"),
      // 26
      ("26", "3093"),
      ("","")
    )

  }
}

