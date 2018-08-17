package org.hasadna.analyze2

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import org.slf4j.{Logger, LoggerFactory}


class Main {

  val logger : Logger = LoggerFactory.getLogger(getClass.getName)

  val filesLocation = System.getProperty("siri.results.dir", "/home/evyatar/logs/work/" );
  val monthToAnalyze = System.getProperty("siri.month.analysis", "08");
  //val filterBusLines = List("18", "50", "51", "110", "111", "150")
  val filterBusLines = List("420", "415")
  //val filterBusLines = List()

  val scheduleFileToAnalyze = 4 ;   // 1 - means we analyze allLines1 (see below), 2, 3, etc


  val ignoreTheseTimes = List("2018-07-02T20:01 to 2018-07-02T22:59",
                              "2018-07-24T16:00 to 2018-07-24T23:59", // suspicious behavior in that interval

                              "2018-08-02T09:30 to 2018-08-02T23:59",  // some results show problems here. but e.g.150 is OK till midnight
                              "2018-08-05T00:01 to 2018-08-05T09:30",  // bug in postponing of nextExecution (maybe also some hours from 04 night)
                              "2018-08-13T00:01 to 2018-08-13T08:00"
                              )


  def findDescriptions(allLinesGroupedByMakat: Map[String, List[BusLineData]]) : Map[String, String] = {
    for ((makat, routes) <- allLinesGroupedByMakat) yield {
      val longDescription = routes(0).description.split("Direction")(0)
      (makat, longDescription)
    }
  }


  def start(args: Array[String]): Unit = {
    logger.info("reading json...")

    val allLines1 : List[BusLineData] = (new JsonParser("/home/evyatar/logs/siri.schedule.18.Monday.json.2018-08-13")).parseJson()
    val allLines2: List[BusLineData] = (new JsonParser("/home/evyatar/logs/siri.schedule.5.Monday.json.2018-08-13")).parseJson()
    val allLines3 : List[BusLineData] = (new JsonParser("/home/evyatar/logs/siri.schedule.3.Monday.json.2018-08-13")).parseJson()
    val allLines4 : List[BusLineData] = (new JsonParser("/home/evyatar/logs/siri.schedule.16.Monday.json.2018-08-13")).parseJson()   // takes about 2 hours!
    //val allLines5 : List[BusLineData] = (new JsonParser("/home/evyatar/logs/siri.schedule.json")).parseJson()
    val allLines5 : List[BusLineData] = (new JsonParser("/home/evyatar/logs/siri.schedule.misc.Monday.json.2018-08-13")).parseJson()
    val allLines6 : List[BusLineData] = (new JsonParser("/home/evyatar/logs/siri.schedule.misc2.Tuesday.json.2018-08-14")).parseJson()
    val allLines0 : Array[List[BusLineData]] = Array(null, allLines1, allLines2, allLines3, allLines4, allLines5, allLines6)
    logger.info("reading json... completed")



    // replace makat="unknown with makat=lineShortName
    val allLines = allLines0(scheduleFileToAnalyze).map(busLine => busLine match {
        case BusLineData(activeRange, "unknown", lineShortName, description, executeEvery,
                        lineRef, previewInterval, stopCode, weeklyDepartureTimes, lastArrivalTimes, maxStopVisits) =>
            BusLineData(activeRange, lineShortName,lineShortName ,description,executeEvery,lineRef,previewInterval,stopCode,weeklyDepartureTimes, lastArrivalTimes, maxStopVisits)
        case _ => busLine
      }
    )
      .filter(b => (filterBusLines.contains( b.lineShortName ) || filterBusLines.isEmpty))

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
        logger.debug(s"${counter*100/total}% completed")

        (makat, results)
      }
    val unique = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC).toString
    val date = LocalDate.now().toString
    new JsonParser(s"/tmp/busLinesAnalysis.$date.$unique.json").writeToFile(resultsByMakat)

    ap.shutdownThreads()
  }

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

