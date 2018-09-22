package org.hasadna.analyze3

import java.io.File

import org.hasadna.analyze2.{BusLineData, DeparturesAnalysisResult, JsonParser}
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Component

@Component
class Analyze {

  val logger : Logger = LoggerFactory.getLogger(getClass.getName)

  @Value("${siri.results.dir}")
  val location : String = ""

  @Value("${siri.month.analysis:08}")
  val monthToAnalyze = ""

  @Value("${schedule.files.dir}")
  val schedulesLocation : String = ""

  @Value("${schedule.file.name.prefix:siri.schedule.}")
  val scheduleFileNamePrefix = ""

  // TODO move to application.properties
  val allAgencies = List(16) //List(3,4,5,7,8,14,15,16,18,19,25,30,31,32)

  @Autowired
  val adp : AnalyzeDepartures = null

  def analyze() : Unit = {
    logger.info(s"$location $monthToAnalyze")
    //val busLinesToAnalyze : List[BusLineData] = useScheduleFiles()
    val busLinesToAnalyze : List[BusLineData] = useScheduleFiles(2018, monthToAnalyze)
    logger.info(s"${busLinesToAnalyze.size} bus lines will be analyzed, for month $monthToAnalyze")

    val enrichedBusLinesToAnalyze : List[BusLineData] = enrichAndFilterBusLines(busLinesToAnalyze)
    logger.info(s"${busLinesToAnalyze.size} bus lines will be analyzed, for month $monthToAnalyze")

    val lineNames : List[String] = enrichedBusLinesToAnalyze.map(b => b.lineShortName).distinct

    val allLinesGroupedByMakat : Map[String, List[BusLineData]] = enrichedBusLinesToAnalyze.groupBy(_.makat)

    logger.info(s"processing ${allLinesGroupedByMakat.keySet.size} bus lines ($lineNames)")

    val ap : AnalyzeDepartures = createAnalyzer(
                                      enrichedBusLinesToAnalyze,
                                      allLinesGroupedByMakat,
                                      lineNames,
                                      List.empty
                                    )
    logger.info(s"processing ${ap.makatsDescription.keySet.size} makats")
    val x : List[DeparturesAnalysisResult] = ap.processAllByDay(enrichedBusLinesToAnalyze)

    logger.info(s"results: ${ap.toJson(x)}")
  }

  def findDescriptions(allLinesGroupedByMakat: Map[String, List[BusLineData]]) : Map[String, String] = {
    for ((makat, routes) <- allLinesGroupedByMakat) yield {
      val longDescription = routes(0).description.split("Direction")(0)
      (makat, longDescription)
    }
  }


  def createAnalyzer(enrichedBusLinesToAnalyze : List[BusLineData],
                     allLinesGroupedByMakat : Map[String, List[BusLineData]],
                     lineNames : List[String],
                     ignoreTheseTimes : List[String]) : AnalyzeDepartures = {
    val ap = adp
    ap.init(location, ignoreTheseTimes);
    ap.routesDescription = enrichedBusLinesToAnalyze.map(busLine => (busLine.lineRef -> busLine.description)).toMap

    logger.info(s"processing ${allLinesGroupedByMakat.keySet.size} bus lines ($lineNames)")

    ap.makatsDescription = findDescriptions(allLinesGroupedByMakat)

    ap
  }

  def enrichAndFilterBusLines(busLines : List[BusLineData]) : List[BusLineData] = {
    busLines.map(busLine =>
      busLine match {
          // if makat is unknown, use the short name instead
        case BusLineData(activeRange, "unknown", lineShortName, description, executeEvery,
        lineRef, previewInterval, stopCode, weeklyDepartureTimes, lastArrivalTimes, maxStopVisits) =>
          BusLineData(activeRange, lineShortName,lineShortName ,description,executeEvery,lineRef,previewInterval,stopCode,weeklyDepartureTimes, lastArrivalTimes, maxStopVisits)
        case _ => busLine
      }
    )//.filter(b => List("420").contains(b.lineShortName))
    // filter bus lines to get only a small list according to the filters
//      .filter(b => (filterBusLines.contains( b.lineShortName ) || filterBusLines.isEmpty))
//      .filter(b => (filterMakats.contains(b.makat) || filterMakats.isEmpty))

  }

  /**
    * Parse all schedule files (of the specified agencies) and find data
    * about the bus lines that should be analyzed.
    * The schedule files are created by GTFS Reader every day at 3:00 AM
    * and are used by SiriRetriever of that day
    *
    * A typical BusLineData looks like this:
    *
      {
        "description" : " --- קו 537 מבועיינה נוג'יידאת אל איזורית עמק הירדן  --- Makat 10537  --- Direction 1  --- Alternative #  ------  Sunday  ------  null",
        "makat" : "10537",
        "lineShortName" : "537",
        "stopCode" : "52497",
        "previewInterval" : "PT2H",
        "lineRef" : "20073",
        "maxStopVisits" : "7",
        "executeEvery" : "60",
        "activeRanges" : null,
        "weeklyDepartureTimes" : {
          "SUNDAY" : [ "06:35", "08:45", "10:35" ],
          "MONDAY" : [ "06:35", "08:45", "10:35" ],
          "TUESDAY" : [ "06:35", "08:45", "10:35" ],
          "WEDNESDAY" : [ "06:35", "08:45", "10:35" ],
          "THURSDAY" : [ "06:35", "08:45", "10:35" ],
          "FRIDAY" : [ ],
          "SATURDAY" : [ ]
        },
        "lastArrivalTimes" : {
          "SUNDAY" : "11:25:29"
        }
      }
    *
    * @return list of all the bus lines found in the schedule files.
    *         Each item in the list is BusLineData (makat, shortName, departure times, etc
    */
  def useScheduleFiles() : List[BusLineData] = {
    // find schedules of specified agency(ies) in the month specified in properties file,
    // using only schedule files of the specified date
    parseSchedules("2018-08-21", allAgencies.map(_.toString))
  }

  def useScheduleFiles(year:Int, month:String) : List[BusLineData] = {
    val list : List[BusLineData] = (1 to 7)
        .map(day => "0" + day.toString)
        .flatMap(day => parseSchedules(s"$year-$month-$day", allAgencies.map(_.toString))).toList
    list
  }

  def parseSchedules(lastDate : String, agencies : List[String]) : List[BusLineData] = {
    // search schedule files in directory $schedulesLocation (val dir = "/home/evyatar/logs/schedules/")
    //val prefix = "siri.schedule."
    //    val lastDate = "2018-08-22"
    //    val agencies = List(18, 5)
    val allFiles : List[String] =
      agencies.flatMap(agency => {
        val fileName = scheduleFileNamePrefix + agency.toString + ".*" + ".json." + lastDate
        val files = new File(schedulesLocation).listFiles().map(file => file.getName)
          .filter(name => name.startsWith(scheduleFileNamePrefix + agency.toString + ".") && name.endsWith(".json." + lastDate))
          .map(name => schedulesLocation + name).toList
        files
      })
    allFiles.flatMap(fileName => (new JsonParser(fileName)).parseJson())
  }

}
