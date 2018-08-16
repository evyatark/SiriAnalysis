package org.hasadna.analyze

import java.io.File
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.concurrent.Executors

import org.hasadna.analyze.Main._

import scala.concurrent.ExecutionContext
//import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.{Failure, Success, Try}

class AnalyzeDepartures(val filesLocation : String, val month : String) {

  // we limit to 2 concurrent threads, because each thread reads a whole file to memory
  // more threads cause OutOfMemory error or Heap error
  // in general, each thread needs about 2GB memory so for 2 threads
  // you should add -Xmx2g to the java command line
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

  // TODO use content of siri.schedule.json to enrich description here
  def processAll(allLines : List[(String, String)]) : List[String] = {
    val days = (1 to 31)
    def produce() = {
      val results: List[Future[List[String]]] = for ((line, route) <- allLines) yield {
        Future {
          processLine(line, route, days)
        }
      }
      Future.sequence(results)
    }
    Await.result(produce(), Duration.Inf).flatten

  }

  def processAllSequentialy() : List[String] = {
    val days = (1 to 31)
    val results : List[List[String]] = for ((line, route) <- Main.allLines()) yield {
      processLine(line, route, days)
    }
    results.flatten
  }

  def processLine(line: String, route: String, days : Range) : List[String] = {
    print(s"$line($route) ")
    val resultsForAllDays =
      for (day <- days)  yield {
        processDay(line: String, route: String, day : Int)
      }
    println("")
    s"\n\nLine $line (route $route)\n" ::
      resultsForAllDays.flatten.toList  //.map(s => s + "\n")
  }


  def findAllLinesOnDay(day : Int) : Set[(String, String)] = {
    val all: List[Reading] = getLinesForDate(month, day, filesLocation)
    all.map(r => (r.lineShortName, r.routeId)).toSet
  }


  def processDay(line: String, route: String, day: Int) : Option[String] = {
    print(".")
    val dayName = createDayName(day)
    dayName.flatMap( dayName => {
      val all: List[Reading] = getLinesForDate(month, day, filesLocation)
      val (aimedDepartures, missingDepartures, lateDepartures) = findMissingDeparturesForLine(line, route, all)

      val header = (dayName + s"  2018-${month}-" + f"${day}%02d" + "\t\t")
      val body = if (!aimedDepartures.isEmpty) {
        Some(displayProblems(line, route, aimedDepartures, missingDepartures, lateDepartures))
      } else None
      val ret = body match {
        case Some(body) => Some(header + body)
        case None => None
      }
      ret
    } )
  }


  def displayProblems(lineShortName: String, routeId : String,
                      aimedDepartures: List[String], missingDepartures: List[String], lateDepartures: List[String]): String = {
    if (missingDepartures.isEmpty)
      s"OK (all ${aimedDepartures.size} departures)"
    else
      s"${missingDepartures.size} missing departures (out of ${aimedDepartures.size} aimed departures)  ---  $missingDepartures"
  }

  def createDayName(day: Int) : Option[String] = {
    Try(
      LocalDate.parse(s"2018-${month}-" + f"${day}%02d").getDayOfWeek.toString
    ).toOption
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
        print(s"\n\n$dep\t")
        print(s"======> missing!")
        println("\n")
      }
      else if (lateDepartures.contains(dep)) {
        print(s"\n$dep\t")
        print(s"================> late (by more than 3 minutes)")
        println("")
      }
      else {
        print(s"$dep\t")
      }
    }
    println("")
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

  def getLinesForDate(month : String, day : Int, fileLocation : String) : List[Reading] = {
    //println(s"processing day $day")
    val files : List[String] = findFiles(day, month, fileLocation)
    var all : ListBuffer[Reading] = ListBuffer.empty
    for (file <- files) {
      val content = Source.fromFile(file).getLines().toList
      val allInFile: List[Reading] = content.toList
        .filter(line => !line.isEmpty)
        .map(line => parseLine(line))
        .filter(re => re.localDateTime.toLocalTime.isAfter(LocalTime.parse("03:00")))
      all ++= allInFile
    }
    all.toList
  }



  def findMissingDeparturesForLine(lineShortName : String, routeId : String, all : List[Reading], intervalForLateInMinutes : Int = 5): (List[String], List[String], List[String]) = {

    val allForLine : List[Reading] = all.filter(r => r.lineShortName.equals(lineShortName) && r.routeId.equals(routeId))

    val aimedDepartures = findAllAimedDepartures(allForLine).toList.sortBy(s => s)
    val missingDepartures : ListBuffer[String] = ListBuffer.empty[String]
    val lateDepartures : ListBuffer[String] = ListBuffer.empty[String]
    for (dep <- aimedDepartures) {
      val depTime = LocalTime.parse(dep)
      val firstReadingInMotion : List[Reading] = allForLine.filter(!_.latitude.equals("0")).filter(r => r.aimedDeparture.equals(dep)).take(1)
      if (firstReadingInMotion.isEmpty) {
        //println(s"\ndeparture $dep does not exist!")
        missingDepartures += dep ;
      }
      //else println(firstReadingInMotion)
      else {
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

    (aimedDepartures, missingDepartures.toList, lateDepartures.toList)

  }



  def filterOnlyReadingsOfDeparture(all : List[Reading], departureTime : String) : List[Reading] = {
    all.filter(_.aimedDeparture.equals(departureTime)).sortBy(_.localDateTime.toString)
  }

  def shouldNotBeIgnored(localDateTime: String ): Boolean = {
    val result = ignoreTheseTimes.filter(timeRange => contains(timeRange, localDateTime)).toList.isEmpty
    //if (!result) println(localDateTime)
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

{
    "lineShortName":"14",
    "lineDescription":"קו 14 -- בית שמש",
    "routeId":"15494",
    [
        {
            "dayOfWeek":"Sunday",
            "date":"2018-07-01",
            "late":[],
            "missing":["07:40","09:50","12:10","18:10","20:10","21:45"]
            "aimed":["06:25","06:35","06:50","06:58","07:06","07:14","07:22","07:30","07:40","07:55","08:10","08:30","08:50","09:10","09:30",
            "09:50","10:10","10:30","10:50","11:10","11:30","11:50","12:10","12:25","12:35","12:45","12:55","13:10","13:25","13:40","13:55",
            "14:10","14:25","14:40","14:55","15:10","15:25","15:40","15:55","16:10","16:25","16:40","16:55","17:10","17:25",
            "17:40","17:55","18:10","18:25","18:40","18:55","19:10","19:25","19:40","19:55","20:10","20:25","20:45","21:10","21:25",
            "21:45","22:10","22:25","22:45","23:30"],
            "totals":[65, 6, "9.2%"]
        }
        {
            "dayOfWeek":"Monday",
            "date":"2018-07-02",
            "late":[],
            "missing":["15:40", "16:40", "18:25"],
            "aimed":["06:25","06:35","06:50","06:58","07:06","07:14","07:22","07:30","07:40","07:55","08:10","08:30","08:50","09:10","09:30","09:50","10:10","10:30",
            "10:50","11:10","11:30","11:50","12:10","12:25","12:35","12:45","12:55","13:10","13:25","13:40","13:55","14:10","14:25","14:40","14:55","15:10","15:25",
            "15:40","15:55","16:10","16:25","16:40","16:55","17:10","17:25","17:40","17:55","18:10","18:25","18:40",
            "18:55","19:10","19:25","19:40","19:55","20:10","20:25","20:45","21:10","22:10","22:25","22:45],
            "totals":[58, 3, "5.1%"]
        }
    ]
}

 */