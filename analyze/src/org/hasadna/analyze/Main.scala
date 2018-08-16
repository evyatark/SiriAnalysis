package org.hasadna.analyze

/**
  * To run this analysis: scala org.hasadna.analyze.Main
  * (or from IDE)
  *
  * requirements:
  * -Dsiri.results.dir=<dir of result files>
  * -Dsiri.month.analysis=<month to analyze (2 digits)>
  *
  *   siri.results.dir should contain all result files (decompressed).
  *   The naming convention is "siri_rt_data.2018-07-01.0.log"
  *
  *   The lines that are analyzed are hard coded in method allLines() below.
  *
  *   Processing takes a long time!
  *
  */
object Main {
  val filesLocation = System.getProperty("siri.results.dir", "/home/evyatar/logs/work/" );
  val monthToAnalyze = System.getProperty("siri.month.analysis", "07");

  val ignoreTheseTimes = List("2018-07-02T20:01 to 2018-07-02T22:59")

  def main(args: Array[String]): Unit = {
    val ap = new AnalyzeDepartures(filesLocation , monthToAnalyze )
    ap.findAllLinesOnDay(16).foreach(println)
    ap.processAll(allLines()).foreach(println)
  }



  def allLines() : List[(String, String)] = {
    List(
      //63
      //("63", "2517"),("63", "2519"),
      // 26
      ("26", "3093"),
      ("","")
    )

  }


  // TODO use file siri.schedule.json to build this list
  def allLines1() : List[(String, String)] = {
    //List( "419", "420", "497", "597", "480", "415", "416", "417", "418", "840", "394") // 7, 14, 15 exist for both BS and Jer
    List(
      //63
      ("63", "2517"),("63", "2519"),
      // BS
      ("7", "16212"),("7", "16211"),  // 7 BS
      ("10", "15438"),("10", "15439"),  // 10 BS
      ("14", "15495"),("14", "15494"),  // 14 BS
      ("15", "15541"), ("15", "15540"),// 15 BS
      // inter-city
      ( "415","8552"), ( "415","15527"),
      ( "416","15529"),
      //( "417",""),
      ( "418","6672"),
      ( "419","16067"),
      ( "480","7020"),( "480","7023"),
      ( "840","19732"),( "840","7117"),
      ( "394","7453"),
      ( "497","15665"),( "497","15663"),( "497","15664"),
      ( "597","15451"),( "597","15454"),( "597","15452"),
      // Jer
/*
      ("1", "12871"),("1", "10379"),("1", "10381"),("1", "10383"),("1", "10384"),("1", "10747"),("1", "10746"),("1", "12112"),("1", "13435"),
      ("3", "10389"),("3", "10391"),("3", "10393"),("3", "22860"),("3", "22898"),
      ("4", "9833"),("4", "12400"),("4", "9834"),("4", "12401"),
      ("5","12439"),("5","9838"),("5","1695"),("5","10866"),("5","10865"),
      ("6", "12403"),("6", "12404"),
      ("7", "10394"),("7", "10395"),("7", "10396"),("7", "16665"),
      ("9","11108"), ("9","11107"),
      ("10","10795"), ("10","10796"),
      ("12","10228"),("12","10229"), ("12","10230"), ("12","10232"),
      ("13","22924"),
      ("14","10179"), ("14","10180"),
      ("15","12405"), ("15","12406"),
      ("16","5442"),("16","5443"),("16","5444"),("16","5446")

      ("17","17361"),("17","10398"),("17","17362"),("17","10399"),
      ("18","10799"),("18","10797"),
      ("19", "10802"), ("19", "15085"), ("19", "11806"), ("19", "10804"),("19", "10807"),("19", "10806"),("19", "15086"), // 19 Jer
      ("19א", "10801"), // 19א

      //20,21,22,23, 24, 25, 26, 27, 28  ,29, 30

      ("20","10181"),      ("20","10182"),      ("20","10184"),      ("20","10185"),
      ("21","12378"),("21","12379"),("21","12870"),
      ("22","5499"),("22","5502"),
      ("23","10186"),("23","10187"),("23","10188"),
      ("24","10189"),("24","10193"),("24","10195"),("24","10197"),
      ("25","5526"),("25","5528"),("25","5530"),("25","2375"),
      ("26","10201"),
      ("27","10203"),("27","10204"),("27","10205"),("27","10207"),
      ("28","10208"),("28","10209"),("28","10211"),("28","21807"),("28","21808"),
      ("29","12407"),("29","12408"),("29","12409"),
      ("30","12419"),("30","12421"),

      ("31", "10403"),("31", "10400"),("31", "10402"),("31", "10401"),  // 31 Jer

      // 32, 33
      ("34", "12425"), ("34", "12424"),("34", "17370"),("34", "17371"), // 34 Jer
*/
      ("","")
    )
  }



/*
  def main1(args: Array[String]): Unit = {
    val fileLocation = "/home/evyatar/logs/work" +"/" //+ "siri_rt_data.2018-06-28.0.log"
    //val fileNames = (8 to 30).map(day => s"siri_rt_data.2018-06-${day}.0.log").sorted

    val month = "07"
    val days = (1 to 31)
    for ((line, route) <- allLines()) {
      println(s"  line $line  (route $route)")
      //val lf = days.map(day =>
      for (day <- days) {
        //val f =
          //Future {
            //println(s"started day $day in thread " + Thread.currentThread().getName )
            val dayNameOption = Try(
              LocalDate.parse(s"2018-${month}-" + f"${day}%02d").getDayOfWeek.toString
            ).toOption
            if (dayNameOption.isDefined) {
              val dayName = dayNameOption.get
              val all: List[Reading] = getLinesForDate(month, day, fileLocation)
              //println(s" processed day $day, found ${all.size} lines")
              val (aimedDepartures, missingDepartures, lateDepartures) = findMissingDeparturesForLine(line, route, all)
              if (!aimedDepartures.isEmpty) {
                print(dayName + s"  2018-${month}-" + f"${day}%02d" + "\t\t") //+ s"  line $line  (route $route)")
                //println(s"=======================================")
                println(displayProblems(line, route, aimedDepartures, missingDepartures, lateDepartures))
              }
            }
          //}).toList
//        Future.sequence(lf). onComplete {
//          case Success(n) => "OK"
//          case Failure(x) => "not OK"
        }

      println("\n"*3)
    }
  }
*/

}



