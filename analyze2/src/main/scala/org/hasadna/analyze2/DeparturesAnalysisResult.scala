package org.hasadna.analyze2

case class DeparturesAnalysisResult (
  val makat : String,
  val lineShortName : String,
  val lineDescription : String,
  val routes : RouteAnalysisResult){}

case class RouteAnalysisResult (
  val routeId : String,
  val alternative : String,
  val direction : String,
  val routeDescription : String,
  val routeResults : List[RouteDailyResults],
  val routeTotals: RouteTotals){}

case class RouteDailyResults (
    val dayOfWeek : String,
    val date : String,
    val late : List[String],
    val latePercent : Int,
    val missing : List[String],
    val missingPercent : String,
    val aimed : List[String]
    //,val totals : RouteTotals
                        ){}

case class RouteTotals (
  val aimed : Int,
  val missing : Int,
  val percentMissing : String,
  val late : Int,
  val percentLate : String){}

/*
{
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
}
*/