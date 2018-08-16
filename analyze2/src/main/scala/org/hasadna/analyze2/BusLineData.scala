package org.hasadna.analyze2


case class BusLineData(activeRanges : String,
                       makat: String,
                       lineShortName : String,
                       description: String,
                       executeEvery: String,
                       lineRef: String,
                       previewInterval: String,
                       stopCode: String,
                       weeklyDepartureTimes : Map[String, List[String]],
                       lastArrivalTimes : Map[String, String],
                       maxStopVisits: String) {


}