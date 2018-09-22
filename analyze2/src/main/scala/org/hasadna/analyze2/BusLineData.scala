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

/**

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
  */