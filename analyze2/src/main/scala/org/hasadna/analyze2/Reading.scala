package org.hasadna.analyze2

import java.time.LocalDateTime

case class Reading (
                     localDateTime: LocalDateTime,  // 2018-06-28T23:59:33.366
                     lineShortName : String,  // 415
                     routeId : String,
                     description : String,    // [line 415 v 9764032 oad 23:15 ea 00:00]
                     aimedDeparture : String, // 20:30
                     latitude : String,
                     longitude : String
                   )