package org.hasadna.analyze3

import java.io.File
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import javax.annotation.PostConstruct
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.{ApplicationContext, ConfigurableApplicationContext}
import org.springframework.stereotype.Service

@Service
class Main {

  @Autowired
  val ctx : ConfigurableApplicationContext = null

  @Autowired
  val analyze : Analyze = null

  val logger : Logger = LoggerFactory.getLogger(getClass.getName)

  @PostConstruct
  def preStart(): Unit = {
    logger.info("analyze3 constructed...")
  }

  def doingManyThings(): Unit = {
    for(i <- 1 to 3) {
      Thread.sleep(5000)
    }
  }

  def start(args: Array[String]): Unit = {
    logger.info("analyze3 starting...")
    analyze.analyze()
    doingManyThings()
    closeApplication
  }


  def closeApplication(): Unit = {
    logger.info("analyze3 stopping...")
    ctx.close()

  }


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

}

