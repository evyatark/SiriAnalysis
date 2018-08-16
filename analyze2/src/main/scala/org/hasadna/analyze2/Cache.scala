//package org.hasadna.analyze2
//
//import scalacache.redis.RedisCache
//import scalacache._
//import scalacache.modes.try_._
//
//class Cache {
//
//  implicit val redisCache =  RedisCache("172.17.0.4", 6379)
//
//  def f() = {
//    val ericTheCat = Cat(1, "Eric", "tuxedo")
//    put("eric")(ericTheCat)
//
//    val x = get("eric")
//
//    println(s"x=$x")
//  }
//
//  final case class Cat(id: Int, name: String, colour: String)
//}
