package com.webex.avwatch.scala

object ScalaDemo {
  def main(args: Array[String]): Unit = {
    val map = Map("Jack"->30,"Paul"->25)
    map foreach println
    
    val jack = map.get("Jack")
    println(jack) // Some
    println(map("Jack")) // 30
    
    for((name,age) <- map) {
      // println(s"name = $name , age = $age")
      print(name)
    }
    
    
  }
}