package com.cjy.sparkcore.T11_project

object LogTests {
  def main(args: Array[String]): Unit = {
    val list: List[String] = List[String]("1","2","3")
    val index: List[(String, Int)] = list.zipWithIndex
    println(index)
    val map: Map[String, Int] = index.toMap
    println(map)

    val s2: List[String] = list.slice(0,1)
    println(list.zip(index))
  }
}
