package com.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class User (
  id: Option[Int],
  country: Option[String],
  points: Option[Int],
  title: Option[String],
  variety: Option[String]
)

object JsonReader {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      throw new IllegalArgumentException("Expected single argument! Run command: "
        + "spark-submit --class com.example.JsonReader <.jar> <.json>")
    }

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("JsonReader")
        .setMaster("local")
    );

    implicit val Formats: DefaultFormats.type = DefaultFormats

    sc.textFile(args(0)).
      collect().
      foreach(line => {
        val decodedUser = parse(line).extract[User]
        println(decodedUser)
      })
  }
}