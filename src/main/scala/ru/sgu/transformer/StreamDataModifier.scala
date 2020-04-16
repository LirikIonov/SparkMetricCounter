package ru.sgu.transformer

import java.beans.Transient

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import ru.sgu.BikeSet
import ru.sgu.io.DataWriter
import ru.sgu.spark.SparkMetricConfigBuilder

import scala.util.Try

object StreamDataModifier extends Logging {
  @Transient
  implicit val ss = SparkSession.builder()
    .appName("spark")
    .getOrCreate()
  @Transient
  implicit val config = SparkMetricConfigBuilder.create
  val bucket = config.bucketName
  ss.conf.set("temporaryGcsBucket", bucket)
  val projectID = config.projectId
  val topic = config.topicName
  val subcription = config.subscriptionName
  val windowLength = config.windowLength
  val slidingInterval = config.slidingInterval

  def process(dataStream: DStream[String])(implicit dw: DataWriter, metrics: MetricCalculator): Unit = {
    write(transform(dataStream))
  }

  def transform(dataStream: DStream[String]): DStream[BikeSet] = {
    //x.foreachRDD(rdd => rdd.collect.foreach(println))
    dataStream.map((line: String) => {
      var res = ""
      val l = line.indexOf("\"\"")
      if (l == -1) res = line.replaceAll("\"", "")
      else {
        val r = line.substring(l + 1).indexOf("\"\"")
        val sub = line.substring(l, r + l + 1).replaceAll(",", "")
        res = (line.substring(0, l) + sub + line.substring(r + l + 1)).replaceAll("\"", "")
      }

      if (line.lastIndexOf(",") == line.length - 1) res = res + "NOVAL"
      res
        .replaceAll(",,", ",NOVAL,")
        .split(",")
        .map(_.trim)
        .filter(!_.isEmpty)
    })
      .filter(row => row(0).matches("\\d+"))
      .map {
        case Array(tripId, startTime, endTime,
        bikeId, tripDuration, fromStationId,
        fromStationName, toStationId, toStationName,
        userType, "NOVAL", "NOVAL") => {
          //println(s"1: $tripId $startTime $endTime $bikeId $tripDuration $fromStationId $fromStationName $toStationId $toStationName $userType")
          Try(BikeSet(BigDecimal(tripId), startTime, endTime,
            BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
            fromStationName, BigDecimal(toStationId), toStationName,
            userType, null, null)).toOption
        }
        case Array(tripId, startTime, endTime,
        bikeId, tripDuration, fromStationId,
        fromStationName, toStationId, toStationName,
        userType, genderType, "NOVAL") => {
          //println(s"2: $tripId $startTime $endTime $bikeId $tripDuration $fromStationId $fromStationName $toStationId $toStationName $userType $genderType")
          Try(BikeSet(BigDecimal(tripId), startTime, endTime,
            BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
            fromStationName, BigDecimal(toStationId), toStationName,
            userType, genderType, null)).toOption
        }
        case Array(tripId, startTime, endTime,
        bikeId, tripDuration, fromStationId,
        fromStationName, toStationId, toStationName,
        userType, "NOVAL", birthYear) => {
          //println(s"3: $tripId $startTime $endTime $bikeId $tripDuration $fromStationId $fromStationName $toStationId $toStationName $userType $birthYear")
          Try(BikeSet(BigDecimal(tripId), startTime, endTime,
            BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
            fromStationName, BigDecimal(toStationId), toStationName,
            userType, null, birthYear)).toOption
        }
        case Array(tripId, startTime, endTime,
        bikeId, tripDuration, fromStationId,
        fromStationName, toStationId, toStationName,
        userType, genderType, birthYear) =>
          //println(s"4: $tripId $startTime $endTime $bikeId $tripDuration $fromStationId $fromStationName $toStationId $toStationName $userType $genderType $birthYear")
          Try(BikeSet(BigDecimal(tripId), startTime, endTime,
            BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
            fromStationName, BigDecimal(toStationId), toStationName,
            userType, genderType, birthYear)).toOption
        case _ => None
      }
      .filter(r => r.isDefined)
      .map(r => r.get)
      .window(Seconds(windowLength), Seconds(slidingInterval))
  }

  import ss.implicits._

  def write(bikeStream: DStream[BikeSet])(implicit dw: DataWriter, metrics: MetricCalculator): Unit = {
    bikeStream.foreachRDD(rdd => if (!rdd.isEmpty()) {
      dw.writeToStorage(rdd.toDF())
      metrics.calcAggregate(rdd)
      metrics.calcTopRentAddresses(rdd, "fromStationName", "from", "top_rent_from_addresses")
      metrics.calcTopRentAddresses(rdd, "toStationName", "to", "top_rent_to_addresses")
    })
  }
}
