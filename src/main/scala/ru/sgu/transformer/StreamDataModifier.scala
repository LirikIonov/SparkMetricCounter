package ru.sgu.transformer

import java.beans.Transient

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import ru.sgu.BikeSet
import ru.sgu.io.DataWriter
import ru.sgu.spark.{SparkMetricConfig, SparkMetricConfigBuilder}

object StreamDataModifier {
		@Transient
		implicit val ss: SparkSession = SparkSession.builder()
				.appName("spark")
				.getOrCreate()
		@Transient
		implicit val config: SparkMetricConfig = SparkMetricConfigBuilder.create
		val bucket = config.bucketName
		ss.conf.set("temporaryGcsBucket", bucket)
		val projectID: String = config.projectId
		val topic: String = config.topicName
		val subcription: String = config.subscriptionName
		val windowLength: Int = config.windowLength
		val slidingInterval: Int = config.slidingInterval


		import ss.implicits._

		def process(x: DStream[String], dw: DataWriter, metrics: MetricCalculator): Unit = {
				x.map((line: String) => {
						val l = line.indexOf("\"\"")
						if (l == -1) line.replaceAll("\"", "")
						else {
								val r = line.substring(l + 1).indexOf("\"\"")
								val sub = line.substring(l, r + l).replaceAll(",", "")
								val res = line.substring(0, l) + sub + line.substring(r)
								res.replaceAll("\"", "")
						}
				})
						.map((line: String) => {
								var res = line + ""
								if (line.lastIndexOf(",") == line.length - 1) res = res + "NOVAL"
								res.replaceAll(",,", ",NOVAL,")
						})
						.map(line => line.split(",").map(_.trim).filter(!_.isEmpty))
						.filter(row => row(0).matches("\\d+"))
						.map {
								case Array(tripId, startTime, endTime,
								bikeId, tripDuration, fromStationId,
								fromStationName, toStationId, toStationName,
								userType, "NOVAL", "NOVAL") => {
										println(s"1: $tripId $startTime $endTime $bikeId $tripDuration $fromStationId $fromStationName $toStationId $toStationName $userType")
										BikeSet(BigDecimal(tripId), startTime, endTime,
												BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
												fromStationName, BigDecimal(toStationId), toStationName,
												userType, null, null)
								}
								case Array(tripId, startTime, endTime,
								bikeId, tripDuration, fromStationId,
								fromStationName, toStationId, toStationName,
								userType, genderType, "NOVAL") => {
										println(s"2: $tripId $startTime $endTime $bikeId $tripDuration $fromStationId $fromStationName $toStationId $toStationName $userType $genderType")
										BikeSet(BigDecimal(tripId), startTime, endTime,
												BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
												fromStationName, BigDecimal(toStationId), toStationName,
												userType, genderType, null)
								}
								case Array(tripId, startTime, endTime,
								bikeId, tripDuration, fromStationId,
								fromStationName, toStationId, toStationName,
								userType, "NOVAL", birthYear) => {
										println(s"3: $tripId $startTime $endTime $bikeId $tripDuration $fromStationId $fromStationName $toStationId $toStationName $userType $birthYear")
										BikeSet(BigDecimal(tripId), startTime, endTime,
												BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
												fromStationName, BigDecimal(toStationId), toStationName,
												userType, null, birthYear)
								}
								case Array(tripId, startTime, endTime,
								bikeId, tripDuration, fromStationId,
								fromStationName, toStationId, toStationName,
								userType, genderType, birthYear) =>
										println(s"4: $tripId $startTime $endTime $bikeId $tripDuration $fromStationId $fromStationName $toStationId $toStationName $userType $genderType $birthYear")
										BikeSet(BigDecimal(tripId), startTime, endTime,
												BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
												fromStationName, BigDecimal(toStationId), toStationName,
												userType, genderType, birthYear)
								case line => {
										println(s"5: $line")
										BikeSet(null, null, null, null, null, null, null, null, null, null, null, null)
								}
						}
						.filter(row => row != null)
						.window(Seconds(windowLength), Seconds(slidingInterval))

						.foreachRDD(rdd => if (!rdd.isEmpty()) {
								dw.writeToStorage(rdd.toDF())
								metrics.calcAggregate(rdd)
								metrics.calcTopRentAddresses(rdd, "fromStationName", "from", "top_rent_from_addresses")
								metrics.calcTopRentAddresses(rdd, "toStationName", "to", "top_rent_to_addresses")
						})
		}
}
