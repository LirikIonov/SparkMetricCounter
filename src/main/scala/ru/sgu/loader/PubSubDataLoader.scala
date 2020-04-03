package ru.sgu.loader

import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import ru.sgu.BikeSet
import ru.sgu.config.SparkMetricConfig
import ru.sgu.logger.BaseLogger

class PubSubDataLoader(implicit sc: StreamingContext, config: SparkMetricConfig) extends BaseLogger {
		val projectID: String = config.projectId
		val topic: String = config.topicName
		val subcription: String = config.subscriptionName
		val windowLength: Int = config.windowLength
		val slidingInterval: Int = config.slidingInterval

		def loadDataAsStream() = {
				logger.info("Loading bike set as stream from Pub/Sub")
				val messagesStream: DStream[String] = PubsubUtils
						.createStream(
								sc,
								projectID,
								Option(topic),
								subcription,
								SparkGCPCredentials.builder.build(),
								StorageLevel.MEMORY_AND_DISK_SER_2)
						.map(message => new String(message.getData(), StandardCharsets.UTF_8))

				// Debug
				//messagesStream.foreachRDD(rdd => rdd.collect().foreach(println))

				val x = messagesStream
						.map((line: String) => {
								val l = line.indexOf("\"\"")
								if (l == -1) line.replaceAll("\"", "")
								else {
										val r = line.substring(l).indexOf("\"\"")
										val sub = line.substring(l, r).replaceAll(",", "")
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
										BikeSet(BigDecimal(tripId), startTime, endTime,
												BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
												fromStationName, BigDecimal(toStationId), toStationName,
												userType, null, null)
								}
								case Array(tripId, startTime, endTime,
								bikeId, tripDuration, fromStationId,
								fromStationName, toStationId, toStationName,
								userType, genderType, "NOVAL") => {
										BikeSet(BigDecimal(tripId), startTime, endTime,
												BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
												fromStationName, BigDecimal(toStationId), toStationName,
												userType, genderType, null)
								}
								case Array(tripId, startTime, endTime,
								bikeId, tripDuration, fromStationId,
								fromStationName, toStationId, toStationName,
								userType, "NOVAL", birthYear) => {
										BikeSet(BigDecimal(tripId), startTime, endTime,
												BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
												fromStationName, BigDecimal(toStationId), toStationName,
												userType, null, birthYear)
								}
								case Array(tripId, startTime, endTime,
								bikeId, tripDuration, fromStationId,
								fromStationName, toStationId, toStationName,
								userType, genderType, birthYear) =>
										BikeSet(BigDecimal(tripId), startTime, endTime,
												BigDecimal(bikeId), BigDecimal(tripDuration), BigDecimal(fromStationId),
												fromStationName, BigDecimal(toStationId), toStationName,
												userType, genderType, birthYear)
								case _ => null
						}
						.filter(row => row != null)
						.window(Seconds(windowLength), Seconds(slidingInterval))

				/*x.foreachRDD(rdd => {
						println("tripId")
						rdd.map(row => row.tripId).collect().foreach(println)
						println("===\nstartTime")
						rdd.map(row => row.startTime).collect().foreach(println)
						println("===\nendTime")
						rdd.map(row => row.endTime).collect().foreach(println)
						println("===\nbikeId")
						rdd.map(row => row.bikeId).collect().foreach(println)
						println("===\ntripDuration")
						rdd.map(row => row.tripDuration).collect().foreach(println)
						println("===\nfromStationId")
						rdd.map(row => row.fromStationId).collect().foreach(println)
						println("===\nfromStationName")
						rdd.map(row => row.fromStationName).collect().foreach(println)
						println("===\ntoStationId")
						rdd.map(row => row.toStationId).collect().foreach(println)
						println("===\ntoStationName")
						rdd.map(row => row.toStationName).collect().foreach(println)
						println("===\nuserType")
						rdd.map(row => row.userType).collect().foreach(println)
						println("===\ngenderType")
						rdd.map(row => row.genderType).collect().foreach(println)
						println("===\nbirthYear")
						rdd.map(row => row.birthYear).collect().foreach(println)
				})*/
				//println("===\nTOTAL")
				//x.foreachRDD(rdd => rdd.collect().foreach(println))
				x
		}
}
