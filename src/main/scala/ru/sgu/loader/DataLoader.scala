package ru.sgu.loader

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import ru.sgu.BikeStats

class DataLoader(sqlContext: SQLContext, sc: SparkContext) {

		import sqlContext.sparkSession.implicits._

		def loadData(): Dataset[BikeStats] = {
				sqlContext.read.format("csv")
						.option("header","true")
						.option("delimiter", ",")
						.load("gs://ssu_practice_451_411/Divvy_Trips_2018_Q1.csv")

						.map((row: Row) => {
								row match {
										case Row(tripId: String, startTime: String, endTime: String,
										bikeId: String, tripDuration: String, fromStationId: String,
										fromStationName: String, toStationId: String, toStationName: String,
										userType: String, genderType: String, birthYear: String) =>
												BikeStats(BigDecimal(tripId), startTime, endTime,
														BigDecimal(bikeId), BigDecimal(tripDuration.replace(",", "")), BigDecimal(fromStationId),
														fromStationName, BigDecimal(toStationId), toStationName,
														userType, genderType, birthYear)

										/*case Row(tripId: String, startTime: String, endTime: String,
										bikeId: String, tripDuration: String, fromStationId: String,
										fromStationName: String, toStationId: String, toStationName: String,
										userType: String, _, birthYear: String) =>
												BikeStats(BigDecimal(tripId), startTime, endTime,
														BigDecimal(bikeId), BigDecimal(tripDuration.replace(",", "")), BigInt(fromStationId),
														fromStationName, BigDecimal(toStationId), toStationName,
														userType, "", birthYear)

										case Row(tripId: String, startTime: String, endTime: String,
										bikeId: String, tripDuration: String, fromStationId: String,
										fromStationName: String, toStationId: String, toStationName: String,
										userType: String, genderType: String, _) =>
												BikeStats(BigDecimal(tripId), startTime, endTime,
														BigDecimal(bikeId), BigDecimal(tripDuration.replace(",", "")), BigDecimal(fromStationId),
														fromStationName, BigDecimal(toStationId), toStationName,
														userType, genderType, "")*/

										case Row(tripId: String, startTime: String, endTime: String,
										bikeId: String, tripDuration: String, fromStationId: String,
										fromStationName: String, toStationId: String, toStationName: String,
										userType: String, _, _) =>
												BikeStats(BigDecimal(tripId), startTime, endTime,
														BigDecimal(bikeId), BigDecimal(tripDuration.replace(",", "")), BigDecimal(fromStationId),
														fromStationName, BigDecimal(toStationId), toStationName,
														userType, null, null)
								}
						})
		}
}
