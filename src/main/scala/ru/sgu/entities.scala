package ru.sgu

case class BikeStats(tripId: BigDecimal, startTime: String, endTime: String,
													bikeId: BigDecimal, tripDuration: BigDecimal, fromStationId: BigDecimal,
													fromStationName: String, toStationId: BigDecimal, toStationName: String,
													userType: String, genderType: String, birthYear: String) extends Serializable

case class BikeMetrics(max: BigDecimal, min: BigDecimal, mean: BigDecimal, median: BigDecimal) extends Serializable
