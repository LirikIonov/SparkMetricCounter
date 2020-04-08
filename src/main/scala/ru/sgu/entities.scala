package ru.sgu

case class BikeSet(tripId: BigDecimal, startTime: String, endTime: String,
													bikeId: BigDecimal, tripDuration: BigDecimal, fromStationId: BigDecimal,
													fromStationName: String, toStationId: BigDecimal, toStationName: String,
													userType: String, genderType: String, birthYear: String) extends Serializable

case class BikeMetrics(maxTripDuration: BigDecimal, minTripDuration: BigDecimal, avgTripDuration: BigDecimal,
											 medianTripDuration: BigDecimal, medianClientAge: BigDecimal) extends Serializable

case class TopCountries(country: String) extends Serializable
