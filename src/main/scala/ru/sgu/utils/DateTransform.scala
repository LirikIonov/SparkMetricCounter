package ru.sgu.utils

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

object DateTransform {
		val dateMask = "yyyy-MM-dd HH:mm:ss"
		val formatter = DateTimeFormatter.ofPattern(dateMask)
		val zoneId = ZoneId.of("UTC")

		def dateToStr(date: ZonedDateTime): String = {
				date.format(formatter)
		}

		def strToDate(date: String): ZonedDateTime = {
				ZonedDateTime.of(LocalDateTime.parse(date, formatter), zoneId)
		}
}
