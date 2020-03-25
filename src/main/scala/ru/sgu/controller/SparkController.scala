package ru.sgu.controller

import ru.sgu.component.SparkComponent
import ru.sgu.writer.DataWriter

class SparkController {
		val spark: SparkComponent = new SparkComponent()

		def start(): Unit = {
				try {
						new DataController(spark).processData()
				}
				finally {
						spark.session.stop()
				}
		}
}
