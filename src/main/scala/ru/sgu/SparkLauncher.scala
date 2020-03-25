package ru.sgu

import com.typesafe.scalalogging.LazyLogging
import ru.sgu.controller.SparkController

object SparkLauncher extends LazyLogging {
		def main(args: Array[String]) {
				try {
						logger.info("Started Spark Controller")
						new SparkController().start()
				}
				catch {
						case e: Exception =>
								logger.error("Spark Controller throwed an exception: ", e)
								logger.info("System.exit(1)")
								sys.exit(1)
				}
		}
}
