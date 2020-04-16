package ru.sgu

import org.apache.logging.log4j.scala.Logging
import ru.sgu.controller.SparkController

object SparkLauncher extends Logging {
		def main(args: Array[String]) {
				try {
						logger.info("Started Spark Controller")
						new SparkController().start(args)
				}
				catch {
						case e: Exception =>
								logger.info(s"Spark Controller throwed an exception: ${e.getMessage}")
								logger.info("System.exit(1)")
								sys.exit(1)
				}
		}
}
