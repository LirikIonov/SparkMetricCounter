package ru.sgu

import com.typesafe.scalalogging.LazyLogging
import ru.sgu.controller.SparkController

object SparkLauncher extends LazyLogging {
		def main(args: Array[String]) {
				try {
						println("Started Spark Controller")
						new SparkController().start(args)
				}
				catch {
						case e: Exception =>
								println("Spark Controller throwed an exception: ", e.getMessage)
								println("System.exit(1)")
								sys.exit(1)
				}
		}
}
