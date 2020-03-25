package ru.sgu.logger

import com.typesafe.scalalogging.LazyLogging

class BaseLogger extends LazyLogging {
		def log[R](block: => R, methodName: String): R = {
				logger.info(s"Started $methodName")
				val result = block
				logger.info(s"Finished $methodName")
				result
		}
}
