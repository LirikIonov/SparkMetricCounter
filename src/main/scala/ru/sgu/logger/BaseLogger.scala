package ru.sgu.logger

import com.typesafe.scalalogging.LazyLogging

class BaseLogger extends LazyLogging {
		def log[R](block: => R, methodName: String): R = {
				logger.info(s"$methodName")
				block
		}
}
