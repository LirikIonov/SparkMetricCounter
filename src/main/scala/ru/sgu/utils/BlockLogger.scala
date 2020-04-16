package ru.sgu.utils

import org.apache.logging.log4j.scala.Logging

class BlockLogger extends Logging {
		def log[R](block: => R, methodName: String): R = {
				logger.info(s"$methodName")
				block
		}
}
