package ru.sgu.utils

import com.typesafe.scalalogging.LazyLogging

class BlockLogger extends LazyLogging {
		def log[R](block: => R, methodName: String): R = {
				println(s"$methodName")
				block
		}
}
