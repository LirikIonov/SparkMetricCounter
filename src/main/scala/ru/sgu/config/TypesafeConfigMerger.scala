package ru.sgu.config

import java.net.URL

import com.typesafe.config.{Config, ConfigFactory}

object TypesafeConfigMerger {
		def loadByPaths(paths: List[URL]): Config = {
				(paths.map(ConfigFactory.parseURL) :+ ConfigFactory.defaultReference())
						.fold(ConfigFactory.empty())((c1, c2) => c1.withFallback(c2))
		}
}
