package ru.sgu.loader

import java.nio.charset.StandardCharsets

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import ru.sgu.BikeSet

class LocalDataLoader(implicit sc: StreamingContext) {

		def loadData(): DStream[String] = {
				val x = sc.textFileStream("Divvy_Trips_2018_Q1.csv")

				x.foreachRDD(rdd => if (!rdd.isEmpty()) {
						println(rdd.count())
				})
				x
		}
}
