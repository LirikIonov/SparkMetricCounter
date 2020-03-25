package ru.sgu.component

import java.beans.Transient

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import ru.sgu.loader.DataLoader
import ru.sgu.writer.DataWriter

class SparkComponent {
		lazy val conf = new SparkConf()
				.setAppName("SparkMetricCounter")
				//.setMaster("local[8]")

		lazy val session: SparkSession = SparkSession.builder()
				.config(conf)
				.getOrCreate()

		@Transient
		lazy val sparkContext: SparkContext = session.sparkContext

		@Transient
		lazy val sqlContext: SQLContext = session.sqlContext

		lazy val dataLoader: DataLoader = new DataLoader(sqlContext, sparkContext)
		lazy val dataWriter: DataWriter = new DataWriter()
}
