package ru.sgu.writer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import ru.sgu.BikeSet
import ru.sgu.config.SparkMetricConfig
import ru.sgu.logger.BaseLogger
import ru.sgu.utils.DateTransform

class DataWriter(implicit config: SparkMetricConfig, sparkSession: SparkSession) extends BaseLogger with Serializable {
		import sparkSession.implicits._

		def writeToStorage(bikeStream: DStream[BikeSet]): Unit = {
				var x: Long = 0
				bikeStream.foreachRDD(rdd => if (!rdd.isEmpty()) {
						logger.info(if (x == 0) "Saving bike rdd into Storage" else "Saving bike rdd into Storage - continue...")
						val outDirectory = s"gs://${config.bucketName}/out/data-${DateTransform.getCurrentDateTime().replace(" ", "T")}"
						rdd.toDF().write.parquet(outDirectory)
						x += 1
				})
		}

		def writeToBigQuery(bikeFrame: DataFrame, tableName: String): Unit = {
				logger.info("Saving into Big Query...")
				bikeFrame.write.format("com.google.cloud.spark.bigquery")
						.option("table", s"${config.projectId}:${config.tablePrefix}.$tableName")
						.mode(org.apache.spark.sql.SaveMode.Overwrite)
						.save()
		}
}
