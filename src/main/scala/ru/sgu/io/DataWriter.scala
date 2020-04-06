package ru.sgu.io

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, functions}
import ru.sgu.spark.SparkMetricConfig
import ru.sgu.utils.DateUtils

class DataWriter(implicit config: SparkMetricConfig) extends Serializable {
		def writeToStorage(bikeRDD: DataFrame): Unit = {
				val bucketName = config.bucketName
				val suffix = DateUtils.getCurrentDateTime().replace(" ", "T")
				println("Saving bike rdd into Storage...")
				val outDirectory = s"gs://$bucketName/out/data-$suffix"
				bikeRDD.write.parquet(outDirectory)
		}

		def writeToBigQuery(bikeFrame: DataFrame, tableName: String): Unit = {
				println(s"Saving into $tableName (BigQuery)")
				//bikeFrame.show(10)
				val projectId = config.projectId
				val tablePrefix = config.tablePrefix
				setNullableStateOfColumn(bikeFrame, nullable = true)
						.write.format("com.google.cloud.spark.bigquery")
						.option("table", s"$projectId:$tablePrefix.$tableName")
						.mode(org.apache.spark.sql.SaveMode.Append)
						.save()
		}

		def setNullableStateOfColumn(bikeFrame: DataFrame, nullable: Boolean): DataFrame = {
				val df = bikeFrame.withColumn("timestamp", functions.lit(DateUtils.getCurrentDateTime()).cast("timestamp"))
				val schema = df.schema
				val newSchema = StructType(schema.map {
						case StructField(c, t, _, m) if c.equals("timestamp") => StructField(c, t, nullable = true, m)
						case StructField(c, t, _, m) if c.contains("StationName") => StructField("country", t, nullable = true, m)
						case y: StructField => y
				})
				df.sqlContext.createDataFrame(df.rdd, newSchema)
		}
}
