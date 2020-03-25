package ru.sgu.writer

import org.apache.spark.sql.DataFrame

class DataWriter {
	def writeToGoogleCloud(df: DataFrame): Unit = {}
	def writeToBigQuery(df: DataFrame): Unit = {}
}
