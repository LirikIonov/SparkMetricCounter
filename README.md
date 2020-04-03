Best in the world Spark Streaming Application which counts metrics of bike dataset loaded via Google Cloud Pub/Sub. 

Launch it in Cloud Shell using command:
```
gcloud dataproc jobs submit spark \
--cluster=claus \
--region=us-central1 \
--class=ru.sgu.SparkLauncher \
--properties ^#^spark.jars.packages=org.apache.bahir:spark-streaming-pubsub_2.11:2.4.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.14.0-beta \
--jars=gs://unrealbucket/bin/SparkMetricCounter.jar
```