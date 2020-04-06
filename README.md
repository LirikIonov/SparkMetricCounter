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

Of course you will need .csv files for counting metrics. It can be uploaded for example like this:
```
gcloud dataflow jobs run superjob \
--gcs-location gs://dataflow-templates/latest/GCS_Text_to_Cloud_PubSub \
--region us-central1 \
--parameters inputFilePattern=gs://unrealbucket/incomingData/test.csv,\
outputTopic=projects/first-apex-272801/topics/faker
```