# vhs-data-analysis
This project use spark 3.2 with Scala to:
1. Consume, clean and enrich VHS events from mongoDB and save the result into parquet file partitioned by codMonth (yyyymm)  - Done
2. Analyze and segment the enriched data to discover unknown patterns among the data.  - In progress

How to build and create the jars enrich-vhs-data.jar(1) and analyze-vhs-data.jar(2)
> sbt clean compile assembly

Spark 3.2 is needed to run this project -> https://spark.apache.org/downloads.html

How to execute locally the vhs-data-enricher module (jar 1) 
> [YourSparkPathHere]/bin/spark-submit \
    --class "VHSDataEnricher" \
    --master local[4] \
    enrich-vhs-data/target/scala-2.12/enrich-vhs-data.jar \
    --behavior=[daily | monthly | both] \
    --mongoUri=[MongoUriHere] \
    --database=vhs \
    --collection=events
    --fromDate=202101
    --toDate=202112

How to execute locally the vhs-data-analyzer module (jar 2)
> [YourSparkPathHere]/bin/spark-submit \
    --class "VHSDataAnalyzer" \
    --master local[4] \
    analyze-vhs-data/target/scala-2.12/analyze-vhs-data.jar \
    --behavior=[daily | monthly] \
    --mainPath=data/output \
    --folderName=enriched-data \
    --fromDate=202101 \
    --toDate=202112
