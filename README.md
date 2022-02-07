# vhs-data-analysis
This project use spark 3.2 with Scala to:
1. Consume, clean and enrich VHS events from mongoDB and save the result into parquet file partitioned by codMonth (yyyymm)  - Done
2. Analyze and segment the enriched data to discover unknown patterns among the data.  - In progress

How to build and create the jars enrich-vhs-data.jar(1) and analyze-vhs-data.jar(2)
> sbt clean compile assembly

Docker is needed to run this project

Go to spark docker directory
> cd [PROJECT_PATH]/docker/spark

Build spark docker from Dockerfile:
> docker build -t scalac/spark .

Run spark container
> sh run 

Enter the spark container
> sh shell

How to execute locally the vhs-data-enricher module (jar 1) 
> spark-submit \
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
- Elbow method to determine the best number of cluster for the kMeans algorithm
> spark-submit \
    --class "VHSDataAnalyzer" \
    --master local[4] \
    analyze-vhs-data/target/scala-2.12/analyze-vhs-data.jar \
    --behavior=[daily | monthly] \
    --mainPath=data/output \
    --folderName=enriched-data \
    --gameId=LOVE_COLORS \
    --fromDate=202101 \
    --toDate=202112 \
    --method=elbow \
    --fromK=3 \
    --toK=30

- KMeans method to segment the data in k cluster
> spark-submit \
   --class "VHSDataAnalyzer" \
   --master local[4] \
   analyze-vhs-data/target/scala-2.12/analyze-vhs-data.jar \
   --behavior=[daily | monthly] \
   --mainPath=data/output \
   --folderName=enriched-data \
   --gameId=LOVE_COLORS \
   --fromDate=202101 \
   --toDate=202112 \
   --method=k-means \
   --k=6
> 
- LTV method to calculate the value of users per day
> /home/segundocruz/Documents/Scalac/spark-3.2.0-bin-hadoop3.2/bin/spark-submit \
   --class "VHSDataAnalyzer" \
   --master local[4] \
   analyze-vhs-data/target/scala-2.12/analyze-vhs-data.jar \
   --behavior=[daily | monthly] \
   --mainPath=data-models \
   --folderName=output \
   --gameId=LOVE_COLORS \
   --fromDate=202101 \
   --toDate=202112 \
   --method=ltv \
   --attribute=[user | cluster]

- Retention method to analyze the retention for all the days of 1 month
> /home/segundocruz/Documents/Scalac/spark-3.2.0-bin-hadoop3.2/bin/spark-submit \
   --class "VHSDataAnalyzer" \
   --master local[4] \
   analyze-vhs-data/target/scala-2.12/analyze-vhs-data.jar \
   --behavior=daily \
   --mainPath=data/output \
   --folderName=enriched-data \
   --gameId=LOVE_COLORS \
   --method=retention \
   --startMonth=202110 \
   --idleTime=0
