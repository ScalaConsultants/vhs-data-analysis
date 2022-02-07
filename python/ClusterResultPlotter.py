import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import PCA

spark = SparkSession.builder.getOrCreate()

def plotPoints(pointsRow, colorPoints):
	pointsX = [row.points[0] for row in pointsRow]
	pointsY = [row.points[1] for row in pointsRow]
	plt.scatter(pointsX, pointsY, color = colorPoints)

def main():
	dfClusterResults = spark.read.parquet("data-models/output/cluster-data")

	#dfClusterResults = dfClusterResults.select("numLevelsCompleted", "numAdsWatched", "numPurchasesDone", "flagOrganic", "cluster")

	pca = PCA(k=2, inputCol="scaledFeatures", outputCol="pcaFeatures")
	#featureVector

	modelPCA = pca.fit(dfClusterResults)

	pcaResult = modelPCA.transform(dfClusterResults).withColumn('points', vector_to_array('pcaFeatures')).select("points", "cluster")

	pcaResult.show(10)

	#filter rows of original data
	points_cluster0 = pcaResult.where(col("cluster") == 0).select("points").collect()
	points_cluster1 = pcaResult.where(col("cluster") == 1).select("points").collect()
	points_cluster2 = pcaResult.where(col("cluster") == 2).select("points").collect()
	points_cluster3 = pcaResult.where(col("cluster") == 3).select("points").collect()
	points_cluster4 = pcaResult.where(col("cluster") == 4).select("points").collect()
	points_cluster5 = pcaResult.where(col("cluster") == 5).select("points").collect()
	points_cluster6 = pcaResult.where(col("cluster") == 6).select("points").collect()
	points_cluster7 = pcaResult.where(col("cluster") == 7).select("points").collect()
	 
	#Plotting the results
	plotPoints(points_cluster0, "red")
	plotPoints(points_cluster1, "black")
	plotPoints(points_cluster2, "yellow")
	plotPoints(points_cluster3, "blue")
	plotPoints(points_cluster4, "green")
	plotPoints(points_cluster5, "pink")
	plotPoints(points_cluster6, "purple")
	plotPoints(points_cluster7, "brown")
	plt.show()

main()

spark.stop()