from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.clustering import KMeans, BisectingKMeans
from pyspark.sql.functions import udf, col
from pyspark.ml.linalg import Vectors
from pyspark.ml.evaluation import ClusteringEvaluator

spark = SparkSession.builder.appName("Load CSV file into Spark DataFrame").getOrCreate()

def load_and_prepare_data():
    df = spark.read.csv("/Users/geyutong/Desktop/242/assignment3/data/sectionB/output_42MB.csv", header=True)
    df = df.sample(fraction=0.01, seed=123)
    
    @udf("float")  # return float
    def ip_2_d(text_item):
        tok_list = text_item.split('.')
        try:
            res = float(tok_list[0]) * 1000 + float(tok_list[1]) * 100 + float(tok_list[2]) * 10 + float(tok_list[3])
        except: 
            res = 0.0  
        return res

    df = df.withColumn("ip_num", ip_2_d(df["host"]))  # 假设 host 列是 IP 地址
    df = df.withColumn("response_code", col("response_code").cast("float"))
    df = df.withColumn("content_size", col("content_size").cast("float"))
    df = df.select(df["ip_num"], df["response_code"], df["content_size"])
    df = df.repartition(10)
    
    assembler = VectorAssembler(inputCols=["ip_num", "response_code", "content_size"], 
                                outputCol = "raw_features", 
                                handleInvalid="skip")
    df = assembler.transform(df)
    
    scaler = StandardScaler(inputCol="raw_features", outputCol="features")
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)
    
    return df

def run_kmeans(df, k=3):
    kmeans = KMeans(featuresCol='features', k=k, seed=123)
    model = kmeans.fit(df)

    # Print the cluster centers
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    # Compute WSSSE
    evaluator = ClusteringEvaluator()
    wssse = evaluator.evaluate(model.transform(df))
    print("Within Set Sum of Squared Errors = " + str(wssse))

def run_bisecting_kmeans(df, k=3):
    bkm = BisectingKMeans(featuresCol='features').setK(k).setSeed(1)
    model2 = bkm.fit(df)
    
    # Print the cluster centers
    centers = model2.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    # Compute WSSSE
    evaluator = ClusteringEvaluator()
    wssse = evaluator.evaluate(model2.transform(df))
    print("Within Set Sum of Squared Errors = " + str(wssse))

df = load_and_prepare_data()
run_kmeans(df)
run_bisecting_kmeans(df)
