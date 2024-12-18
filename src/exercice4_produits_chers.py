from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, sum

def main():
    spark = SparkSession.builder.appName("Exercice 4 - Produits chers").getOrCreate()
    df = spark.read.csv("/Users/mac/Documents/Tp1HebSalwa/data/produits.csv", header=True, inferSchema=True)

    df.show()

    #Les 3 produits les plus chers
    produitCher = df.orderBy(col("Prix").desc()).limit(3)
    produitCher.show()

if __name__ == "__main__":
    main()