from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, sum

def main():
    spark = SparkSession.builder.appName("Exercice 6 - Agrégation").getOrCreate()
    df = spark.read.csv("/Users/mac/Documents/Tp1HebSalwa/data/produits.csv", header=True, inferSchema=True)

    df.show()

    #Le prix moyen et le prix total par catégorie
    prix_moyen = df.groupBy("Catégorie").agg(avg("Prix").alias("Prix_moyen"), sum("Prix").alias("Prix_total"))

    prix_moyen.show()

if __name__ == "__main__":
    main()