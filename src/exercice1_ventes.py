from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


def main():
    spark = SparkSession.builder.appName("Exercice 1 - Ventes").getOrCreate()
    df = spark.read.csv("/Users/mac/Documents/Tp1HebSalwa/data/ventes.csv", header=True, inferSchema=True)

    df = df.withColumn("Chiffre d'affaires", col("Quantité") * col("Prix_unitaire"))

    chiffre_affaire_totale = df.agg(sum("Chiffre d'affaires")).collect()[0][0]
    print(f"Chiffre d'affaires total: {chiffre_affaire_totale} €")

    # Trouver le produit le plus vendu
    produit_plus_vendu = df.groupBy("Produit") \
        .agg(sum("Quantité").alias("Quantité_totale")) \
        .orderBy(col("Quantité_totale").desc()) \
        .first()

    print(
        f"Produit le plus vendu: {produit_plus_vendu['Produit']} avec {produit_plus_vendu['Quantité_totale']} unités vendues")


if __name__ == "__main__":
    main()
