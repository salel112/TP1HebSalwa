from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, sum

def main():
    spark = SparkSession.builder.appName("Exercice 5 - Transactions").getOrCreate()
    df = spark.read.csv("/Users/mac/Documents/Tp1HebSalwa/data/transaction.csv", header=True, inferSchema=True)
    df.show()

    #Dépenses totales par utilisateur
    dep_totale= df.groupBy("Client").agg(sum("Montant").alias("Dépenses_totales"))
    dep_totale.show()

    #Le client qui a dépensé le plus
    client= dep_totale.orderBy(col("Dépenses_totales").desc()).first()

    print(f"Client ayant dépensé le plus : {client['Client']} avec {client['Dépenses_totales']}€")

if __name__ == "__main__":
    main()