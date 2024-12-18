from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when

def main():
    spark = SparkSession.builder.appName("Exercice 3 - Nettoyage").getOrCreate()
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Users/mac/Documents/Tp1HebSalwa/data/clients.csv")

    df.show()

    # Remplissage de l'âge moyen pour les valeurs manquantes
    age_avg = df.agg(avg("âge")).collect()[0][0]
    df=df.fillna({"Âge":age_avg})

    # Remplissage de la ville inconnue
    df = df.fillna({"Ville": "Inconnue"})

    # Supression des lignes avec revenues manquants
    df=df.filter(df.Revenu.isNotNull())

    df.show()

if __name__ == "__main__":
    main()