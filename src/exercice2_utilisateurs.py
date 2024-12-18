from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count


def main():
    spark = SparkSession.builder.appName("Exercice 2 - Utilisateurs").getOrCreate()
    df = spark.read.option("mode", "DROPMALFORMED").json("/Users/mac/Documents/Tp1HebSalwa/data/utilisateurs.json")

    df.show()


    #L'âge moyen des utilisateurs
    age_avg = df.agg(avg("âge")).collect()[0][0]
    print(f"Age moyen des utilisateurs: {age_avg:.2f} ans")

    #Nombre utilisateurs par ville
    user_par_ville = df.groupBy("ville").agg(count("id").alias("Nombre_utilisateurs"))
    print("Nombre d'utilisateurs par ville:")
    user_par_ville.show()

    #Le plus jeune Utilisateur
    user_plus_jeune = df.orderBy(col("âge").asc()).first()
    print(f"Utilisateur le plus jeune: {user_plus_jeune['nom']} ({user_plus_jeune['âge']} ans)")


if __name__ == "__main__":
    main()