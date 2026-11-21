
import os
import json
import time
import io
import contextlib
import statistics
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

# ==========================================
# 1. CONFIGURATION SPARK ET MINIO
# ==========================================

spark = (
    SparkSession.builder
    .appName("FastFood-Partie3-SparkSQL")
    .master("spark://spark-master:7077")
    .config("spark.driver.host", "jupyter")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.cores.max", "2")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint",
            "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key",
            os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key",
            os.environ["MINIO_ROOT_PASSWORD"])
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

RESULTS = Path("/workspace/results")
RESULTS.mkdir(parents=True, exist_ok=True)

ORDERS_PATH = "s3a://lake/bronze/history/orders_history.jsonl"
RESTAURANTS_PATH = "s3a://lake/bronze/reference/restaurants.json"
SILVER_PATH = "s3a://lake/silver/orders_history/"

print("\n=== PARTIE 3 : SPARK SQL ET DATAFRAMES ===")
print("Version Spark :", spark.version)
print("Master :", sc.master)

# ==========================================
# 2. DEFINIR LES SCHEMAS EXPLICITES
# ==========================================

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount_mad", DoubleType(), True),
    StructField("items_count", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("courier_id", StringType(), True),
    StructField("event_time", StringType(), True)
])

restaurants_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("category", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
])

# ==========================================
# 3. LECTURE DES DONNEES JSON
# ==========================================

orders = (
    spark.read
    .schema(orders_schema)
    .json(ORDERS_PATH)
)

restaurants = (
    spark.read
    .schema(restaurants_schema)
    .json(RESTAURANTS_PATH)
)

print("\n=== SCHEMA DES COMMANDES ===")
orders.printSchema()

print("\n=== SCHEMA DES RESTAURANTS ===")
restaurants.printSchema()

# Mise en cache pour les analyses suivantes
orders.cache()

total_orders = orders.count()
total_restaurants = restaurants.count()

print("\nNombre d'evenements :", total_orders)
print("Nombre de restaurants :", total_restaurants)

assert total_orders == 50000
assert total_restaurants == 200

# ==========================================
# 4. FILTRAGE DES COMMANDES LIVREES
# ==========================================

delivered = orders.filter(
    F.col("status") == "DELIVERED"
)

delivered_count = delivered.count()

print("\n=== COMMANDES LIVREES ===")
print("Evenements DELIVERED :", delivered_count)

# ==========================================
# 5. CA PAR VILLE AVEC DATAFRAME
# ==========================================

start_df = time.perf_counter()

ca_dataframe = (
    delivered
    .groupBy("city")
    .agg(
        F.round(
            F.sum("amount_mad"), 2
        ).alias("ca_mad")
    )
    .orderBy("city")
)

df_rows = ca_dataframe.collect()

temps_dataframe = time.perf_counter() - start_df

print("\n=== CA PAR VILLE : DATAFRAME ===")

ca_dataframe.show(truncate=False)

print("Temps DataFrame :", round(temps_dataframe, 4), "s")

# ==========================================
# 6. CA PAR VILLE AVEC SPARK SQL
# ==========================================

delivered.createOrReplaceTempView(
    "orders_delivered"
)

start_sql = time.perf_counter()

ca_sql = spark.sql("""
    SELECT
        city,
        ROUND(SUM(amount_mad), 2) AS ca_mad
    FROM orders_delivered
    GROUP BY city
    ORDER BY city
""")

sql_rows = ca_sql.collect()

temps_sql = time.perf_counter() - start_sql

print("\n=== CA PAR VILLE : SPARK SQL ===")

ca_sql.show(truncate=False)

print("Temps Spark SQL :", round(temps_sql, 4), "s")

# Verifier que les deux API produisent le meme CA
ca_df_values = {
    r["city"]: r["ca_mad"] for r in df_rows
}

ca_sql_values = {
    r["city"]: r["ca_mad"] for r in sql_rows
}

assert ca_df_values == ca_sql_values

print("Comparaison DataFrame / SQL : OK")

# Comparaison avec la Partie 2 RDD
rdd_file = RESULTS / "partie2_resultats.json"

if rdd_file.exists():
    rdd_results = json.loads(
        rdd_file.read_text(encoding="utf-8")
    )

    rdd_ca = rdd_results["ca_par_ville"]

    for city, amount in ca_df_values.items():
        assert abs(
            amount - rdd_ca[city]
        ) < 0.02

    print("Comparaison des CA RDD / DataFrame / SQL : OK")

# ==========================================
# 7. JOINTURE AVEC LES RESTAURANTS
# ==========================================

print("\n=== JOINTURE COMMANDES / RESTAURANTS ===")

restaurant_ref = restaurants.select(
    "restaurant_id",
    F.col("name").alias("restaurant_name"),
    F.col("city").alias("restaurant_city"),
    "category"
)

# Broadcast adapte au petit referentiel de 200 restaurants
joined = orders.join(
    F.broadcast(restaurant_ref),
    on="restaurant_id",
    how="left"
)

print("Exemple de jointure :")

joined.select(
    "order_id",
    "city",
    "restaurant_name",
    "category",
    "amount_mad"
).show(10, truncate=False)

# Verification des correspondances
missing_ref = joined.filter(
    F.col("category").isNull()
).count()

print("Restaurants sans correspondance :", missing_ref)

# ==========================================
# 8. CA PAR VILLE ET PAR CATEGORIE
# ==========================================

print("\n=== CA PAR VILLE ET CATEGORIE ===")

joined_delivered = joined.filter(
    F.col("status") == "DELIVERED"
)

ca_category = (
    joined_delivered
    .groupBy("city", "category")
    .agg(
        F.round(
            F.sum("amount_mad"), 2
        ).alias("ca_mad")
    )
    .orderBy("city", "category")
)

ca_category.show(50, truncate=False)

category_rows = [
    row.asDict() for row in ca_category.collect()
]

# ==========================================
# 9. UDF PYTHON DISTANCE_BAND
# ==========================================

print("\n=== CLASSIFICATION DES DISTANCES ===")

def distance_band(distance):
    if distance is None:
        return None
    if distance < 2:
        return "court"
    if distance <= 5:
        return "moyen"
    return "long"


distance_udf = F.udf(
    distance_band,
    StringType()
)

orders_udf = orders.withColumn(
    "distance_band",
    distance_udf(F.col("distance_km"))
)

# ==========================================
# 10. VERSION NATIVE WHEN / OTHERWISE
# ==========================================

native_expression = (
    F.when(
        F.col("distance_km").isNull(),
        F.lit(None).cast("string")
    )
    .when(F.col("distance_km") < 2, "court")
    .when(F.col("distance_km") <= 5, "moyen")
    .otherwise("long")
)

orders_native = orders.withColumn(
    "distance_band",
    native_expression
)

# ==========================================
# 11. COMPARAISON DES PERFORMANCES UDF
# ==========================================

def measure_distance(df, repetitions=3):
    durations = []
    result = None

    for _ in range(repetitions):
        begin = time.perf_counter()

        # groupBy oblige Spark a evaluer distance_band
        rows = (
            df.groupBy("distance_band")
            .count()
            .collect()
        )

        durations.append(
            time.perf_counter() - begin
        )

        result = {
            row["distance_band"]: row["count"]
            for row in rows
        }

    return result, statistics.median(durations)


udf_counts, temps_udf = measure_distance(
    orders_udf
)

native_counts, temps_native = measure_distance(
    orders_native
)

assert udf_counts == native_counts

print("\nResultats classification :", native_counts)
print("Temps median UDF :", round(temps_udf, 4), "s")
print("Temps median natif :", round(temps_native, 4), "s")

if temps_native > 0:
    print(
        "Rapport temps UDF / natif :",
        round(temps_udf / temps_native, 2)
    )

# ==========================================
# 12. EXPLAIN(TRUE) SUR LA JOINTURE
# ==========================================

print("\n=== EXPLAIN TRUE : JOINTURE ===")

explain_buffer = io.StringIO()

with contextlib.redirect_stdout(explain_buffer):
    joined_delivered.explain(True)

explain_text = explain_buffer.getvalue()

print(explain_text)

(RESULTS / "partie3_explain.txt").write_text(
    explain_text,
    encoding="utf-8"
)

# ==========================================
# 13. NETTOYAGE ET ENRICHISSEMENT
# ==========================================

print("\n=== NETTOYAGE DES DONNEES ===")

cleaned = (
    joined
    .dropna(
        subset=[
            "order_id",
            "restaurant_id",
            "city",
            "status",
            "amount_mad"
        ]
    )
    .dropDuplicates(["order_id", "status"])
    .withColumn(
        "distance_band",
        native_expression
    )
    .withColumn(
        "event_timestamp",
        F.to_timestamp("event_time")
    )
)

cleaned_count = cleaned.count()

print("Evenements avant nettoyage :", total_orders)
print("Evenements apres nettoyage :", cleaned_count)
print("Doublons / lignes elimines :", total_orders - cleaned_count)

# ==========================================
# 14. ECRITURE PARQUET DANS MINIO
# ==========================================

print("\n=== ECRITURE PARQUET SILVER ===")

cleaned.write \
    .mode("overwrite") \
    .partitionBy("city") \
    .parquet(SILVER_PATH)

print("Ecriture terminee :", SILVER_PATH)

# ==========================================
# 15. VERIFICATION DU STOCKAGE PARQUET
# ==========================================

print("\n=== VERIFICATION PARQUET ===")

silver_df = spark.read.parquet(SILVER_PATH)

silver_count = silver_df.count()

print("Nombre de lignes Parquet :", silver_count)

silver_df.groupBy("city") \
    .count() \
    .orderBy("city") \
    .show()

assert silver_count == cleaned_count

print("Lecture Parquet : OK")

# ==========================================
# 16. ENREGISTREMENT DES RESULTATS
# ==========================================

results = {
    "total_events": total_orders,
    "delivered_events": delivered_count,
    "restaurants": total_restaurants,
    "ca_dataframe": ca_df_values,
    "ca_sql": ca_sql_values,
    "ca_by_category": category_rows,
    "missing_restaurant_refs": missing_ref,
    "distance_counts": native_counts,
    "temps_dataframe_secondes": round(temps_dataframe, 4),
    "temps_sql_secondes": round(temps_sql, 4),
    "temps_udf_secondes": round(temps_udf, 4),
    "temps_native_secondes": round(temps_native, 4),
    "cleaned_events": cleaned_count,
    "silver_events": silver_count,
    "silver_path": SILVER_PATH
}

(RESULTS / "partie3_resultats.json").write_text(
    json.dumps(
        results,
        indent=2,
        ensure_ascii=False
    ),
    encoding="utf-8"
)

print("\n======================================")
print("PARTIE 3 TERMINEE AVEC SUCCES")
print("======================================")

print("Resultats : /workspace/results/")
print("Parquet : ", SILVER_PATH)

orders.unpersist()

# Garder Spark actif pour les captures
input(
    "\nAppuyer sur Entree apres les captures Spark UI..."
)

spark.stop()
