
import json
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession

# ==========================================
# 1. CONFIGURATION SPARK ET MINIO
# ==========================================

spark = (
    SparkSession.builder
    .appName("FastFood-Partie2-RDD")
    .master("spark://spark-master:7077")
    .config("spark.driver.host", "jupyter")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.cores.max", "2")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "1g")
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
RESULTS.mkdir(exist_ok=True)

# ==========================================
# 2. CHARGEMENT RDD DEPUIS MINIO
# ==========================================

path = "s3a://lake/bronze/history/orders_history.jsonl"

start = time.perf_counter()

lines = sc.textFile(path)

orders = lines.map(lambda line: json.loads(line))

print("\n=== CHARGEMENT DES COMMANDES ===")

total = orders.count()
print("Nombre total d'evenements :", total)

# ==========================================
# 3. FILTRER LES COMMANDES LIVREES
# ==========================================

delivered = orders.filter(
    lambda order: order["status"] == "DELIVERED"
).cache()

delivered_count = delivered.count()

print("Commandes DELIVERED :", delivered_count)

# ==========================================
# 4. CHIFFRE D'AFFAIRES PAR VILLE
# ==========================================

ca_rdd = (
    delivered
    .map(lambda order: (
        order["city"],
        float(order["amount_mad"])
    ))
    .reduceByKey(lambda a, b: a + b)
)

ca = sorted(ca_rdd.collect())

print("\n=== CHIFFRE D'AFFAIRES PAR VILLE ===")

for city, amount in ca:
    print(city, ":", round(amount, 2), "MAD")

# ==========================================
# 5. PANIER MOYEN PAR VILLE
# ==========================================

panier_rdd = (
    delivered
    .map(lambda order: (
        order["city"],
        (float(order["amount_mad"]), 1)
    ))
    .reduceByKey(
        lambda a, b: (
            a[0] + b[0],
            a[1] + b[1]
        )
    )
)

paniers = sorted(panier_rdd.collect())

print("\n=== PANIER MOYEN PAR VILLE ===")

for city, (amount, count) in paniers:
    print(city, ":", round(amount / count, 2), "MAD")

# ==========================================
# 6. TOP 5 RESTAURANTS
# ==========================================

restaurants_rdd = (
    delivered
    .map(lambda order: (
        order["restaurant_id"], 1
    ))
    .reduceByKey(lambda a, b: a + b)
)

top5 = restaurants_rdd.takeOrdered(
    5,
    key=lambda x: (-x[1], x[0])
)

print("\n=== TOP 5 RESTAURANTS ===")

for restaurant, count in top5:
    print(restaurant, ":", count, "commandes")

# ==========================================
# 7. LINEAGE RDD
# ==========================================

lineage = ca_rdd.toDebugString()

if isinstance(lineage, bytes):
    lineage = lineage.decode("utf-8")

print("\n=== LINEAGE RDD ===")
print(lineage)

# ==========================================
# 8. MESURER ET ENREGISTRER
# ==========================================

duration = time.perf_counter() - start

resultats = {
    "total_events": total,
    "delivered_events": delivered_count,
    "ca_par_ville": {
        city: round(amount, 2)
        for city, amount in ca
    },
    "panier_moyen": {
        city: round(amount / count, 2)
        for city, (amount, count) in paniers
    },
    "top5_restaurants": top5,
    "temps_total_secondes": round(duration, 4)
}

(RESULTS / "partie2_resultats.json").write_text(
    json.dumps(resultats, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

(RESULTS / "partie2_lineage.txt").write_text(
    lineage, encoding="utf-8"
)

print("\nTemps total :", round(duration, 4), "secondes")
print("Resultats enregistres dans results/")

# Conserver Spark actif pour consulter le DAG
input("\nAppuyer sur Entree apres les captures Spark UI...")

spark.stop()
