import os
import socket
import subprocess

# Fix 1: ensure JAVA_HOME is set.
os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-21-openjdk-amd64")

# Fix 2: PySpark's spark-class uses bash process substitution which requires
# /dev/fd. On some Linux containers it is missing; create the symlink if needed.
if not os.path.exists("/dev/fd"):
    os.symlink("/proc/self/fd", "/dev/fd")

# Fix 3: Spark's JVM calls InetAddress.getLocalHost() at startup. If the
# hostname is not in /etc/hosts the DNS lookup fails with UnknownHostException,
# which causes the Java gateway to exit before sending its port number.
hostname = socket.gethostname()
try:
    socket.gethostbyname(hostname)
except socket.gaierror:
    # Hostname not resolvable — add it to /etc/hosts.
    with open("/etc/hosts", "a") as f:
        f.write(f"\n127.0.0.1 {hostname}\n")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("AlgeriaPopulationAnalysis")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("DZmap.csv", header=True, inferSchema=True)

print("=== Algeria Population by Wilaya ===\n")
df.select("Wilaya", "TOTAL").orderBy(F.col("TOTAL").desc()).show(58, truncate=False)

print("=== Summary Statistics ===")
df.agg(
    F.sum("TOTAL").alias("Total_Population"),
    F.avg("TOTAL").alias("Avg_Population_per_Wilaya"),
    F.max("TOTAL").alias("Max_Population"),
    F.min("TOTAL").alias("Min_Population"),
).show()

spark.stop()
