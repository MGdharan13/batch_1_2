
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper
import mysql.connector

# ==========================================
# 1. CONFIGURATION
# ==========================================

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DATABASE = "pyspark_demo"

JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    "?useSSL=false&allowPublicKeyRetrieval=true"
)

# ==========================================
# 2. SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("Incremental_ETL_Parquet")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "com.mysql:mysql-connector-j:8.4.0"
    )
    .getOrCreate()
)

# ==========================================
# 3. MYSQL CONNECTION
# ==========================================

connection = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD
)

cursor = connection.cursor()

cursor.execute(
    f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}"
)

cursor.execute(f"USE {MYSQL_DATABASE}")

# Create one target table
cursor.execute("""
CREATE TABLE IF NOT EXISTS employees (
    employee_id BIGINT PRIMARY KEY,
    name VARCHAR(100),
    salary BIGINT,
    department VARCHAR(100)
)
""")

connection.commit()


# ==========================================
# 4. PROCESS AND LOAD BATCH
# ==========================================

def process_batch(file_path, batch_name):

    print("\n===================================")
    print("PROCESSING:", batch_name)
    print("===================================")

    # Read cleaned CSV
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

    print("Source records:", df.count())

    # Validate and transform
    df = (
        df.select(
            col("employee_id").cast("long"),
            trim(col("name")).alias("name"),
            col("salary").cast("long"),
            trim(col("department")).alias("department")
        )
        .filter(
            col("employee_id").isNotNull()
            & col("name").isNotNull()
            & col("salary").isNotNull()
            & col("department").isNotNull()
        )
        .filter(
            (col("employee_id") > 0)
            & (col("salary") > 0)
            & (col("name") != "")
            & (col("department") != "")
            & (~upper(col("name")).isin("NA", "NULL", "N/A"))
            & (~upper(col("department")).isin("NA", "NULL", "N/A"))
        )
        .dropDuplicates(["employee_id"])
    )

    print("Valid records:", df.count())

    # ======================================
    # 5. INSERT INTO MYSQL TARGET TABLE
    # ======================================

    records = [
        tuple(row)
        for row in df.collect()
    ]

    cursor.executemany("""
        INSERT IGNORE INTO employees
        (
            employee_id,
            name,
            salary,
            department
        )
        VALUES (%s, %s, %s, %s)
    """, records)

    connection.commit()

    print("New records inserted:", cursor.rowcount)

    # ======================================
    # 6. DISPLAY CURRENT DATABASE COUNT
    # ======================================

    cursor.execute("SELECT COUNT(*) FROM employees")

    total = cursor.fetchone()[0]

    print("Total employees in MySQL:", total)


# ==========================================
# 7. BATCH 1 - INITIAL LOAD
# ==========================================

process_batch(
    "clean/batch1",
    "BATCH 1 - INITIAL LOAD"
)


# ==========================================
# 8. BATCH 2 - INCREMENTAL LOAD
# ==========================================

process_batch(
    "clean/batch2",
    "BATCH 2 - INCREMENTAL LOAD"
)


# ==========================================
# 9. READ ALL DATA FROM MYSQL
# ==========================================

print("\nReading complete employee table...")

final_df = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "employees")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .load()
)

print("Total MySQL records:", final_df.count())

final_df.show(truncate=False)


# ==========================================
# 10. EXPORT ALL DATA TO PARQUET
# ==========================================

(
    final_df
    .write
    .mode("overwrite")
    .parquet("output/employees_parquet")
)

print("\nAll employee records exported to Parquet!")


# ==========================================
# 11. VERIFY PARQUET OUTPUT
# ==========================================

parquet_df = spark.read.parquet(
    "output/employees_parquet"
)

print("\n========== PARQUET VERIFICATION ==========")

print("Parquet records:", parquet_df.count())

parquet_df.show(truncate=False)


# ==========================================
# 12. CLOSE CONNECTIONS
# ==========================================

cursor.close()
connection.close()

spark.stop()

print("\nINCREMENTAL ETL COMPLETED SUCCESSFULLY!")
