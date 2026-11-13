
import os
import uuid
import argparse
import mysql.connector

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, initcap,
    current_timestamp, to_date,
    to_timestamp, lit, row_number
)
from pyspark.sql.window import Window

from config import (
    DB_HOST, DB_PORT, DB_NAME,
    DB_USER, DB_PASSWORD,
    JDBC_URL, DB_PROPERTIES, JDBC_DRIVER
)


# =====================================================
# 1. FOLDER CONFIGURATION
# =====================================================

OUTPUT_DIR = "output"

CLEAN_DIR = os.path.join(OUTPUT_DIR, "clean")
REJECT_DIR = os.path.join(OUTPUT_DIR, "rejected")

os.makedirs(CLEAN_DIR, exist_ok=True)
os.makedirs(REJECT_DIR, exist_ok=True)


# =====================================================
# 2. SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("PySpark_MySQL_Incremental_ETL")
    .config("spark.jars.packages", JDBC_DRIVER)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =====================================================
# 3. MYSQL CONNECTION
# =====================================================

def get_mysql_connection():

    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


# =====================================================
# 4. READ CSV, JSON AND PARQUET
# =====================================================

def read_input(file_path):

    extension = os.path.splitext(
        file_path.rstrip("/")
    )[1].lower()

    if extension == ".csv":

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(file_path)
        )

    elif extension == ".json":

        df = (
            spark.read
            .option("multiLine", True)
            .json(file_path)
        )

    elif extension == ".parquet":

        df = spark.read.parquet(file_path)

    else:

        raise ValueError("Unsupported input format")

    print("Input loaded:", file_path)

    return df


# =====================================================
# 5. STANDARDIZE COLUMNS
# =====================================================

def standardize_columns(df):

    for old_name in df.columns:

        new_name = (
            old_name.strip()
            .lower()
            .replace(" ", "_")
        )

        if old_name != new_name:
            df = df.withColumnRenamed(
                old_name, new_name
            )

    required_columns = [
        "employee_id",
        "name",
        "department",
        "salary",
        "joining_date",
        "updated_at"
    ]

    for column in required_columns:

        if column not in df.columns:

            df = df.withColumn(
                column,
                lit(None).cast("string")
            )

    return df.select(*required_columns)


# =====================================================
# 6. DATA CLEANING AND TRANSFORMATION
# =====================================================

def clean_data(df):

    df = (
        df
        .withColumn(
            "employee_id",
            trim(col("employee_id").cast("string"))
        )
        .withColumn(
            "name",
            initcap(trim(col("name")))
        )
        .withColumn(
            "department",
            upper(trim(col("department")))
        )
        .withColumn(
            "salary",
            col("salary").cast("double")
        )
        .withColumn(
            "joining_date",
            to_date(col("joining_date"))
        )
        .withColumn(
            "updated_at",
            to_timestamp(col("updated_at"))
        )
    )

    return df


# =====================================================
# 7. DATA VALIDATION
# =====================================================

def validate_data(df):

    invalid_condition = (
        col("employee_id").isNull() |
        (trim(col("employee_id")) == "") |
        col("name").isNull() |
        (trim(col("name")) == "") |
        col("department").isNull() |
        (trim(col("department")) == "") |
        col("salary").isNull() |
        (col("salary") < 0)
    )

    valid_df = df.filter(~invalid_condition)

    rejected_df = df.filter(invalid_condition)

    return valid_df, rejected_df


# =====================================================
# 8. REMOVE DUPLICATES
# =====================================================

def remove_duplicates(df):

    window_spec = (
        Window
        .partitionBy("employee_id")
        .orderBy(col("updated_at").desc_nulls_last())
    )

    df = (
        df
        .withColumn(
            "row_number",
            row_number().over(window_spec)
        )
        .filter(col("row_number") == 1)
        .drop("row_number")
    )

    return df


# =====================================================
# 9. SAVE CLEAN AND REJECTED DATA
# =====================================================

def save_output(valid_df, rejected_df):

    valid_df.write.mode("overwrite").parquet(
        os.path.join(CLEAN_DIR, "cleaned_employees")
    )

    (
        rejected_df.write
        .mode("overwrite")
        .option("header", True)
        .csv(os.path.join(REJECT_DIR, "invalid_employees"))
    )

    print("Clean and rejected files saved")


# =====================================================
# 10. CREATE MYSQL TABLES
# =====================================================

def create_tables():

    connection = get_mysql_connection()

    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            employee_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(100),
            department VARCHAR(100),
            salary DOUBLE,
            joining_date DATE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employee_staging (
            employee_id VARCHAR(50),
            name VARCHAR(100),
            department VARCHAR(100),
            salary DOUBLE,
            joining_date DATE,
            updated_at TIMESTAMP NULL,
            run_id VARCHAR(100),
            INDEX idx_run_id (run_id)
        )
    """)

    connection.commit()

    cursor.close()
    connection.close()


# =====================================================
# 11. LOAD INTO MYSQL STAGING
# =====================================================

def load_staging(valid_df, run_id):

    staging_df = valid_df.withColumn(
        "run_id", lit(run_id)
    )

    (
        staging_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "employee_staging")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("append")
        .save()
    )

    print("Data loaded into MySQL staging table")


# =====================================================
# 12. INCREMENTAL INSERT AND UPDATE
# =====================================================

def incremental_load(run_id):

    connection = get_mysql_connection()

    cursor = connection.cursor()

    try:

        cursor.execute("""
            INSERT INTO employees (
                employee_id,
                name,
                department,
                salary,
                joining_date,
                updated_at
            )

            SELECT
                employee_id,
                name,
                department,
                salary,
                joining_date,
                CURRENT_TIMESTAMP
            FROM employee_staging
            WHERE run_id = %s

            ON DUPLICATE KEY UPDATE

                updated_at = IF(
                    NOT (
                        employees.name <=> VALUES(name)
                        AND employees.department <=> VALUES(department)
                        AND employees.salary <=> VALUES(salary)
                        AND employees.joining_date <=> VALUES(joining_date)
                    ),
                    CURRENT_TIMESTAMP,
                    employees.updated_at
                ),

                name = VALUES(name),
                department = VALUES(department),
                salary = VALUES(salary),
                joining_date = VALUES(joining_date)

        """, (run_id,))

        connection.commit()

        print("Incremental loading completed")

        print("Affected rows:", cursor.rowcount)

        # Delete successfully processed staging records
        cursor.execute("""
            DELETE FROM employee_staging
            WHERE run_id = %s
        """, (run_id,))

        connection.commit()

    except Exception:

        connection.rollback()
        raise

    finally:

        cursor.close()
        connection.close()


# =====================================================
# 13. MAIN ETL PIPELINE
# =====================================================

def main(file_path):

    try:

        run_id = str(uuid.uuid4())

        print("\n========== ETL STARTED ==========")

        # Create database tables
        create_tables()

        # Read input file
        df = read_input(file_path)

        print("Original records:", df.count())

        # Standardize columns
        df = standardize_columns(df)

        # Clean and transform
        df = clean_data(df)

        # Validate records
        valid_df, rejected_df = validate_data(df)

        print("Valid records:", valid_df.count())
        print("Rejected records:", rejected_df.count())

        # Remove duplicate records
        valid_df = remove_duplicates(valid_df)

        print(
            "Records after deduplication:",
            valid_df.count()
        )

        # Save output files
        save_output(valid_df, rejected_df)

        # Incremental loading
        if valid_df.limit(1).count() > 0:

            load_staging(valid_df, run_id)

            incremental_load(run_id)

        else:

            print("No valid records to load")

        print("\n========== ETL COMPLETED ==========")

    except Exception as e:

        print("ETL FAILED:", str(e))
        raise

    finally:

        spark.stop()


# =====================================================
# 14. EXECUTION
# =====================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="CSV, JSON or Parquet file path"
    )

    args = parser.parse_args()

    main(args.input)
