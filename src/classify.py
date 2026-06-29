from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, when, udf, array, lit
from pyspark.sql.types import StringType

CORPORATE_KEYWORDS = ['pvt ltd', 'llp', 'tower', 'floor', 'park',
                       'solutions', 'technologies', 'plot no', 'it park',
                       'suite', 'building', 'corp', 'industries',
                       'business', 'enterprises', 'estate', 'midc']

RESIDENTIAL_KEYWORDS = ['house no', 'h.no', 'flat', 'society', 'nagar',
                         'apartment', 'colony', 'residency', 'row house']

JDBC_URL = "jdbc:postgresql://postgres:5432/address_poc"
JDBC_PROPS = {"user": "poc_user", "password": "poc_pass", "driver": "org.postgresql.Driver"}


def get_matches(address, keyword_list):
    addr = address.lower()
    return [kw for kw in keyword_list if kw in addr]

@udf(returnType=StringType())
def matched_keywords_udf(address):
    corp_hits = get_matches(address, CORPORATE_KEYWORDS)
    res_hits = get_matches(address, RESIDENTIAL_KEYWORDS)
    all_hits = corp_hits + res_hits
    return ", ".join(all_hits) if all_hits else "none"

@udf(returnType=StringType())
def confidence_udf(address):
    corp_hits = get_matches(address, CORPORATE_KEYWORDS)
    res_hits = get_matches(address, RESIDENTIAL_KEYWORDS)
    total_hits = len(corp_hits) + len(res_hits)
    if total_hits == 0:
        return "None"
    elif total_hits == 1:
        return "Low"
    else:
        return "High"


def build_spark():
    return SparkSession.builder \
        .appName("AddressClassifierPOC") \
        .config("spark.jars", "/opt/spark-jars/postgresql-42.7.3.jar") \
        .getOrCreate()


def classify_df(df):
    df = df.withColumn("address_lower", lower(col("address")))

    corp_expr = None
    res_expr = None
    for kw in CORPORATE_KEYWORDS:
        cond = col("address_lower").contains(kw)
        corp_expr = cond if corp_expr is None else (corp_expr | cond)
    for kw in RESIDENTIAL_KEYWORDS:
        cond = col("address_lower").contains(kw)
        res_expr = cond if res_expr is None else (res_expr | cond)

    df = df.withColumn(
        "address_type",
        when(corp_expr & ~res_expr, "Corporate")
        .when(res_expr & ~corp_expr, "Residential")
        .when(corp_expr & res_expr, "Unknown")   # both matched -> ambiguous, needs review
        .otherwise("Unknown")
    )

    df = df.withColumn("matched_keywords", matched_keywords_udf(col("address")))
    df = df.withColumn("confidence", confidence_udf(col("address")))

    return df.drop("address_lower")


def main():
    spark = build_spark()

    # 1. Load raw CSV (no label column)
    df = spark.read.csv("/app/data/addresses.csv", header=True, inferSchema=True)

    # 2. Classify
    classified_df = classify_df(df)
    classified_df.select("name", "address", "address_type", "matched_keywords", "confidence") \
        .show(30, truncate=False)

    # 3. Summary counts
    print("\n--- Classification Summary ---")
    classified_df.groupBy("address_type").count().show()

    # 4. Write results to Postgres
    classified_df.select("name", "address", "pincode", "city",
                          "address_type", "matched_keywords", "confidence") \
        .write.jdbc(url=JDBC_URL, table="addresses", mode="append", properties=JDBC_PROPS)

    print("Written to Postgres.")
    spark.stop()


if __name__ == "__main__":
    main()