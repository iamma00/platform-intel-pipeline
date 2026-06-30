from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, when, udf
from pyspark.sql.types import StringType

CORPORATE_KEYWORDS = ['pvt ltd', 'llp', 'tower', 'floor', 'park',
                       'solutions', 'technologies', 'plot no', 'it park',
                       'suite', 'building', 'corp', 'industries',
                       'business', 'enterprises', 'estate', 'midc']

RESIDENTIAL_KEYWORDS = ['house no', 'h.no', 'flat', 'society', 'nagar',
                         'apartment', 'colony', 'residency', 'row house']

S3_INPUT_PATH = "s3a://address-intel-pipeline-poc/raw/addresses.csv"

JDBC_URL = "jdbc:postgresql://POSTGRES_HOST:5432/address_poc"
JDBC_PROPS = {"user": "poc_user", "password": "poc_pass", "driver": "org.postgresql.Driver"}


def get_matches(address, keyword_list):
    addr = address.lower()
    return [kw for kw in keyword_list if kw in addr]

@udf(returnType=StringType())
def matched_keywords_udf(address):
    hits = get_matches(address, CORPORATE_KEYWORDS) + get_matches(address, RESIDENTIAL_KEYWORDS)
    return ", ".join(hits) if hits else "none"

@udf(returnType=StringType())
def confidence_udf(address):
    total = len(get_matches(address, CORPORATE_KEYWORDS)) + len(get_matches(address, RESIDENTIAL_KEYWORDS))
    return "None" if total == 0 else ("Low" if total == 1 else "High")


def build_spark():
    return SparkSession.builder \
        .appName("AddressClassifierK8s") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .getOrCreate()


def classify_df(df):
    df = df.withColumn("address_lower", lower(col("address")))

    corp_expr, res_expr = None, None
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
        .otherwise("Unknown")
    )
    df = df.withColumn("matched_keywords", matched_keywords_udf(col("address")))
    df = df.withColumn("confidence", confidence_udf(col("address")))
    return df.drop("address_lower")


def main():
    spark = build_spark()

    df = spark.read.csv(S3_INPUT_PATH, header=True, inferSchema=True)
    classified_df = classify_df(df)
    classified_df.select("name", "address", "address_type", "matched_keywords", "confidence") \
        .show(30, truncate=False)

    print("\n--- Classification Summary ---")
    classified_df.groupBy("address_type").count().show()

    classified_df.select("name", "address", "pincode", "city",
                          "address_type", "matched_keywords", "confidence") \
        .write.jdbc(url=JDBC_URL, table="addresses", mode="append", properties=JDBC_PROPS)

    print("Written to Postgres.")
    spark.stop()


if __name__ == "__main__":
    main()
