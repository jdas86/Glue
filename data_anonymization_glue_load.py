import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1690062717387 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://jd-aws-proj-bucket/employee-src-bkt/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1690062717387",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    AmazonS3_node1690062717387, ["PERSON_NAME", "EMAIL", "PHONE_NUMBER"], 1.0, 0.1
)


def maskDf(df, keys):
    if not keys:
        return df
    df_to_mask = df.toDF()
    for key in keys:
        df_to_mask = df_to_mask.withColumn(key, lit("**********"))
    return DynamicFrame.fromDF(df_to_mask, glueContext, "updated_masked_df")


DetectSensitiveData_node1690062789776 = maskDf(
    AmazonS3_node1690062717387, list(classified_map.keys())
)

# Script generated for node Amazon S3-Publish
AmazonS3Publish_node1690063724008 = glueContext.getSink(
    path="s3://jd-aws-proj-bucket/target-zone-bkt/employee/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3Publish_node1690063724008",
)
AmazonS3Publish_node1690063724008.setCatalogInfo(
    catalogDatabase="jd-catalog-s3", catalogTableName="employee-pz"
)
AmazonS3Publish_node1690063724008.setFormat("glueparquet")
AmazonS3Publish_node1690063724008.writeFrame(DetectSensitiveData_node1690062789776)
job.commit()
