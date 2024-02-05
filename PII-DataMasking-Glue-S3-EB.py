import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ["JOB_NAME","SourceS3Path","TargetS3Path"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SourceS3Path = args['SourceS3Path'] 
#s3://jd-aws-proj-bucket/employee-src-bkt/

TargetS3Path = args['TargetS3Path']
#s3://jd-aws-proj-bucket-publish/employee_tgt/

# Script generated for node Amazon S3
AmazonS3_node1691033759464 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": -1,
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [SourceS3Path],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1691033759464",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    AmazonS3_node1691033759464, ["PERSON_NAME", "PHONE_NUMBER", "EMAIL"], 1.0, 0.1
)


def maskDf(df, keys):
    if not keys:
        return df
    df_to_mask = df.toDF()
    for key in keys:
        df_to_mask = df_to_mask.withColumn(key, lit("************"))
    return DynamicFrame.fromDF(df_to_mask, glueContext, "updated_masked_df")


DetectSensitiveData_node1691033986217 = maskDf(
    AmazonS3_node1691033759464, list(classified_map.keys())
)

AddCurrentTimestamp_node = DetectSensitiveData_node1691033986217.gs_now(
    colName="LoadDate", dateFormat="yyyyMMdd"
)


# Script generated for node Amazon S3
AmazonS3_node1691034171091 = glueContext.write_dynamic_frame.from_options(
    frame=AddCurrentTimestamp_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": TargetS3Path,
        "partitionKeys": ["loaddate"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1691034171091",
)

job.commit()
