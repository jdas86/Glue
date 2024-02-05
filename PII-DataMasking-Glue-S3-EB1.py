import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
import gs_now


args = getResolvedOptions(sys.argv, ["JOB_NAME","SourceS3Path","TargetS3Path"])
SourceS3Path = args['SourceS3Path'] 
#s3://jd-aws-proj-bucket/employee-src-bkt/

TargetS3Path = args['TargetS3Path']
#s3://jd-aws-proj-bucket-publish/employee_tgt/

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1691035739024 = glueContext.create_dynamic_frame.from_options(
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
    transformation_ctx="AmazonS3_node1691035739024",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    AmazonS3_node1691035739024, ["PERSON_NAME", "EMAIL", "PHONE_NUMBER"], 1.0, 0.1
)


def maskDf(df, keys):
    if not keys:
        return df
    df_to_mask = df.toDF()
    for key in keys:
        df_to_mask = df_to_mask.withColumn(key, lit("************"))
    return DynamicFrame.fromDF(df_to_mask, glueContext, "updated_masked_df")


DetectSensitiveData_node1691035949034 = maskDf(
    AmazonS3_node1691035739024, list(classified_map.keys())
)

# Script generated for node Add Current Timestamp
AddCurrentTimestamp_node1691036499826 = DetectSensitiveData_node1691035949034.gs_now(
    colName="loaddate", dateFormat="%Y%m%d%H%M%S"
)

# Script generated for node Amazon S3
AmazonS3_node1691036773951 = glueContext.write_dynamic_frame.from_options(
    frame=AddCurrentTimestamp_node1691036499826,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": TargetS3Path,
        "partitionKeys": ["loaddate"],
    },
    transformation_ctx="AmazonS3_node1691036773951",
)

job.commit()
