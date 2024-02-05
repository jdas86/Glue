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

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1690307918064 = glueContext.create_dynamic_frame.from_catalog(
    database="jd-catalog-s3",
    table_name="employee_src_bkt",
    transformation_ctx="AWSGlueDataCatalog_node1690307918064",
)

# Script generated for node Change Schema
ChangeSchema_node1690308036301 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1690307918064,
    mappings=[
        ("employee_id", "long", "employee_id", "string"),
        ("first_name", "string", "first_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("email", "string", "email", "string"),
        ("phone_number", "string", "phone_number", "string"),
        ("hire_date", "string", "hire_date", "string"),
        ("job_id", "string", "job_id", "string"),
        ("salary", "long", "salary", "string"),
        ("commission_pct", "long", "commission_pct", "string"),
        ("manager_id", "long", "manager_id", "string"),
        ("department_id", "long", "department_id", "string"),
    ],
    transformation_ctx="ChangeSchema_node1690308036301",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    ChangeSchema_node1690308036301, ["PERSON_NAME", "EMAIL", "PHONE_NUMBER"], 1.0, 0.1
)


def maskDf(df, keys):
    if not keys:
        return df
    df_to_mask = df.toDF()
    for key in keys:
        df_to_mask = df_to_mask.withColumn(key, lit("**********"))
    return DynamicFrame.fromDF(df_to_mask, glueContext, "updated_masked_df")


DetectSensitiveData_node1690308205650 = maskDf(
    ChangeSchema_node1690308036301, list(classified_map.keys())
)

# Script generated for node Add Current Timestamp
AddCurrentTimestamp_node1690308995119 = DetectSensitiveData_node1690308205650.gs_now(
    colName="LoadDate", dateFormat="yyyyMMdd"
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1690310900654 = glueContext.write_dynamic_frame.from_catalog(
    frame=AddCurrentTimestamp_node1690308995119,
    database="jd-catalog-s3",
    table_name="employee_data",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
        "partitionKeys": ["loaddate"],
    },
    transformation_ctx="AWSGlueDataCatalog_node1690310900654",
)

job.commit()
