import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1767270981998 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1767270981998")

# Script generated for node customer_trusted
customer_trusted_node1767270980051 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1767270980051")

# Script generated for node Join
Join_node1767271011495 = Join.apply(frame1=accelerometer_trusted_node1767270981998, frame2=customer_trusted_node1767270980051, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1767271011495")

# Script generated for node Drop Duplicates
DropDuplicates_node1767271019282 =  DynamicFrame.fromDF(Join_node1767271011495.toDF().dropDuplicates(["user"]), glueContext, "DropDuplicates_node1767271019282")

# Script generated for node Drop Fields
DropFields_node1767271466129 = DropFields.apply(frame=DropDuplicates_node1767271019282, paths=["z", "user", "x", "y", "timestamp"], transformation_ctx="DropFields_node1767271466129")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1767271466129, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767270942191", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767271039808 = glueContext.getSink(path="s3://sparkglueproject/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767271039808")
AmazonS3_node1767271039808.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_cuarated")
AmazonS3_node1767271039808.setFormat("json")
AmazonS3_node1767271039808.writeFrame(DropFields_node1767271466129)
job.commit()