import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1767272807943 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1767272807943")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1767272806584 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1767272806584")

# Script generated for node SQL Query
SqlQuery2444 = '''
select * from t1 join t2 on t1.sensorreadingtime = t2.timestamp

'''
SQLQuery_node1767272828074 = sparkSqlQuery(glueContext, query = SqlQuery2444, mapping = {"t1":step_trainer_trusted_node1767272807943, "t2":accelerometer_trusted_node1767272806584}, transformation_ctx = "SQLQuery_node1767272828074")

# Script generated for node Drop Fields
DropFields_node1767273078231 = DropFields.apply(frame=SQLQuery_node1767272828074, paths=["user", "timestamp", "serialNumber"], transformation_ctx="DropFields_node1767273078231")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1767273078231, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767272273313", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767272833368 = glueContext.getSink(path="s3://sparkglueproject/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767272833368")
AmazonS3_node1767272833368.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learing_curated")
AmazonS3_node1767272833368.setFormat("json")
AmazonS3_node1767272833368.writeFrame(DropFields_node1767273078231)
job.commit()