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

# Script generated for node customer_curated
customer_curated_node1767272157799 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_cuarated", transformation_ctx="customer_curated_node1767272157799")

# Script generated for node step_trainer_landing
step_trainer_landing_node1767272155994 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing_table", transformation_ctx="step_trainer_landing_node1767272155994")

# Script generated for node SQL Query
SqlQuery2333 = '''
select * from t1 join t2 on t1.serialnumber = t2.serialnumber

'''
SQLQuery_node1767272169585 = sparkSqlQuery(glueContext, query = SqlQuery2333, mapping = {"t1":customer_curated_node1767272157799, "t2":step_trainer_landing_node1767272155994}, transformation_ctx = "SQLQuery_node1767272169585")

# Script generated for node Drop Fields
DropFields_node1767272176730 = DropFields.apply(frame=SQLQuery_node1767272169585, paths=["birthDay", "shareWithPublicAsOfDate", "shareWithResearchAsOfDate", "registrationDate", "customerName", "shareWithFriendsAsOfDate", "email", "lastUpdateDate", "phone"], transformation_ctx="DropFields_node1767272176730")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1767272176730, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767270942191", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767272181689 = glueContext.getSink(path="s3://sparkglueproject/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767272181689")
AmazonS3_node1767272181689.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1767272181689.setFormat("json")
AmazonS3_node1767272181689.writeFrame(DropFields_node1767272176730)
job.commit()