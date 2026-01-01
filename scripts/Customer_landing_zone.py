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

# Script generated for node customer_landing
customer_landing_node1767267442601 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing_table", transformation_ctx="customer_landing_node1767267442601")

# Script generated for node SQL Query
SqlQuery2262 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
SQLQuery_node1767267447662 = sparkSqlQuery(glueContext, query = SqlQuery2262, mapping = {"myDataSource":customer_landing_node1767267442601}, transformation_ctx = "SQLQuery_node1767267447662")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1767267447662, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767267428548", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767267451010 = glueContext.getSink(path="s3://sparkglueproject/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767267451010")
AmazonS3_node1767267451010.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1767267451010.setFormat("json")
AmazonS3_node1767267451010.writeFrame(SQLQuery_node1767267447662)
job.commit()