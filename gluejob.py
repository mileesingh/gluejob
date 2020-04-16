import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "ses-sftp-database", table_name = "sample_", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "ses-sftp-database", table_name = "sample_", transformation_ctx = "datasource0")
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "ses-sftp-database", table_name = "shopping", transformation_ctx = "datasource1")
source0=datasource0.toDF()
source1=datasource1.toDF()
joinframe = source0.join(source1, source0.identifier == source1.identifier,how='left_outer')
## @type: ApplyMapping
## @args: [mapping = [("username", "string", "username", "string"), ("identifier", "long", "identifier", "long"), ("first name", "string", "first name", "string"), ("last name", "string", "last name", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
##applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("username", "string", "username", "string"), ("identifier", "long", "identifier", "long"), ("first name", "string", "first name", "string"), ("last name", "string", "last name", "string")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://etl-sftp-bucket"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(joinframe.repartition(1),glueContext,"frame"), connection_type = "s3", connection_options = {"path": "s3://etl-sftp-bucket"}, format = "csv", transformation_ctx = "datasink2")
job.commit()