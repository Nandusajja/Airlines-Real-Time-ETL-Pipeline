import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node flight_data
flight_data_node1752926866484 = glueContext.create_dynamic_frame.from_catalog(database="airlines_datamart", table_name="flights_data", transformation_ctx="flight_data_node1752926866484")

# Script generated for node airport_dim
airport_dim_node1752926896866 = glueContext.create_dynamic_frame.from_catalog(database="airlines_datamart", table_name="dev_airlines_airports_dim",redshift_tmp_dir="s3://redshift-temp-bucket123/temp_data/airline_dim/", transformation_ctx="airport_dim_node1752926896866")

# Script generated for node Filter
Filter_node1752926880666 = Filter.apply(frame=flight_data_node1752926866484, f=lambda row: (row["depdelay"] >= 60), transformation_ctx="Filter_node1752926880666")

# Script generated for node Join_dep
Filter_node1752926880666DF = Filter_node1752926880666.toDF()
airport_dim_node1752926896866DF = airport_dim_node1752926896866.toDF()
Join_dep_node1752926891383 = DynamicFrame.fromDF(Filter_node1752926880666DF.join(airport_dim_node1752926896866DF, (Filter_node1752926880666DF['originairportid'] == airport_dim_node1752926896866DF['airport_id']), "left"), glueContext, "Join_dep_node1752926891383")

# Script generated for node modify_dep
modify_dep_node1752927016399 = ApplyMapping.apply(frame=Join_dep_node1752926891383, mappings=[("carrier", "string", "carrier", "string"), ("destairportid", "long", "destairportid", "long"), ("depdelay", "long", "dep_delay", "bigint"), ("arrdelay", "long", "arr_delay", "bigint"), ("city", "string", "dep_city", "string"), ("name", "string", "dep_airport", "string"), ("state", "string", "dep_state", "string")], transformation_ctx="modify_dep_node1752927016399")

# Script generated for node Join_arr
modify_dep_node1752927016399DF = modify_dep_node1752927016399.toDF()
airport_dim_node1752926896866DF = airport_dim_node1752926896866.toDF()
Join_arr_node1752927076585 = DynamicFrame.fromDF(modify_dep_node1752927016399DF.join(airport_dim_node1752926896866DF, (modify_dep_node1752927016399DF['destairportid'] == airport_dim_node1752926896866DF['airport_id']), "left"), glueContext, "Join_arr_node1752927076585")

# Script generated for node modify_arr
modify_arr_node1752927110783 = ApplyMapping.apply(frame=Join_arr_node1752927076585, mappings=[("carrier", "string", "carrier", "string"), ("dep_state", "string", "dep_state", "string"), ("state", "string", "arr_state", "string"), ("arr_delay", "bigint", "arr_delay", "long"), ("city", "string", "arr_city", "string"), ("dep_city", "string", "dep_city", "string"), ("dep_delay", "bigint", "dep_delay", "long"), ("dep_airport", "string", "dep_airport", "string")], transformation_ctx="modify_arr_node1752927110783")

# Script generated for node target_redshift
target_redshift_node1752927161448 = glueContext.write_dynamic_frame.from_catalog(frame=modify_arr_node1752927110783, database="airlines_datamart", table_name="dev_airlines_daily_flights_fact", redshift_tmp_dir="s3://redshift-temp-bucket123/temp_data/airline_fact/",additional_options={"aws_iam_role": "arn:aws:iam::196219932948:role/temp_role"}, transformation_ctx="target_redshift_node1752927161448")

job.commit()