from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


 
#Instantiate spark builder and Set spark app name. Also, enable hive support using enableHiveSupport option of spark builder.
spark = (SparkSession
                .builder
                .appName('pyspark-read-and-write-from-hive')
                .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
                .enableHiveSupport()
                .getOrCreate()
                )
#Read hive table in spark using sql method of spark session class
df_pat_contact = spark.sql("select * from Patients_Contact_Info") # Patient Contact information
df_pat_vital= spark.sql("select * from Patients_Vital_Info  ")  # Patient Vital Information
df_pat_ref  = spark.sql("select * from Threshold_Reference_Table ") # Vital Threshold level
df_pat_vital_contact=df_pat_vital.join(df_pat_contact, expr("""customerid = patientid"""), "left_outer")
df_pat_vital_contact.createOrReplaceTempView("allpatients")
high_heartbeat_under40=spark.sql("select * from allpatients where age<=40 and heartbeat>78")
low_heartbeat_under40=spark.sql("select * from allpatients where age<=40 and heartbeat<70")
high_bp_under40=spark.sql("select * from allpatients where age<=40 and bp>220")
low_bp_under40=spark.sql("select * from allpatients where age<=40 and bp<161")
high_heartbeat_over40=spark.sql("select * from allpatients where age>40 and heartbeat>73")
low_heartbeat_over40=spark.sql("select * from allpatients where age>40 and heartbeat<66")
high_bp_over40=spark.sql("select * from allpatients where age>40 and bp>180")
low_bp_over40=spark.sql("select * from allpatients where age>40 and bp<151")




high_heartbeat_under40_pat=high_heartbeat_under40 \
			.withColumn("patientname",high_heartbeat_under40.patientname) \
                        .withColumn("age",high_heartbeat_under40.age)\
                        .withColumn("patientaddress",high_heartbeat_under40.patientaddress) \
                        .withColumn("phone_number",high_heartbeat_under40.phone_number)\
                        .withColumn("admitted_ward",high_heartbeat_under40.admitted_ward) \
                        .withColumn("bp",high_heartbeat_under40.bp) \
                        .withColumn("heartBeat",high_heartbeat_under40.heartBeat) \
                        .withColumn("input_message_time",high_heartbeat_under40.time) \
                        .withColumn("alert_generated_time",current_timestamp()) \
                        .withColumn("alert_message","Higher heartbeat than normal") \
low_heartbeat_under40_pat=low_heartbeat_under40 \
			.withColumn("patientname",low_heartbeat_under40.patientname) \
                        .withColumn("age",low_heartbeat_under40.age)\
                        .withColumn("patientaddress",low_heartbeat_under40.patientaddress) \
                        .withColumn("phone_number",low_heartbeat_under40.phone_number)\
                        .withColumn("admitted_ward",low_heartbeat_under40.admitted_ward) \
                        .withColumn("bp",low_heartbeat_under40.bp) \
                        .withColumn("heartBeat",low_heartbeat_under40.heartBeat) \
                        .withColumn("input_message_time",low_heartbeat_under40.time) \
                        .withColumn("alert_generated_time",current_timestamp()) \
                        .withColumn("alert_message","low heartbeat than normal") \
high_bp_under40_pat=high_bp_under40 \
			.withColumn("patientname",high_bp_under40.patientname) \
                        .withColumn("age",high_bp_under40.age)\
                        .withColumn("patientaddress",high_bp_under40.patientaddress) \
                        .withColumn("phone_number",high_bp_under40.phone_number)\
                        .withColumn("admitted_ward",high_bp_under40.admitted_ward) \
                        .withColumn("bp",high_bp_under40.bp) \
                        .withColumn("heartBeat",high_bp_under40.heartBeat) \
                        .withColumn("input_message_time",high_bp_under40.time) \
                        .withColumn("alert_generated_time",current_timestamp()) \
                        .withColumn("alert_message","higher bp than normal") \

low_bp_under40_pat=low_bp_under40 \
			.withColumn("patientname",low_bp_under40.patientname) \
                        .withColumn("age",low_bp_under40.age)\
                        .withColumn("patientaddress",low_bp_under40.patientaddress) \
                        .withColumn("phone_number",low_bp_under40.phone_number)\
                        .withColumn("admitted_ward",low_bp_under40.admitted_ward) \
                        .withColumn("bp",low_bp_under40.bp) \
                        .withColumn("heartBeat",low_bp_under40.heartBeat) \
                        .withColumn("input_message_time",low_bp_under40.time) \
                        .withColumn("alert_generated_time",current_timestamp()) \
                        .withColumn("alert_message","low bp than normal") \
                    
high_heartbeat_over40_pat=high_heartbeat_over40 \
			.withColumn("patientname",high_heartbeat_over40.patientname) \
                        .withColumn("age",high_heartbeat_over40.age)\
                        .withColumn("patientaddress",high_heartbeat_over40.patientaddress) \
                        .withColumn("phone_number",high_heartbeat_over40.phone_number)\
                        .withColumn("admitted_ward",high_heartbeat_over40.admitted_ward) \
                        .withColumn("bp",high_heartbeat_over40.bp) \
                        .withColumn("heartBeat",high_heartbeat_over40.heartBeat) \
                        .withColumn("input_message_time",high_heartbeat_over40.time) \
                        .withColumn("alert_generated_time",current_timestamp()) \
                        .withColumn("alert_message","Higher heartbeat than normal") \
low_heartbeat_over40_pat=low_heartbeat_over40 \
			.withColumn("patientname",low_heartbeat_over40.patientname) \
                        .withColumn("age",low_heartbeat_over40.age)\
                        .withColumn("patientaddress",low_heartbeat_over40.patientaddress) \
                        .withColumn("phone_number",low_heartbeat_over40.phone_number)\
                        .withColumn("admitted_ward",low_heartbeat_over40.admitted_ward) \
                        .withColumn("bp",low_heartbeat_over40.bp) \
                        .withColumn("heartBeat",low_heartbeat_over40.heartBeat) \
                        .withColumn("input_message_time",low_heartbeat_over40.time) \
                        .withColumn("alert_generated_time",current_timestamp()) \
                        .withColumn("alert_message","low heartbeat than normal") \
high_bp_over40_pat=high_bp_over40 \
			.withColumn("patientname",high_bp_over40.patientname) \
                        .withColumn("age",high_bp_over40.age)\
                        .withColumn("patientaddress",high_bp_over40.patientaddress) \
                        .withColumn("phone_number",high_bp_over40.phone_number)\
                        .withColumn("admitted_ward",high_bp_over40.admitted_ward) \
                        .withColumn("bp",high_bp_over40.bp) \
                        .withColumn("heartBeat",high_bp_over40.heartBeat) \
                        .withColumn("input_message_time",high_bp_over40.time) \
                        .withColumn("alert_generated_time",current_timestamp()) \
                        .withColumn("alert_message","higher bp than normal") \

low_bp_over40_pat=low_bp_over40 \
			.withColumn("patientname",low_bp_over40.patientname) \
                        .withColumn("age",low_bp_over40.age)\
                        .withColumn("patientaddress",low_bp_over40.patientaddress) \
                        .withColumn("phone_number",low_bp_over40.phone_number)\
                        .withColumn("admitted_ward",low_bp_over40.admitted_ward) \
                        .withColumn("bp",low_bp_over40.bp) \
                        .withColumn("heartBeat",low_bp_over40.heartBeat) \
                        .withColumn("input_message_time",low_bp_over40.time) \
                        .withColumn("alert_generated_time",current_timestamp()) \
                        .withColumn("alert_message","low bp than normal") \                
                    
query1 = high_heartbeat_under40_pat  \
        .select("patientname","age","patientaddress","phone_number","admitted_ward","bp","heartBeat","input_message_time","alert_generated_time","alert_message") \
	.writeStream  \
	.outputMode("append")  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","localhost:9092")  \
	.option("topic",'doctors-queue')  \
	.option("checkpointLocation","/user/cp3")  \
	.start()
	
query2 = low_heartbeat_under40_pat  \
	.select("patientname","age","patientaddress","phone_number","admitted_ward","bp","heartBeat","input_message_time","alert_generated_time","alert_message") \
        .writeStream  \
	.outputMode("append")  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","localhost:9092")  \
	.option("topic",'doctors-queue')  \
	.option("checkpointLocation","/user/cp4")  \
	.start()

query3 = low_bp_under40_pat  \
	.select("patientname","age","patientaddress","phone_number","admitted_ward","bp","heartBeat","input_message_time","alert_generated_time","alert_message") \
        .writeStream  \
	.outputMode("append")  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","localhost:9092")  \
	.option("topic",'doctors-queue')  \
	.option("checkpointLocation","/user/cp5")  \
	.start()

query4 = high_bp_under40_pat  \
	.select("patientname","age","patientaddress","phone_number","admitted_ward","bp","heartBeat","input_message_time","alert_generated_time","alert_message") \
        .writeStream  \
	.outputMode("append")  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","localhost:9092")  \
	.option("topic",'doctors-queue')  \
	.option("checkpointLocation","/user/cp6")  \
	.start()


query5 = high_heartbeat_over40_pat  \
	.select("patientname","age","patientaddress","phone_number","admitted_ward","bp","heartBeat","input_message_time","alert_generated_time","alert_message") \
        .writeStream  \
	.outputMode("append")  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","localhost:9092")  \
	.option("topic",'doctors-queue')  \
	.option("checkpointLocation","/user/cp7")  \
	.start()
	
query6 = low_heartbeat_over40_pat  \
	.select("patientname","age","patientaddress","phone_number","admitted_ward","bp","heartBeat","input_message_time","alert_generated_time","alert_message") \
        .writeStream  \
	.outputMode("append")  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","localhost:9092")  \
	.option("topic",'doctors-queue')  \
	.option("checkpointLocation","/user/cp8")  \
	.start()

query7 = low_bp_over40_pat  \
	.select("patientname","age","patientaddress","phone_number","admitted_ward","bp","heartBeat","input_message_time","alert_generated_time","alert_message") \
        .writeStream  \
	.outputMode("append")  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","localhost:9092")  \
	.option("topic",'doctors-queue')  \
	.option("checkpointLocation","/user/cp9")  \
	.start()

query8 = high_bp_over40_pat  \
.select("patientname","age","patientaddress","phone_number","admitted_ward","bp","heartBeat","input_message_time","alert_generated_time","alert_message") \
	.writeStream  \
	.outputMode("append")  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","localhost:9092")  \
	.option("topic",'doctors-queue')  \
	.option("checkpointLocation","/user/cp10")  \
	.start()


# Wait until any of the queries on the associated SQLContext has terminated 
spark.streams.awaitAnyTermination()
