# Databricks notebook source
import datetime

def get_date_range_from_today(num_days_back):
    diff = datetime.timedelta(num_days_back-1) 
    d1 = datetime.datetime.today().date() - diff - datetime.timedelta(1)
    date_range = "*{"
    for i in range(diff.days + 1):
        date_range = date_range + (d1 + datetime.timedelta(i)).strftime('%Y/%m/%d') + "/*/*/*,"
    date_range = date_range[:len(date_range)-1] + "}" #"}.json"
    return date_range
  
def get_date_range_from_today_wib(num_days_back):
    diff = datetime.timedelta(num_days_back-1) 
    d1 = datetime.datetime.today().date() - diff - datetime.timedelta(1)
    print("diff = " + str(diff))
    date_range = "*{"
    for i in range(diff.days + 1):
        for j in range(24):
          if not(i == 0 and j < 17) and not(i == diff.days and j >= 17):
            date_range = date_range + (d1 + datetime.timedelta(i)).strftime('%Y/%m/%d') + "/" + str(j).zfill(2) + "/*/*,"
    date_range = date_range[:len(date_range)-1] + "}" #"}.json"
    return date_range

def get_date_range(start_date, end_date):
    d1 = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    d2 = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    diff = d2 - d1
    date_range = "*{"
    for i in range(diff.days + 1):
        date_range = date_range + (d1 + datetime.timedelta(i)).strftime('%Y/%m/%d') + "/*/*/*,"
    date_range = date_range[:len(date_range)-1] + "}" #"}.json"
    return date_range
  

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.sql.types import IntegerType
import re

def get_booking_id(tags):
    
    for tag in tags:
      if tag and tag.strip():
            result = re.search("bid_([0-9]+)", tag, re.IGNORECASE)
            if result:
                return int(result.group(1))
    
    return None

get_booking_id = udf(get_booking_id, IntegerType())

# COMMAND ----------

## Initialize Variables

## Start and end date without use_today feature
start_date = "2017-08-08"
end_date   = "2017-08-13"

## Dataset dates interval
# Setting use_today = True will override the start_date and end_date above
# Setting use_today = False will start_date and end_date above instead
# I.e Today is '2017-06-19', and you set lookback_day=2, num_days=4; then start_date='2017-06-14' and end_date='2017-06-17'
use_today = True
lookback_day = 1
num_days = 3
query_date = get_date_range(start_date,end_date)
print(query_date)

# COMMAND ----------

#dbutils.fs.ls("mnt/S3_db_dev/pt_playground")
#df_survey = spark.read.json("/mnt/S3_raw/mongodwh.monitor.client.survey.answer/2017/06/01/*/*/*", schema = schema_survey)

survey_file_name = "dbfs:/mnt/S3_raw/mongodwh.monitor.client.survey.answer/"
survey_file_path = survey_file_name + query_date
df_survey = spark.read.json(survey_file_path)



# COMMAND ----------

display(df_survey)

# COMMAND ----------

df_survey = df_survey.withColumn("booking_id", get_booking_id(df_survey.tags))
df_survey.createOrReplaceTempView("survey")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   TO_DATE(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(CAST(`timestamp` AS BIGINT)/1000 AS BIGINT)),'GMT+7')) AS date_wib,
# MAGIC   CASE WHEN booking_id is null THEN 'undefined' else 'defined' END as has_booking_id,
# MAGIC   count(*)
# MAGIC FROM survey
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC from survey
# MAGIC where 
# MAGIC   TO_DATE(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(CAST(`timestamp` AS BIGINT)/1000 AS BIGINT)),'GMT+7')) = '2017-08-12'
# MAGIC   AND interface = 'desktop'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   TO_DATE(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(CAST(`timestamp` AS BIGINT)/1000 AS BIGINT)),'GMT+7')) AS date_wib,
# MAGIC   count(*)
# MAGIC from survey
# MAGIC where 
# MAGIC   booking_id is not NULL
# MAGIC   AND questionId = 2
# MAGIC group by 1
# MAGIC order by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   TO_DATE(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(CAST(`timestamp` AS BIGINT)/1000 AS BIGINT)),'GMT+0')) AS date_wib,
# MAGIC   booking_id,
# MAGIC  tags[0] as tag1,
# MAGIC  tags[1] as tag2
# MAGIC from survey
# MAGIC where 
# MAGIC   booking_id is not NULL
# MAGIC   AND questionId = 2
# MAGIC   AND TO_DATE(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(CAST(`timestamp` AS BIGINT)/1000 AS BIGINT)),'GMT+0')) = '2017-07-16'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   booking_id, interface
# MAGIC   tags
# MAGIC from survey_nov
# MAGIC where 
# MAGIC   booking_id is NULL
# MAGIC   AND questionId = 2
# MAGIC   AND tags[0] LIKE "%bid_undefined%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   TO_DATE(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(CAST(`timestamp` AS BIGINT)/1000 AS BIGINT)),'GMT+0')) AS date_wib,
# MAGIC   count(*)
# MAGIC from survey_nov
# MAGIC where 
# MAGIC   booking_id is NULL
# MAGIC   AND questionId = 2
# MAGIC   AND tags[0] LIKE "%bid_undefined%"
# MAGIC   AND interface = 'desktop'
# MAGIC group by 1
# MAGIC order by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   TO_DATE(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(CAST(`timestamp` AS BIGINT)/1000 AS BIGINT)),'GMT+0')) AS date_wib,
# MAGIC   count(*)
# MAGIC from survey_nov
# MAGIC where 
# MAGIC   questionId = 2
# MAGIC   AND tags[0] NOT LIKE "%bid_undefined%"
# MAGIC   AND interface = 'desktop'
# MAGIC group by 1
# MAGIC order by 1

# COMMAND ----------

df_result = spark.sql("""SELECT 
                            TO_DATE(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(CAST(`timestamp` AS BIGINT)/1000 AS BIGINT)),'GMT+0')) AS date_wib,
                            COUNT(*) 
                          FROM survey_oct
                          WHERE
                            questionId = 2
                            AND interface = 'desktop'
                            AND booking_id is NULL
                          GROUP BY
                            1
                          UNION
                          SELECT 
                            TO_DATE(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(CAST(`timestamp` AS BIGINT)/1000 AS BIGINT)),'GMT+0')) AS date_wib,
                            COUNT(*) 
                          FROM survey_nov
                          WHERE
                            questionId = 2
                            AND interface = 'desktop'
                            AND booking_id is NULL
                          GROUP BY
                            1                         
                      """)
df_result.createOrReplaceTempView("result")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from result order by date_wib

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   TO_DATE(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(CAST(`timestamp` AS BIGINT)/1000 AS BIGINT)),'GMT+0')) AS date_wib,
# MAGIC   COUNT(*) 
# MAGIC FROM survey_oct
# MAGIC WHERE
# MAGIC   questionId = 2
# MAGIC   AND interface = 'desktop'
# MAGIC GROUP BY
# MAGIC   1
# MAGIC UNION
# MAGIC SELECT 
# MAGIC   TO_DATE(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(CAST(`timestamp` AS BIGINT)/1000 AS BIGINT)),'GMT+0')) AS date_wib,
# MAGIC   COUNT(*) 
# MAGIC FROM survey_nov
# MAGIC WHERE
# MAGIC   questionId = 2
# MAGIC   AND interface = 'desktop'
# MAGIC GROUP BY
# MAGIC   1

# COMMAND ----------

dbutils.fs.ls("mnt/")

# COMMAND ----------

df_experience = spark.read.format("com.databricks.spark.avro").load("/mnt/S3_final_hour_1/edw.fact_experience_booking/2017/06/01/*/*/")
#schema_experience = df_experience.schema

#df_experience = spark.read.format("com.databricks.spark.avro").load("/mnt/S3_final_hour_1/edw.fact_experience_booking/2017/06/10/*/*/", schema=schema_experience)
df_experience.printSchema()

# COMMAND ----------

df_flight = spark.read.format("com.databricks.spark.avro").load("/mnt/S3_final_hour_1/edw.fact_flight_booking/2017/06/01/*/*/")
#schema_experience = df_experience.schema
from pyspark.sql.window import Window


#df_experience = spark.read.format("com.databricks.spark.avro").load("/mnt/S3_final_hour_1/edw.fact_experience_booking/2017/06/10/*/*/", schema=schema_experience)
#df_flight = df_flight.select("booking_id").orderBy('booking_id')
#display(df_flight)

# COMMAND ----------

display(df_flight.select("booking_id","booking_status","modified_at").filter(df_flight.booking_id == '162290951'))

# COMMAND ----------

#use window function row_number to dedup booking.
import pyspark.sql.functions as func
spec = Window.partitionBy(df_flight.booking_id).orderBy(df_flight.modified_at.desc())

row_number = func.row_number().over(spec)

df_flight2 = df_flight.select("booking_id",row_number.alias("row_number"))
df_flight2 = df_flight2.filter(df_flight2.row_number == 1)
display(df_flight2)

# COMMAND ----------

df_experience.groupby(df_experience.booking_status).agg({'booking_id':'count'}).show()

# COMMAND ----------

df_survey.show(2,truncate = True)

# COMMAND ----------

df_survey.count()

# COMMAND ----------

df_survey.describe('tags').show()

# COMMAND ----------

df_survey.select('tags').show(5, truncate = False)

# COMMAND ----------

df_survey.groupby('questionId').agg({'_id':'count'}).show()

# COMMAND ----------

# only select question number 2 (NPS question)
df_survey.filter(df_survey.questionId == 2)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.sql.types import IntegerType
import re

def booking_id_filter(tags):
    regexs = ['bid_[0-9]+']
    
    for tag in tags:
      if tag and tag.strip():
        for r in regexs:
            if re.match(r, tag, re.IGNORECASE):
                return True
    
    return False
  
def get_booking_id(tags):
    
    for tag in tags:
      if tag and tag.strip():
            result = re.search("bid_([0-9]+)", tag, re.IGNORECASE)
            if result:
                return int(result.group(1))
    
    return None
  
has_booking_id = udf(booking_id_filter, BooleanType())
get_booking_id = udf(get_booking_id, IntegerType())

# COMMAND ----------

df_sample = df_survey.sample(False, 0.01, 42)
df_sample.count()

# COMMAND ----------

#calculate row with invalid booking_id. around 6.3% of the data.the filter_udf is the negation of has_booking_id filter. can't find a way to do the negation without having to change the function..
df_no_id = df_survey.filter(filter_udf(df_survey.tags))
df_no_id.groupby('questionId').agg({'_id':'count'}).show()
display(df_no_id.select('tags').distinct())

# COMMAND ----------

# filter data based on question id = 2, extract booking id then drop rows without booking_id
df_survey_2 = df_survey.filter(df_survey.questionId == 2)

df_survey_2 = df_survey_2.withColumn("booking_id", get_booking_id(df_survey.tags))
df_survey_2 = df_survey_2.na.drop(subset=["booking_id"])
display(df_survey_2.select("booking_id","tags","answer"))

# COMMAND ----------

df_survey_2.count()
df_survey_2.createOrReplaceTempView("survey")
df_experience.createOrReplaceTempView("experience")

# COMMAND ----------

df_experience.filter(df_experience.booking_status == 'ISSUED').count(), df_survey_2.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select s.answer, sum(1)
# MAGIC from
# MAGIC   survey s
# MAGIC   inner join experience i
# MAGIC   on
# MAGIC     s.booking_id = i.booking_id
# MAGIC group by s.answer

# COMMAND ----------

df_survey_issued = df_experience.join(df_survey_2, ["booking_id","interface"], how = 'inner')
df_survey_issued.groupby('interface','answer').agg({'booking_id':'count'}).show()

# COMMAND ----------

display(df_survey_issued)

# COMMAND ----------

def get_nps(answer):
    answer = int(answer)
    if answer == 5:
        return 1
    elif answer == 4:
        return 0
    else:
        return -1
    
get_nps = udf(get_nps, IntegerType())
  

# COMMAND ----------

df_survey_issued = df_survey_issued.withColumn('nscore', get_nps(df_survey_issued.answer))

# COMMAND ----------

df_survey_issued.select('answer','nscore').show()
df_survey_issued.groupby('interface').agg('nscore'

# COMMAND ----------

