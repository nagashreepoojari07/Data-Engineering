# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE users (
# MAGIC     USER_ID INT PRIMARY KEY,
# MAGIC     USER_NAME VARCHAR(20) NOT NULL,
# MAGIC     USER_STATUS VARCHAR(20) NOT NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE logins (
# MAGIC     USER_ID INT,
# MAGIC     LOGIN_TIMESTAMP DATETIME NOT NULL,
# MAGIC     SESSION_ID INT PRIMARY KEY,
# MAGIC     SESSION_SCORE INT,
# MAGIC     FOREIGN KEY (USER_ID) REFERENCES USERS(USER_ID)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Users Table
# MAGIC INSERT INTO USERS VALUES (1, 'Alice', 'Active');
# MAGIC INSERT INTO USERS VALUES (2, 'Bob', 'Inactive');
# MAGIC INSERT INTO USERS VALUES (3, 'Charlie', 'Active');
# MAGIC INSERT INTO USERS  VALUES (4, 'David', 'Active');
# MAGIC INSERT INTO USERS  VALUES (5, 'Eve', 'Inactive');
# MAGIC INSERT INTO USERS  VALUES (6, 'Frank', 'Active');
# MAGIC INSERT INTO USERS  VALUES (7, 'Grace', 'Inactive');
# MAGIC INSERT INTO USERS  VALUES (8, 'Heidi', 'Active');
# MAGIC INSERT INTO USERS VALUES (9, 'Ivan', 'Inactive');
# MAGIC INSERT INTO USERS VALUES (10, 'Judy', 'Active');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO LOGINS  VALUES (1, '2023-07-15 09:30:00', 1001, 85);
# MAGIC INSERT INTO LOGINS VALUES (2, '2023-07-22 10:00:00', 1002, 90);
# MAGIC INSERT INTO LOGINS VALUES (3, '2023-08-10 11:15:00', 1003, 75);
# MAGIC INSERT INTO LOGINS VALUES (4, '2023-08-20 14:00:00', 1004, 88);
# MAGIC INSERT INTO LOGINS  VALUES (5, '2023-09-05 16:45:00', 1005, 82);
# MAGIC INSERT INTO LOGINS  VALUES (6, '2023-10-12 08:30:00', 1006, 77);
# MAGIC INSERT INTO LOGINS  VALUES (7, '2023-11-18 09:00:00', 1007, 81);
# MAGIC INSERT INTO LOGINS VALUES (8, '2023-12-01 10:30:00', 1008, 84);
# MAGIC INSERT INTO LOGINS  VALUES (9, '2023-12-15 13:15:00', 1009, 79);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (1, '2024-01-10 07:45:00', 1011, 86);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (2, '2024-01-25 09:30:00', 1012, 89);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (3, '2024-02-05 11:00:00', 1013, 78);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (4, '2024-03-01 14:30:00', 1014, 91);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (5, '2024-03-15 16:00:00', 1015, 83);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (6, '2024-04-12 08:00:00', 1016, 80);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (7, '2024-05-18 09:15:00', 1017, 82);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (8, '2024-05-28 10:45:00', 1018, 87);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (9, '2024-06-15 13:30:00', 1019, 76);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (10, '2024-06-25 15:00:00', 1010, 92);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (10, '2024-06-26 15:45:00', 1020, 93);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (10, '2024-06-27 15:00:00', 1021, 92);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (10, '2024-06-28 15:45:00', 1022, 93);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (1, '2024-01-10 07:45:00', 1101, 86);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (3, '2024-01-25 09:30:00', 1102, 89);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (5, '2024-01-15 11:00:00', 1103, 78);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (2, '2023-11-10 07:45:00', 1201, 82);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (4, '2023-11-25 09:30:00', 1202, 84);
# MAGIC INSERT INTO LOGINS (USER_ID, LOGIN_TIMESTAMP, SESSION_ID, SESSION_SCORE) VALUES (6, '2023-11-15 11:00:00', 1203, 80);

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Define schema for users DataFrame
users_schema = StructType([
    StructField("USER_ID", IntegerType(), True),
    StructField("USER_NAME", StringType(), True),
    StructField("USER_STATUS", StringType(), True)
])

# Create data for users DataFrame
users_data = [
    (1, 'Alice', 'Active'),
    (2, 'Bob', 'Inactive'),
    (3, 'Charlie', 'Active'),
    (4, 'David', 'Active'),
    (5, 'Eve', 'Inactive'),
    (6, 'Frank', 'Active'),
    (7, 'Grace', 'Inactive'),
    (8, 'Heidi', 'Active'),
    (9, 'Ivan', 'Inactive'),
    (10, 'Judy', 'Active')
]

# Create users DataFrame
users_df = spark.createDataFrame(users_data, schema=users_schema)

# Display users DataFrame
display(users_df)
users_df.createOrReplaceTempView("users_df")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

# Define schema for logins DataFrame
logins_schema = StructType([
    StructField("USER_ID", IntegerType(), True),
    StructField("LOGIN_TIMESTAMP", TimestampType(), True),
    StructField("SESSION_ID", IntegerType(), True),
    StructField("SESSION_SCORE", IntegerType(), True)
])

# Create data for logins DataFrame
logins_data = [
    (1, datetime.strptime('2023-07-15 09:30:00', '%Y-%m-%d %H:%M:%S'), 1001, 85),
    (2, datetime.strptime('2023-07-22 10:00:00', '%Y-%m-%d %H:%M:%S'), 1002, 90),
    (3, datetime.strptime('2023-08-10 11:15:00', '%Y-%m-%d %H:%M:%S'), 1003, 75),
    (4, datetime.strptime('2023-08-20 14:00:00', '%Y-%m-%d %H:%M:%S'), 1004, 88),
    (5, datetime.strptime('2023-09-05 16:45:00', '%Y-%m-%d %H:%M:%S'), 1005, 82),
    (6, datetime.strptime('2023-10-12 08:30:00', '%Y-%m-%d %H:%M:%S'), 1006, 77),
    (7, datetime.strptime('2023-11-18 09:00:00', '%Y-%m-%d %H:%M:%S'), 1007, 81),
    (8, datetime.strptime('2023-12-01 10:30:00', '%Y-%m-%d %H:%M:%S'), 1008, 84),
    (9, datetime.strptime('2023-12-15 13:15:00', '%Y-%m-%d %H:%M:%S'), 1009, 79),
    (1, datetime.strptime('2025-01-10 07:45:00', '%Y-%m-%d %H:%M:%S'), 1011, 86),
    (2, datetime.strptime('2024-01-25 09:30:00', '%Y-%m-%d %H:%M:%S'), 1012, 89),
    (3, datetime.strptime('2024-02-05 11:00:00', '%Y-%m-%d %H:%M:%S'), 1013, 78),
    (4, datetime.strptime('2024-03-01 14:30:00', '%Y-%m-%d %H:%M:%S'), 1014, 91),
    (5, datetime.strptime('2024-03-15 16:00:00', '%Y-%m-%d %H:%M:%S'), 1015, 83),
    (6, datetime.strptime('2024-04-12 08:00:00', '%Y-%m-%d %H:%M:%S'), 1016, 80),
    (7, datetime.strptime('2024-05-18 09:15:00', '%Y-%m-%d %H:%M:%S'), 1017, 82),
    (8, datetime.strptime('2024-05-28 10:45:00', '%Y-%m-%d %H:%M:%S'), 1018, 87),
    (9, datetime.strptime('2024-12-15 13:30:00', '%Y-%m-%d %H:%M:%S'), 1019, 76),
    (10, datetime.strptime('2024-06-25 15:00:00', '%Y-%m-%d %H:%M:%S'), 1010, 92),
    (10, datetime.strptime('2024-06-26 15:45:00', '%Y-%m-%d %H:%M:%S'), 1020, 93),
    (10, datetime.strptime('2024-06-27 15:00:00', '%Y-%m-%d %H:%M:%S'), 1021, 92),
    (10, datetime.strptime('2024-06-28 15:45:00', '%Y-%m-%d %H:%M:%S'), 1022, 93),
    (1, datetime.strptime('2024-01-10 07:45:00', '%Y-%m-%d %H:%M:%S'), 1101, 86),
    (3, datetime.strptime('2024-01-25 09:30:00', '%Y-%m-%d %H:%M:%S'), 1102, 89),
    (5, datetime.strptime('2024-01-15 11:00:00', '%Y-%m-%d %H:%M:%S'), 1103, 78),
    (2, datetime.strptime('2024-11-10 07:45:00', '%Y-%m-%d %H:%M:%S'), 1201, 82),
    (4, datetime.strptime('2023-11-25 09:30:00', '%Y-%m-%d %H:%M:%S'), 1202, 84),
    (6, datetime.strptime('2024-11-15 11:00:00', '%Y-%m-%d %H:%M:%S'), 1203, 80)
]

# Create logins DataFrame
logins_df = spark.createDataFrame(logins_data, schema=logins_schema)

# Display logins DataFrame
display(logins_df)
logins_df.createOrReplaceTempView("logins_df")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Q1: Management wants to see all the users who have not logged in the past 5 months.

# COMMAND ----------

# MAGIC %md
# MAGIC solution1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT USER_ID, MAX(LOGIN_TIMESTAMP) AS LAST_LOGIN 
# MAGIC FROM logins_df
# MAGIC GROUP BY USER_ID
# MAGIC HAVING MAX(LOGIN_TIMESTAMP) < DATEADD(MONTH, -5, GETDATE())

# COMMAND ----------

# MAGIC %md
# MAGIC solution2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT USER_ID FROM logins_df
# MAGIC WHERE USER_ID NOT IN (
# MAGIC   SELECT USER_ID FROM logins_df
# MAGIC   WHERE LOGIN_TIMESTAMP > DATEADD(MONTH, -5, GETDATE())
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ####Q2: For the business units quarterly analysis, calculate how many users and how many sessions were at each quarter
# MAGIC ####order by quarter from newest and oldest
# MAGIC ####return first day of the quarter, user_cnt,session_cnt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, date_part('QUARTER', LOGIN_TIMESTAMP) AS QUARTER_NUMBER
# MAGIC FROM logins_df

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_trunc('QUARTER',MIN(LOGIN_TIMESTAMP)) AS FIRST_DATE_QUARTER, COUNT(DISTINCT USER_ID) AS USR_CNT, COUNT(*) AS SESSION_CNT
# MAGIC FROM logins_df
# MAGIC GROUP BY date_part('QUARTER', LOGIN_TIMESTAMP)
# MAGIC SORT BY date_part('QUARTER', LOGIN_TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Q3: Display user ids that log in in january 2024 and did not log in november 2023

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

# Define schema for logins DataFrame
logins_schema = StructType([
    StructField("USER_ID", IntegerType(), True),
    StructField("LOGIN_TIMESTAMP", TimestampType(), True),
    StructField("SESSION_ID", IntegerType(), True),
    StructField("SESSION_SCORE", IntegerType(), True)
])

# Create data for logins DataFrame
logins_data = [
    (1, datetime.strptime('2023-07-15 09:30:00', '%Y-%m-%d %H:%M:%S'), 1001, 85),
    (2, datetime.strptime('2023-07-22 10:00:00', '%Y-%m-%d %H:%M:%S'), 1002, 90),
    (3, datetime.strptime('2023-08-10 11:15:00', '%Y-%m-%d %H:%M:%S'), 1003, 75),
    (4, datetime.strptime('2023-08-20 14:00:00', '%Y-%m-%d %H:%M:%S'), 1004, 88),
    (5, datetime.strptime('2023-09-05 16:45:00', '%Y-%m-%d %H:%M:%S'), 1005, 82),
    (6, datetime.strptime('2023-10-12 08:30:00', '%Y-%m-%d %H:%M:%S'), 1006, 77),
    (7, datetime.strptime('2023-11-18 09:00:00', '%Y-%m-%d %H:%M:%S'), 1007, 81),
    (8, datetime.strptime('2023-12-01 10:30:00', '%Y-%m-%d %H:%M:%S'), 1008, 84),
    (9, datetime.strptime('2023-12-15 13:15:00', '%Y-%m-%d %H:%M:%S'), 1009, 79),
    (1, datetime.strptime('2024-01-10 07:45:00', '%Y-%m-%d %H:%M:%S'), 1011, 86),
    (2, datetime.strptime('2024-01-25 09:30:00', '%Y-%m-%d %H:%M:%S'), 1012, 89),
    (3, datetime.strptime('2024-02-05 11:00:00', '%Y-%m-%d %H:%M:%S'), 1013, 78),
    (4, datetime.strptime('2024-03-01 14:30:00', '%Y-%m-%d %H:%M:%S'), 1014, 91),
    (5, datetime.strptime('2024-03-15 16:00:00', '%Y-%m-%d %H:%M:%S'), 1015, 83),
    (6, datetime.strptime('2024-04-12 08:00:00', '%Y-%m-%d %H:%M:%S'), 1016, 80),
    (7, datetime.strptime('2024-05-18 09:15:00', '%Y-%m-%d %H:%M:%S'), 1017, 82),
    (8, datetime.strptime('2024-05-28 10:45:00', '%Y-%m-%d %H:%M:%S'), 1018, 87),
    (9, datetime.strptime('2024-06-15 13:30:00', '%Y-%m-%d %H:%M:%S'), 1019, 76),
    (10, datetime.strptime('2024-06-25 15:00:00', '%Y-%m-%d %H:%M:%S'), 1010, 92),
    (10, datetime.strptime('2024-06-26 15:45:00', '%Y-%m-%d %H:%M:%S'), 1020, 93),
    (10, datetime.strptime('2024-06-27 15:00:00', '%Y-%m-%d %H:%M:%S'), 1021, 92),
    (10, datetime.strptime('2024-06-28 15:45:00', '%Y-%m-%d %H:%M:%S'), 1022, 93),
    (1, datetime.strptime('2024-01-10 07:45:00', '%Y-%m-%d %H:%M:%S'), 1101, 86),
    (3, datetime.strptime('2024-01-25 09:30:00', '%Y-%m-%d %H:%M:%S'), 1102, 89),
    (5, datetime.strptime('2024-01-15 11:00:00', '%Y-%m-%d %H:%M:%S'), 1103, 78),
    (2, datetime.strptime('2023-11-10 07:45:00', '%Y-%m-%d %H:%M:%S'), 1201, 82),
    (4, datetime.strptime('2023-11-25 09:30:00', '%Y-%m-%d %H:%M:%S'), 1202, 84),
    (6, datetime.strptime('2023-11-15 11:00:00', '%Y-%m-%d %H:%M:%S'), 1203, 80)
]

# Create logins DataFrame
logins_df = spark.createDataFrame(logins_data, schema=logins_schema)

# Display logins DataFrame
display(logins_df)
logins_df.createOrReplaceTempView("logins_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT USER_ID FROM logins_df
# MAGIC WHERE LOGIN_TIMESTAMP BETWEEN '2024-01-01' AND '2024-01-31'

# COMMAND ----------

#1,2,3,5
#2,7,4,6

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT USER_ID FROM logins_df
# MAGIC WHERE LOGIN_TIMESTAMP BETWEEN '2023-11-01' AND '2023-11-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT USER_ID FROM logins_df
# MAGIC WHERE LOGIN_TIMESTAMP BETWEEN '2024-01-01' AND '2024-01-31' AND 
# MAGIC USER_ID NOT IN (
# MAGIC   SELECT USER_ID FROM logins_df
# MAGIC   WHERE LOGIN_TIMESTAMP BETWEEN '2023-11-01' AND '2023-11-30'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Q4: Add to the query from the 2, percentage change in sessions from the last quarter
# MAGIC #### return first day of the quarter, session_cnt,session_cnt_prev, session_percenatge_change

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte(
# MAGIC SELECT date_trunc('QUARTER',MIN(LOGIN_TIMESTAMP)) AS FIRST_DATE_QUARTER, COUNT(DISTINCT USER_ID) AS USR_CNT, COUNT(*) AS SESSION_CNT
# MAGIC FROM logins_df
# MAGIC GROUP BY date_part('QUARTER', LOGIN_TIMESTAMP)
# MAGIC -- SORT BY FIRST_DATE_QUARTER
# MAGIC )
# MAGIC select *,
# MAGIC LAG(SESSION_CNT,1) OVER(ORDER BY FIRST_DATE_QUARTER) AS PREV_SESSION_CNT,
# MAGIC ((SESSION_CNT - (LAG(SESSION_CNT,1) OVER(ORDER BY FIRST_DATE_QUARTER)))/(LAG(SESSION_CNT,1) OVER(ORDER BY FIRST_DATE_QUARTER)))*100 as PERCENTAGE_CNG
# MAGIC FROM cte
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Q5:display the user that had the highest session score(max) for each day
# MAGIC ####return date,username,score

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte(
# MAGIC SELECT USER_ID, CAST(LOGIN_TIMESTAMP AS DATE) AS LOGIN_DATE, SUM(SESSION_SCORE) AS SCORE
# MAGIC FROM logins_df
# MAGIC GROUP BY USER_ID,LOGIN_DATE
# MAGIC ORDER BY LOGIN_DATE
# MAGIC )
# MAGIC SELECT * FROM (
# MAGIC SELECT *, ROW_NUMBER() OVER(PARTITION BY LOGIN_DATE ORDER BY SCORE DESC) AS ROW_NUM
# MAGIC FROM CTE
# MAGIC )
# MAGIC WHERE ROW_NUM == 1

# COMMAND ----------

# MAGIC %md
# MAGIC ####Q6: To identify our best user - return the users thathad a session on every single days since their fIrst login
# MAGIC ####make assumptions if needed
# MAGIC ####return user_id

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

# Define schema for logins DataFrame
logins_schema = StructType([
    StructField("USER_ID", IntegerType(), True),
    StructField("LOGIN_TIMESTAMP", TimestampType(), True),
    StructField("SESSION_ID", IntegerType(), True),
    StructField("SESSION_SCORE", IntegerType(), True)
])

# Create data for logins DataFrame
logins_data = [
    (1, datetime.strptime('2023-07-15 09:30:00', '%Y-%m-%d %H:%M:%S'), 1001, 85),
    (2, datetime.strptime('2023-07-22 10:00:00', '%Y-%m-%d %H:%M:%S'), 1002, 90),
    (3, datetime.strptime('2023-08-10 11:15:00', '%Y-%m-%d %H:%M:%S'), 1003, 75),
    (4, datetime.strptime('2023-08-20 14:00:00', '%Y-%m-%d %H:%M:%S'), 1004, 88),
    (5, datetime.strptime('2023-09-05 16:45:00', '%Y-%m-%d %H:%M:%S'), 1005, 82),
    (6, datetime.strptime('2023-10-12 08:30:00', '%Y-%m-%d %H:%M:%S'), 1006, 77),
    (7, datetime.strptime('2023-11-18 09:00:00', '%Y-%m-%d %H:%M:%S'), 1007, 81),
    (8, datetime.strptime('2023-12-01 10:30:00', '%Y-%m-%d %H:%M:%S'), 1008, 84),
    (9, datetime.strptime('2023-12-15 13:15:00', '%Y-%m-%d %H:%M:%S'), 1009, 79),
    (1, datetime.strptime('2025-01-10 07:45:00', '%Y-%m-%d %H:%M:%S'), 1011, 86),
    (2, datetime.strptime('2024-01-25 09:30:00', '%Y-%m-%d %H:%M:%S'), 1012, 89),
    (3, datetime.strptime('2024-02-05 11:00:00', '%Y-%m-%d %H:%M:%S'), 1013, 78),
    (4, datetime.strptime('2024-03-01 14:30:00', '%Y-%m-%d %H:%M:%S'), 1014, 91),
    (5, datetime.strptime('2024-03-15 16:00:00', '%Y-%m-%d %H:%M:%S'), 1015, 83),
    (6, datetime.strptime('2024-04-12 08:00:00', '%Y-%m-%d %H:%M:%S'), 1016, 80),
    (7, datetime.strptime('2024-05-18 09:15:00', '%Y-%m-%d %H:%M:%S'), 1017, 82),
    (8, datetime.strptime('2024-05-28 10:45:00', '%Y-%m-%d %H:%M:%S'), 1018, 87),
    (9, datetime.strptime('2024-12-15 13:30:00', '%Y-%m-%d %H:%M:%S'), 1019, 76),
    (10, datetime.strptime('2025-02-26 15:00:00', '%Y-%m-%d %H:%M:%S'), 1010, 92),
    (10, datetime.strptime('2025-02-27 15:45:00', '%Y-%m-%d %H:%M:%S'), 1020, 93),
    (10, datetime.strptime('2025-02-28 15:00:00', '%Y-%m-%d %H:%M:%S'), 1021, 92),
    (10, datetime.strptime('2025-03-01 15:45:00', '%Y-%m-%d %H:%M:%S'), 1022, 93),
    (1, datetime.strptime('2024-01-10 07:45:00', '%Y-%m-%d %H:%M:%S'), 1101, 86),
    (3, datetime.strptime('2024-01-25 09:30:00', '%Y-%m-%d %H:%M:%S'), 1102, 89),
    (5, datetime.strptime('2024-01-15 11:00:00', '%Y-%m-%d %H:%M:%S'), 1103, 78),
    (2, datetime.strptime('2024-11-10 07:45:00', '%Y-%m-%d %H:%M:%S'), 1201, 82),
    (4, datetime.strptime('2023-11-25 09:30:00', '%Y-%m-%d %H:%M:%S'), 1202, 84),
    (6, datetime.strptime('2024-11-15 11:00:00', '%Y-%m-%d %H:%M:%S'), 1203, 80)
]

# Create logins DataFrame
logins_df = spark.createDataFrame(logins_data, schema=logins_schema)

# Display logins DataFrame
display(logins_df)
logins_df.createOrReplaceTempView("logins_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT USER_ID
# MAGIC FROM logins_df
# MAGIC GROUP BY USER_ID
# MAGIC HAVING datediff(DAY, MIN(CAST(LOGIN_TIMESTAMP AS DATE)), GETDATE())+1==COUNT(DISTINCT CAST(LOGIN_TIMESTAMP AS DATE))
# MAGIC ORDER BY USER_ID

# COMMAND ----------

# MAGIC %md
# MAGIC ####Q7: What days there were no login atall

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH date_range AS (
# MAGIC     SELECT MIN(CAST(LOGIN_TIMESTAMP AS DATE)) AS FIRST_LOGIN_DATE, CAST(GETDATE() AS DATE) AS CURRENT_DATE
# MAGIC     FROM logins_df
# MAGIC ),
# MAGIC recursive_dates AS (
# MAGIC     SELECT FIRST_LOGIN_DATE AS calendar_date
# MAGIC     FROM date_range
# MAGIC     UNION ALL
# MAGIC     SELECT DATEADD(DAY, 1, calendar_date)
# MAGIC     FROM recursive_dates
# MAGIC     WHERE calendar_date < (SELECT CURRENT_DATE FROM date_range)
# MAGIC )
# MAGIC SELECT calendar_date FROM recursive_dates;
# MAGIC

# COMMAND ----------

