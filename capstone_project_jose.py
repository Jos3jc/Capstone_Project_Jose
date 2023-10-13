import requests as re
import json
import private_info
import mysql.connector as dbconnect
from mysql.connector import Error
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType

spark = SparkSession.builder.getOrCreate()

# CREDIT CARD DATA SET 

def clean_dataset(file_name):
    
    if file_name == 'cdw_sapp_branch.json':
        df_func1 = spark.read.json(file_name)
        df_func1.createTempView("df_branch")
        df_func1_clean = spark.sql(" SELECT  BRANCH_CODE,\
                                            BRANCH_NAME,\
                                            BRANCH_STREET,\
                                            BRANCH_CITY,\
                                            BRANCH_STATE,\
                                            IF(BRANCH_ZIP IS NULL, 99999, BRANCH_ZIP) AS BRANCH_ZIP,\
                                            CONCAT( '(', SUBSTR(BRANCH_PHONE,1,3),\
                                                    ')', SUBSTR(BRANCH_PHONE,4,3),\
                                                    '-', SUBSTR(BRANCH_PHONE,7) ) as BRANCH_PHONE,\
                                            LAST_UPDATED\
                                    FROM    df_branch")
        return df_func1_clean
    
    if file_name == 'cdw_sapp_credit.json':
        df_func2 = spark.read.json(file_name)
        df_func2.createTempView("df_credit")
        df_func2_clean = spark.sql(" SELECT  CREDIT_CARD_NO AS CUST_CC_NO,\
                                            CONCAT(YEAR, IF(LEN(MONTH)=1,CONCAT('0',MONTH),MONTH), IF(LEN(DAY)=1,CONCAT('0',DAY),DAY)) AS TIMEID,\
                                            CUST_SSN,\
                                            BRANCH_CODE,\
                                            TRANSACTION_TYPE,\
                                            TRANSACTION_VALUE,\
                                            TRANSACTION_ID\
                                    FROM    df_credit")
        return df_func2_clean

    if file_name == 'cdw_sapp_custmer.json':
        df_func3 = spark.read.json(file_name)
        df_func3.createTempView("df_customer")
        df_func3_clean = spark.sql("SELECT SSN,\
                                            CONCAT(UPPER(LEFT(FIRST_NAME,1)), RIGHT(FIRST_NAME,LEN(FIRST_NAME)-1)) AS FIRST_NAME,\
                                            LOWER(MIDDLE_NAME) AS MIDDLE_NAME,\
                                            CONCAT(UPPER(LEFT(LAST_NAME,1)), RIGHT(LAST_NAME,LEN(LAST_NAME)-1)) AS LAST_NAME,\
                                            CREDIT_CARD_NO,\
                                            CONCAT(STREET_NAME, ', ', APT_NO) AS FULL_STREET_ADDRESS,\
                                            CUST_CITY,\
                                            CUST_STATE,\
                                            CUST_COUNTRY,\
                                            CUST_ZIP,\
                                            CONCAT( '(', SUBSTR(CUST_PHONE,1,3),\
                                                    ')', SUBSTR(CUST_PHONE,4,3),\
                                                    '-', SUBSTR(CUST_PHONE,7) ) as CUST_PHONE,\
                                            CUST_EMAIL,\
                                            LAST_UPDATED\
                                     FROM   df_customer")
        return df_func3_clean

df_branch_new = clean_dataset('cdw_sapp_branch.json')
df_credit_new = clean_dataset('cdw_sapp_credit.json')
df_customer_new = clean_dataset('cdw_sapp_custmer.json')


def create_database(db_name):
        try:
                connect = dbconnect.connect(host="localhost", user=private_info.user, password=private_info.password)
                if connect.is_connected():
                        print(f'Successfully Connected to MySQL database')
                        cursor = connect.cursor()
                        cursor.execute(f"CREATE DATABASE {db_name}")
                        print(f"Database '{db_name}' created")
                        cursor.execute(f"USE {db_name};") 

        except Error as e:
                print("Error while connect to Database:", e)
        finally:
                if connect.is_connected():
                        cursor.close()
                        connect.close()
                print("Database connect is closed")

create_database('creditcard_capstone')


db_name = 'creditcard_capstone'
tables_names = {'CDW_SAPP_BRANCH':df_branch_new,\
                'CDW_SAPP_CREDIT_CARD':df_credit_new,\
                'CDW_SAPP_CUSTOMER':df_customer_new}
for k,v in tables_names.items():
    v.write.format("jdbc") \
    .mode("append") \
    .option("url", f"jdbc:mysql://localhost:3306/{db_name}") \
    .option("dbtable", k) \
    .option("user", private_info.user) \
    .option("password", private_info.password) \
    .save()

def modify_table_type():
        try:
                connect_u_t = dbconnect.connect(database=db_name, user=private_info.user, password=private_info.password)
                if connect_u_t.is_connected():
                        print(f'Coneceted to: {db_name}')

                        cursor = connect_u_t.cursor()   
                        cursor.execute("ALTER TABLE CDW_SAPP_BRANCH \
                                        MODIFY COLUMN BRANCH_CODE INTEGER,\
                                        MODIFY COLUMN BRANCH_NAME VARCHAR(50),\
                                        MODIFY COLUMN BRANCH_STREET VARCHAR(50),\
                                        MODIFY COLUMN BRANCH_CITY VARCHAR(50),\
                                        MODIFY COLUMN BRANCH_STATE VARCHAR(50),\
                                        MODIFY COLUMN BRANCH_ZIP INTEGER,\
                                        MODIFY COLUMN BRANCH_PHONE VARCHAR(50),\
                                        MODIFY COLUMN LAST_UPDATED TIMESTAMP")

                        cursor.execute("ALTER TABLE CDW_SAPP_CREDIT_CARD \
                                        MODIFY COLUMN CUST_CC_NO VARCHAR(50),\
                                        MODIFY COLUMN TIMEID VARCHAR(50),\
                                        MODIFY COLUMN CUST_SSN INTEGER,\
                                        MODIFY COLUMN BRANCH_CODE INTEGER,\
                                        MODIFY COLUMN TRANSACTION_TYPE VARCHAR(50),\
                                        MODIFY COLUMN TRANSACTION_VALUE DOUBLE,\
                                        MODIFY COLUMN TRANSACTION_ID INTEGER")  
                        
                        cursor.execute("ALTER TABLE CDW_SAPP_CUSTOMER \
                                        MODIFY COLUMN SSN INTEGER,\
                                        MODIFY COLUMN FIRST_NAME VARCHAR(50),\
                                        MODIFY COLUMN MIDDLE_NAME VARCHAR(50),\
                                        MODIFY COLUMN LAST_NAME VARCHAR(50),\
                                        MODIFY COLUMN CREDIT_CARD_NO VARCHAR(50),\
                                        MODIFY COLUMN FULL_STREET_ADDRESS VARCHAR(50),\
                                        MODIFY COLUMN CUST_CITY VARCHAR(50),\
                                        MODIFY COLUMN CUST_STATE VARCHAR(50),\
                                        MODIFY COLUMN CUST_COUNTRY VARCHAR(50),\
                                        MODIFY COLUMN CUST_ZIP INTEGER,\
                                        MODIFY COLUMN CUST_PHONE VARCHAR(50),\
                                        MODIFY COLUMN CUST_EMAIL VARCHAR(50),\
                                        MODIFY COLUMN LAST_UPDATED TIMESTAMP,\
                                        ADD PRIMARY KEY (SSN)")
                        connect_u_t.commit()

        except Error as e:
                print("Error while connect_u_t to Database:", e)
        finally:
                if connect_u_t.is_connected():
                        cursor.close()
                        connect_u_t.close()
                print("Database connect_u_t is closed")

modify_table_type()

# LOAN APPLICATION DATA API

url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
loan_get = re.get(url)
print (str(loan_get.status_code))

loan_json = loan_get.json()
with open('cdw_sapp_loan_application.json', 'w') as f:
    json.dump(loan_json, f)

df_loan = spark.read.json('cdw_sapp_loan_application.json')
df_loan.write.format("jdbc") \
.mode("append") \
.option("url", f"jdbc:mysql://localhost:3306/{db_name}") \
.option("dbtable", 'CDW_SAPP_LOAN_APPLICATION') \
.option("user", private_info.user) \
.option("password", private_info.password) \
.save()