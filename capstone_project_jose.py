import requests as re
import json
import private_info
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType

spark = SparkSession.builder.getOrCreate()

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

df_branch_new.show(3), df_credit_new.show(3), df_customer_new.show(3)