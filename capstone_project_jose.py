import os
import requests as re
import json
import private_info
import mysql.connector as dbconnect
from mysql.connector import Error
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
import matplotlib as mpl
import matplotlib.pyplot as plt

spark = SparkSession.builder.getOrCreate()

# CREDIT CARD DATA SET 
## TRANSFORMING CREDIT CARD DATA

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

## LOADING CREDIT CARD DATA
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

# FRONT-END
## CREATING DATAFRAMES AND TEMPORARY VIEWS

df_branch_SQL=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                    user=private_info.user,\
                                    password=private_info.password,\
                                    url=f"jdbc:mysql://localhost:3306/{db_name}",\
                                    dbtable="CDW_SAPP_BRANCH").load()

df_credit_SQL=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                    user=private_info.user,\
                                    password=private_info.password,\
                                    url=f"jdbc:mysql://localhost:3306/{db_name}",\
                                    dbtable="CDW_SAPP_CREDIT_CARD").load()

df_customer_SQL=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                    user=private_info.user,\
                                    password=private_info.password,\
                                    url=f"jdbc:mysql://localhost:3306/{db_name}",\
                                    dbtable="CDW_SAPP_CUSTOMER").load()

df_loan_SQL=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                    user=private_info.user,\
                                    password=private_info.password,\
                                    url=f"jdbc:mysql://localhost:3306/{db_name}",\
                                    dbtable="CDW_SAPP_LOAN_APPLICATION").load()



df_branch_SQL.createTempView("CDW_SAPP_BRANCH")
df_credit_SQL.createTempView("CDW_SAPP_CREDIT_CARD")
df_customer_SQL.createTempView("CDW_SAPP_CUSTOMER")
df_loan_SQL.createTempView("CDW_SAPP_LOAN_APPLICATION")

## CREATING FRONT-END INTERFACE

end_project = ''
while end_project != 'exit':
        print('MAIN MENU')
        print("To interact and query data from the database, type: 'query'")
        print("To see the Data Analysis and Visualizations from the CREDIT CARD SYSTEM DATA, type: 'credit'")
        print("To see the Data Analysis and Visualizations from the LOAN APPLICATION DATA API, type: 'loan'")
        print("To exit the program, type: 'exit'")

        main_option = input("type your next action: ")

        if main_option == 'query':

                action = ''

                while action != 'back':
                        print("These are all the options that you can execute.")
                        print("(1) Option #1: To display the transactions made by customers living in a given zip code for a given month and year ordered by day in descending order.")
                        print("(2) Option #2: To display the number and total values of transactions for a given type.")
                        print("(3) Option #3: To display the total number and total values of transactions for branches in a given state.")
                        print("(4) Option #4: To check the existing account details of a customer.")
                        print("(5) Option #5: To modify the existing account details of a customer.")
                        print("(6) Option #6: To generate a monthly bill for a credit card number for a given month and year.")
                        print("(7) Option #7: To display the transactions made by a customer between two dates ordered by year, month, and day in descending order.")
                        print("(back) type 'back' to go back to the main menu")

                        option = input("Type the option to be executed")
                        option_triggers = { 'opt1':['Type the zip code: ', 'Type the year (YYYY): , 4 digits are required: ', 'Type the month (MM): , 2 digits are required: ']
                                        ,'opt2':['Type the transaction type: ']
                                        ,'opt3':['Type the US state abbreviation: ']
                                        ,'opt4':['Type the credit card number: ']
                                        ,'opt5':['Type the SSN: ', 'Type the new first name: ', 'Type the new middle name: ', 'Type the new last name: ', 'Type the new Address: ', 'Type the new City: ', 'Type the new State: ', 'Type the new Country: ', 'Type the new zip code: ', 'Type the new phone number: ', 'Type the new email: ']
                                        ,'opt6':['Type the credit card number: ', 'Type the year (YYYY): ', 'Type the month (MM): ']
                                        ,'opt7':['Type the credit card number: ', 'Type the first date (YYYYMMDD): ', 'Type the second date (YYYYMMDD): ']}
                        
                        if option == '1':
                                input_commands = []
                                for msg in option_triggers['opt1']:
                                        user_input =  input(msg)
                                        input_commands.append(user_input)
                                # 23223, 2018 07 OK
                                spark.sql(f"    SELECT cc.CUST_SSN, cc.CUST_CC_NO, cu.CUST_ZIP, cc.TIMEID, cc.TRANSACTION_VALUE\
                                                FROM CDW_SAPP_CREDIT_CARD cc\
                                                JOIN CDW_SAPP_CUSTOMER cu\
                                                        ON cc.CUST_SSN = cu.SSN\
                                                WHERE   cu.CUST_ZIP = '{input_commands[0]}' AND\
                                                        LEFT(cc.TIMEID,6) = '{input_commands[1]+input_commands[2]}'\
                                                ORDER BY TIMEID DESC").show()
                                                
                        elif option == '2':
                                input_commands = []
                                for msg in option_triggers['opt2']:
                                        user_input =  input(msg)
                                        input_commands.append(user_input)
                                #Bills OK
                                spark.sql(f"    SELECT TRANSACTION_TYPE, COUNT(TRANSACTION_TYPE) AS NUMBER_OF_TRANSACTIONS, ROUND(SUM(TRANSACTION_VALUE),2) AS TOTAL_VALUE_OF_TRANSACTIONS\
                                                FROM cdw_sapp_credit_card\
                                                GROUP BY TRANSACTION_TYPE\
                                                HAVING TRANSACTION_TYPE = '{input_commands[0]}'").show()
                                                
                        elif option == '3':
                                input_commands = []
                                for msg in option_triggers['opt3']:
                                        user_input =  input(msg)
                                        input_commands.append(user_input)
                                #WA OK
                                spark.sql(f"    SELECT br.BRANCH_CODE, COUNT(br.BRANCH_CODE) AS TOTAL_NUMBER_OF_TRANSACTIONS, ROUND(SUM(cc.TRANSACTION_VALUE),2) AS TOTAL_VALUE_OF_TRANSACTIONS\
                                                FROM CDW_SAPP_BRANCH br\
                                                LEFT JOIN CDW_SAPP_CREDIT_CARD cc\
                                                        ON br.BRANCH_CODE = cc.BRANCH_CODE\
                                                WHERE br.BRANCH_STATE = '{input_commands[0]}'\
                                                GROUP BY br.BRANCH_CODE").show()
                        elif option == '4':
                                input_commands = []
                                for msg in option_triggers['opt4']:
                                        user_input =  input(msg)
                                        input_commands.append(user_input)
                                # 123452373 OK
                                spark.sql(f"    SELECT *\
                                                FROM CDW_SAPP_CUSTOMER\
                                                WHERE SSN = '{input_commands[0]}'").show()
                                
                        elif option == '5':
                               pass
                                # # input_commands = []
                                # # for msg in option_triggers['opt5']:
                                # #         user_input =  input(msg)
                                # #         input_commands.append(user_input)

                                # try:
                                #         connect = dbconnect.connect(host="localhost", user=private_info.user, password=private_info.password)
                                #         if connect.is_connected():
                                #                 print(f'Successfully Connected to MySQL database')
                                #                 cursor = connect.cursor()
                                #                 cursor.execute(f"USE {db_name};") 

                                #                 cursor.execute(f"       UPDATE CDW_SAPP_CUSTOMER\
                                #                                         SET FIRST_NAME = 'fffff'\
                                #                                         WHERE SSN = 123451037 ")
                                                
                                # except Error as e:
                                #         print("Error while connect to Database:", e)
                                # finally:
                                #         if connect.is_connected():
                                #                 cursor.close()
                                #                 connect.close()
                                #         print("Database connect is closed")


                                # print('The account was updated successfully.')


                                
                                # # 123451037 SSN
                                # # LAST_UPDATED = NOW()\
                                #                         # MIDDLE_NAME = '{input_commands[2]}',\
                                #                         # LAST_NAME = '{input_commands[3]}',\
                                #                         # FULL_STREET_ADDRESS = '{input_commands[4]}',\
                                #                         # CUST_CITY = '{input_commands[5]}',\
                                #                         # CUST_STATE = '{input_commands[6]}',\
                                #                         # CUST_COUNTRY = '{input_commands[7]}',\
                                #                         # CUST_ZIP = '{input_commands[8]}',\
                                #                         # CUST_PHONE = '{input_commands[9]}',\
                                #                         # CUST_EMAIL = '{input_commands[10]}'\
                                # # spark.sql(f"    UPDATE CDW_SAPP_CUSTOMER\
                                # #                 SET FIRST_NAME = '{input_commands[1]}'\
                                # #                 WHERE SSN = '{input_commands[0]}' ").show()

                        elif option == '6':
                                input_commands = []
                                for msg in option_triggers['opt6']:
                                        user_input =  input(msg)
                                        input_commands.append(user_input)
                                #4210653385089392 2018 07 OK
                                spark.sql(f"    SELECT CUST_CC_NO, ROUND(SUM(TRANSACTION_VALUE),2) AS MONTHLY_BILL\
                                                FROM CDW_SAPP_CREDIT_CARD\
                                                WHERE   CUST_CC_NO = '{input_commands[0]}' AND\
                                                        LEFT(TIMEID,6) = '{input_commands[1]+input_commands[2]}'\
                                                GROUP BY CUST_CC_NO").show() 
                                
                        elif option == '7':
                                input_commands = []
                                for msg in option_triggers['opt7']:
                                        user_input =  input(msg)
                                        input_commands.append(user_input)
                                # 4210653385089392 201805 201808
                                spark.sql(f"    SELECT *\
                                                FROM CDW_SAPP_CREDIT_CARD\
                                                WHERE   CUST_CC_NO = '{input_commands[0]}' AND\
                                                        TIMEID BETWEEN '{input_commands[1]}' AND '{input_commands[2]}'\
                                                ORDER BY TIMEID DESC").show()
                                
                        elif option == 'back':
                                action = 'back'

                        elif option not in ['1', '2', '3', '4', '5', '6', '7', 'e']:
                                print('Please make sure your are typing the correct characters.')
                        
                        else:
                                None


        elif main_option == 'credit':

                print('Number of Transactions by Transaction type')
                df_31 = spark.sql("     SELECT TRANSACTION_TYPE, COUNT(TRANSACTION_TYPE) as COUNT\
                                        FROM CDW_SAPP_CREDIT_CARD\
                                        GROUP BY TRANSACTION_TYPE\
                                        ORDER BY COUNT DESC\
                                        ").toPandas()
                ax31 = df_31.plot.bar(  x = 'TRANSACTION_TYPE', 
                                        y ='COUNT',
                                        figsize = (13,5),
                                        legend = False,
                                        rot = 0,
                                        color=['seagreen' if i == 'Bills' else 'turquoise' for i in df_31['TRANSACTION_TYPE']])
                plt.grid(linestyle='--', linewidth=0.8)
                plt.title('Number of Transactions by Transaction type', size = 15, color= 'black', weight ='bold')
                plt.ylabel('Number of Transactions', size = 15, color= 'gray')
                plt.xlabel('Transaction Type', size = 15, color= 'gray')
                ax31.bar_label(ax31.containers[0]);
                plt.savefig(str(os.getcwd())[2:]+'\plot_folder\creditcard_1_Number_of_Transactions_by_Transaction_type.png')
                plt.show()


                print('Number of customers by States')
                df_32 = spark.sql("     SELECT CUST_STATE, COUNT(CUST_STATE) as COUNT\
                                        FROM CDW_SAPP_CUSTOMER\
                                        GROUP BY CUST_STATE\
                                        ORDER BY COUNT DESC\
                                        ").toPandas()
                ax32 = df_32.plot.bar(x = 'CUST_STATE', 
                                y ='COUNT',
                                figsize=(13,5),
                                legend = False,
                                rot = 0,
                                color= ['seagreen' if i == 'NY' else 'turquoise' for i in df_32['CUST_STATE']])
                plt.grid(linestyle='--', linewidth=0.8)
                plt.title('Number of customers by States', size = 15, color= 'black', weight ='bold')
                plt.ylabel('Number of Customers', size = 15, color= 'gray')
                plt.xlabel('US States', size = 15, color= 'gray')
                ax32.bar_label(ax32.containers[0], size=8);
                plt.savefig(str(os.getcwd())[2:]+'\plot_folder\creditcard_2_Number_of_customers_by_States.png')
                plt.show()

                print('Top_10_Customers_with_the_highest_transaction_amounts')
                df_33 = spark.sql("     SELECT CUST_SSN, ROUND(SUM(TRANSACTION_VALUE),2) AS HIGHEST_AMOUNT\
                                        FROM CDW_SAPP_CREDIT_CARD\
                                        GROUP BY CUST_SSN\
                                        ORDER BY HIGHEST_AMOUNT DESC\
                                        LIMIT 10").toPandas()
                ax33 = df_33.plot.bar(x = 'CUST_SSN', 
                                y ='HIGHEST_AMOUNT',
                                figsize = (13,5),
                                legend = False,
                                rot = 0,
                                color=['seagreen' if i == 123451125 else 'turquoise' for i in df_33['CUST_SSN']])
                plt.grid(linestyle='--', linewidth=0.8)
                plt.title('Top 10 Customers with the highest transaction amounts ', size = 15, color= 'black', weight ='bold')
                plt.ylabel('Total transaction amount made it by customer', size = 13, color= 'gray')
                plt.xlabel('SSN of Customers', size = 15, color= 'gray')
                ax33.bar_label(ax33.containers[0]);
                plt.savefig(str(os.getcwd())[2:]+'\plot_folder\creditcard_3_Top_10_Customers.png')
                plt.show()

        elif main_option == 'loan':
                pass

        elif main_option == 'exit':
                end_project = 'exit'

        else:
                print('Please make sure your are typing the correct characters')