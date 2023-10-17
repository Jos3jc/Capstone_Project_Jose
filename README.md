Project Overview

This project consists of Extraction and Transformation of two datasets named “Credit Card dataset” and 
“Loan Application dataset” which later will be Loaded into a database from where we can perform queries 
to display data and visualizations through the interaction with a Command Line Interface (CLI) Programming
in Python. The Credit Card dataset is divided into three JSON files: CDW_SAPP_CUSTMER.json, CDW_SAPP_CREDITCARD.json,
and CDW_SAPP_BRANCH.json. These contain information about customer’s account details, credit card transaction
information, and branch’s details. The Loan Application dataset is stored in a REST API in JSON format that contains
information about all the requested applications and their status.


CLI Functionality

All the code is saved in one python file named “capstone_project_jose.py”. Once it is executed, the user will have four options to type.
  	By typing ‘query’, it will redirect you to another interface where you can perform default queries and see the data. The data will be displayed and not saved.
  	By typing ‘credit’, it will display visualizations required by the business analyst team from Credit Card Tables. The plots will be saved automatically in the folder ‘plot_folder’.
  	By typing ‘loan’, it will display visualizations required by the business analyst team from the Loan Application Table. The plots will be saved automatically in the folder ‘plot_folder’.
  	By typing ‘exit’, it will end the CLI.


Challenges Faced

  	To read and transform the 3 Credit Card datasets according to the specifications found in the mapping.png files, I used pyspark sql and ‘mysql.connector’ to modify the type of the attributes.
  	To start the project, I started and created a session using the libraries pyspark and findspark.
  	To create the Data Base named ‘cerditcard_capstone’, I used ‘mysql.connector’.
  	To connect to MySQL bench, I created an external hidden python file that contains the username and user password.
  	To extract the data from the API, I used the libraries requests and json.
  	To load the clean data to the Database, I used the “.write.format(‘jdbc’)” dataframe method.
  	To play with the data stored in the database, I created temporary views.
  	The front-end code is structured by two mains “While” loops.
  	To update data, I used ‘mysql.connector’
  	To get the “x” and “y” parameters for each visualization, I sub-created spark data frames and pandas data frames to remove rows and merge data frames.
  	To display all the visualizations, I used standard matplotlib plots and artist layer.
  	To get the current directory path, I used the os.getcwd() which would later help me save the generated plots.

Libraries Used

All the libraries needed are listed in the requirements.txt file, and it can be installed by executing the command “pip install -r requirements.txt” on the terminal.
