# Sparify Onprem to Cloud ETL Transition

 My client Sparkify has grown their user base and song database capcity enough to move into a dynamic cloud platform.
 Currently their data sits in a Amazons S3 bucket and needs a pipline into a Amazon Redshift Data Warehouse to feed into a BI Application of the Organizations choice.
 We have decided to build a Dimensional Model for Analytical processing. 
  
 # ***Installation***

 #### Amazon Web Services Account
 In Order to build the necessary ETL pipline from Amazon S3 - Amazon Redshift you will need a few resources created in the AWS Cloud. Here is what you need:
    Click [here](https://aws.amazon.com/) to visit AWS Console.

#### _Create IAM User credentials_
* Navigate to the IAM Dashboard.
* Create a role with policy ***AmazonS3ReadOnlyAccess***
* Create a IAM user with ***AmazonRedshiftFullAccess***
  and ***AmazonS3ReadOnlyAccess*** policies assigned to it.
* Record AWS Access and Secret Key in `project_dwh.cfg` file. 

#### _S3 Bucket Source Data_
*  Prepopulated variables in project_dwh.cfg to public S3 source bucket and datasets.
* Song data: ***`s3://udacity-dend/song_data`*** Sample Below:
 ```
     {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
 ```
* Log data: ***`s3://udacity-dend/log_data`*** Snapshot of Data below:
    ![Alt Text](log_data.png)
* Metadata: ***`s3://udacity-dend/log_json_path.json`*** Snapshot of Data below:
    ![Alt Text](log_json.png)

#### _Create Amazon Redshift Cluster with 8 Nodes_
* Navigate to [Amazon Redshift Console](https://console.aws.amazon.com/redshift/) 
* Create an 8 node Cluster.
* Associate IAM role you created with S3 Read Access to this Cluster.

### Python 
* Install the latest version of [Python](https://www.python.org/)
#### Dependencies to Install with ***pip***
* Open command prompt and run:
  ***`pip install psycopg2, configparser`***
* psycopg2

### Clone GitHub Respoistory
Run this command to Clone the necessary files into your own GiHub Repositry:
`git clone https://github.com/jm2826/jm2826-UDacity-Data-Engineering-with-AWS.git` 

### File Description:
`create_tables.py`
Create_tables.py is the first script. This file will connect to the
Amazon Redshift database and call the functions and methods that will:
1. DROP existing tables in the Database
2. CREATE the tables to store the source data from Amazon S3
3. CREATE Fact and Dimension Tables of Star Schema built in Database.

`etl.py`
etl.py is the second script to run. This file will connect to the
Amazon Redshift database and create the Extract Transform and Load Pipeline that will:
1. Call the Dictionaries from the sql.queries Module that are used as arguments in the script functions.
2. COPY data from S3 source that are in 3NF.
3. INSERT Copied S3 DATA into the Redshift Staging Tables
4. INSERT data into Fact and Dimension tables.

## License
The content of this repository is licensed under a
[Creative Commons Attribution License](http://creativecommons.org/licenses/by/3.0/us/)
