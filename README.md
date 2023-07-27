# Sparify Onprem to Cloud ETL Transition

 My client Sparkify has grown their user base and song database capcity enough to move into a dynamic cloud platform.
 Currently their data sits in a Amazons S3 bucket and needs a pipline into a Amazon Redshift Data Warehouse to feed into a BI Application of the Organizations choice.
 We have decided to build a Dimensional Model for Analytical processing. 
  
 ## Installation 


 ### Amazon Web Services Account
 In Order to build the necessary ETL pipline from Amazon S3 - Amazon Redshift you will need a few resources created in the AWS Cloud. Here is what you need:
    Click [here](https://aws.amazon.com/) to visit AWS Console.

* IAM User credentials and also assign a Role for Amazon Redshift to have S3 Read Only Access
* S3 Bucket
* Amazon Redshift Cluster with 8 Nodes

### Python 
#### Dependencies to Install
* configparser
* psycopg2

### Clone GitHub Respoistory
`git clone https://github.com/jm2826/jm2826-UDacity-Data-Engineering-with-AWS.git` 

### Run Files in this Order
1. create_tables.py
2. etl.py

## License

The content of this repository is licensed under a
[Creative Commons Attribution License](http://creativecommons.org/licenses/by/3.0/us/)
