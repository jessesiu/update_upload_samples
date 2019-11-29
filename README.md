# Update and Upload Sample in GigaDB
Upload the dataset samples and sample_attributes to GigaDB database via the excel spreadsheets

## Requirements
JDK 1.7 and import all jars in the lib directory

### Step1
Setting the parameters for database connection, dataset doi and excel spreadsheets in configuration/setting.xml

### Step2
Run the SQL query file to delete sample and sample_attributes associated with the dataset. N.B. Replace the DOI number in '100XXX'

### Step3
Run the java file src/sample_update/sample.java to insert the sample and sample_attributes into database
