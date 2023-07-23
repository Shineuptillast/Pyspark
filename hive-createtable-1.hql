
Create external table hive_emr_1(
Id int,
Name string,
Age int,
Status string,
Skills Array<string>
)
row format delimited
fields terminated by ','
collection items terminated by ','
stored as textfile
location "s3://emrbucketforpractice/inputs/hive_1_data.csv"
