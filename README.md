# impala-get-json-object-udf
A UDF for Cloudera Impala ( hive get_json_object equivalent )

## build
- cmake .
- make

## install
- upload build/jsonUdf.ll to [hdfs path]/jsonUdf.ll
- impala-shell> create function [database.]json_get_object (string, string) returns string location '[hdfs path]/jsonUdf.ll' symbol='JsonGetObject';

## usage
- impala-shell> select json_get_object('{"name":"steven"}', '$.name');
- --> returns a string steven
