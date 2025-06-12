# DATA INTEGRATOR TOOL


## Project Structure


| Folder  | Type  | Definition |
| :- | :-| :-|
| bin | system | Executables |
| conf | user-defined | Configurations |
| jars | user-defined | JAR Files |
| maps | user-defined | Mapping Files |
| obj | system | System Classes |
| python | system | Python Scripts |
| spark3 | system | Spark Scripts |
| tests | system | Unit Tests |
| upload | user-defined | Data Files |


## bin

Executables

## conf

Configurations

| YAML File    | Definition|
|---------|------------|
| catalog.yml | For Catalog Definitions on Iceberg |
| jdbc.yml | For JDBC Connections to DBs |
| spark.yml | For Spark Context |

## jars

Drivers required for the project according to dependency (Spark version, Java version, etc.)

## maps

User defined mapping files to implement the data flow

## obj

Classes to define project components

## python

Python scripts that operate the processes

## spark3

Python scripts to generate the Spark Contexts

## tests

Unit Test files
