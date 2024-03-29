# TPC-H PySpark

TPC-H benchmark (3.0.0) implemented in PySpark (Spark 3.2.0 with in-built Hadoop 2.7) using the DataFrames API.


## Generating tables

Under the dbgen directory do:
```
make
```

This should generate an executable called `dbgen`
```
./dbgen -h
```
gives you the various options for generating the tables. 

To generate tables, the simplest case is running:
```
./dbgen
```
which generates tables with extension `.tbl` with scale 1 (default) for a total of rougly 1GB size across all tables. For different size tables you can use the `-s` option:
```
./dbgen -s 10
```
will generate roughly 10GB of input data.

You can then either upload your data to hdfs or read them locally.


## Running

The following command will run query 1 of TPC-H benchmark on Spark. 
```bash
python tpch_perf.py -q <query-id, e.g., q1, q2> -d <data-path> -r <results-path>
```

## Queries and Answers

All the reference queries (q1-q22) are provided in the *queries* folder, and the associate answers on the dataset with scale 1 (~1GB across all tables) are listed in the *answers* folder.