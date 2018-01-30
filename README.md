# JoinApacheSpark
Perform join operations between RDD, Dataframes, Datasets using Scala and Apache Spark

First the join is performed between Rdds that have been "cleared" and grouped by their keys.

Second the join is performed between Datasets

Third the join is performed between Dataframes

Fourth the join is performed using broadcast so that the smaller relation (rdd) gets into memory - Map Side Join

Fifth the join is performed as in fourth but now using function repartitionandsortwithinpartitions

Two sample files are given for test purposes . 


