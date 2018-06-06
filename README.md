# TestBench
PySpark Application for Distributed Random Data Generation.
Types of Random data and configuration can be specified in "datagen_schema_config.json". Please refer to doc(link above) for more info about the same.

Dependencies :- 
1) HDP 			(should have been pre-installed in cluster)
2) PySpark v2.3		 (should have been pre-installed in cluster)
3) Python2.7.5		 (should have been pre-installed in cluster)
4) exrex	(should be manually installed across all nodes)
5) Numpy	(should be manually installed across all nodes (in some HDP clusters it may not be there))


BootStrap and Onboarding :- 
1) After cloning the repo into the resource manager (yarn) node/process of your cluster, please install the above dependencies manually across all nodes of your cluster
2) If it’s a kerberized cluster, please ensure kinit and kerberos appropriate kerberos token is authorized
3) Ensure appropriate Pyspark version
export SPARK_MAJOR_VERSION=2
4) Run the PySpark job “datagen_job.py”
spark-submit --master yarn --files datagen_schema_config.json datagen_job.py
5)Accessing location of data generated :- \
  a)> beeline <br/>
  b)> !connect <hive jdbc url> <br/>
  c)show tables; <br/>
  d)describe <HIVE_TABLE_NAME>;
