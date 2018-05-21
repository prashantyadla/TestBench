# TestBench
PySpark Application for Distributed Random Data Generation.
Types of Random data and configuration can be specified in "datagen_schema_config.json". Please refer to doc(link above) for more info about the same.

Dependencies :- 
1) HDP 			(should have been pre-installed in cluster)
2) PySpark v2.3		 (should have been pre-installed in cluster)
3) Python2.7.5		 (should have been pre-installed in cluster)
4) Xeger	(should be manually installed across all nodes)
5) Numpy	(should be manually installed across all nodes)


BootStrap and Onboarding :- 
1) After cloning the repo into the resource manager (yarn) node/process of your cluster, please install the above dependencies manually across all nodes of your cluster
2) If it’s a kerberized cluster, please copy HIVE user credentials into the repo using the following commands :-
cp /etc/security/keytabs/hive.service.keytab .
kinit -kt hive.service.keytab hive/ctr-e138-1518143905142-314233-01-000003.hwx.site@HWQE.HORTONWORKS.COM
3) Ensure appropriate Pyspark version
export SPARK_MAJOR_VERSION=2
4) Run the PySpark job “datagen_job.py”
spark-submit --master yarn --files datagen_schema_config.json datagen_job.py
5)Accessing location of data generated :- 
1)> beeline
2)> !connect <hive jdbc url>
3)show tables;
4)describe <HIVE_TABLE_NAME>;
