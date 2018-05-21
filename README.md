# TestBench
PySpark Application for Distributed Random Data Generation.
Types of Random data and configuration can be specified in "datagen_schema_config.json"

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
> beeline
> !connect <hive jdbc url> jdbc:hive2://ctr-e138-1518143905142-314233-01-000006.hwx.site:2181,ctr-e138-1518143905142-314233-01-000004.hwx.site:2181,ctr-e138-1518143905142-314233-01-000005.hwx.site:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
show tables;
describe <HIVE_TABLE_NAME>;
