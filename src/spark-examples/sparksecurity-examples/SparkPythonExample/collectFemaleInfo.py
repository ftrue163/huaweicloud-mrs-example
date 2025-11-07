# -*- coding: utf-8 -*

import sys

# from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def contains(str, substr):
	if substr in str:
		return True
	return False

if __name__ == "__main__":
	# if len(sys.argv) < 2:
	# 	print("Usage: CollectFemaleInfo <file>")
	# 	exit(-1)

	# Create SparkContext and set AppName.
	spark = SparkSession \
		.builder \
		.appName("CollectFemaleInfo") \
		.getOrCreate()


	"""
	The following programs are used to implement the following functions:
	1. Read data. This code indicates the data path that the input parameter argv[1] specifies. - text
	2. Filter data about the time that female netizens spend online. - filter
	3. Aggregate the total time that each female netizen spends online. - map/map/reduceByKey
	4. Filter information about female netizens who spend more than 2 hours online. - filter
	"""
	inputPath = sys.argv[1]
	result = spark.read.text(inputPath).rdd.map(lambda r: r[0]) \
		.filter(lambda line: contains(line, "female")) \
		.map(lambda line: line.split(',')) \
		.map(lambda dataArr: (dataArr[0], int(dataArr[2]))) \
		.reduceByKey(lambda v1, v2: v1 + v2) \
		.filter(lambda tupleVal: tupleVal[1] > 120) \
		.collect()
	for (k, v) in result:
		print(k + "," + str(v))

	# Stop SparkContext
	spark.stop()


'''
-- 参数
/opt/data_projects/spark_data/data
/tmp/spark/input/
-- 环境变量
export JAVA_HOME=/opt/client/JDK/jdk1.8.0_372/bin/java
HADOOP_CONF_DIR=/opt/client/HDFS/hadoop/etc

export PYSPARK_PYTHON=/opt/app/anaconda3/envs/pyspark38/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/app/anaconda3/envs/pyspark38/bin/python



-- 提交方式
-- 1.本地方式
spark-submit \
--master local[1] \
/tmp/pycharm_project_376/collectFemaleInfo.py \
/tmp/spark/input/

-- 2.提交到yarn
spark-submit \
--master yarn \
--deploy-mode cluster \
--name pyspark_test \
--driver-memory 1G \
--executor-memory 1G \
--num-executors 1 \
--executor-cores 1 \
/tmp/pycharm_project_376/collectFemaleInfo.py \
/tmp/spark/input/

--archives hdfs://hacluster/tmp/spark/myenv.tar.gz#pyspark38 \
--conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./pyspark38/bin/python" \
--conf "spark.executorEnv.PYSPARK_PYTHON=./pyspark38/bin/python" \

--conf "spark.pyspark.python=./pyspark38/bin/python" \
--conf "spark.pyspark.driver.python=./pyspark38/bin/python" \
--conf "spark.pyspark.driver.python=/opt/app/anaconda3/envs/pyspark38/bin/python" \

spark-submit \
--master yarn \
--deploy-mode client \
--name pyspark_test \
--driver-memory 1G \
--executor-memory 1G \
--num-executors 1 \
--executor-cores 1 \
/tmp/pycharm_project_376/collectFemaleInfo.py \
/tmp/spark/input/


spark-submit \
--master yarn \
--deploy-mode client \
--name pyspark_test \
--driver-memory 1G \
--executor-memory 1G \
--num-executors 1 \
--executor-cores 1 \
--conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/app/anaconda3/envs/pyspark38/bin/python" \
--conf "spark.executorEnv.PYSPARK_PYTHON=/opt/app/anaconda3/envs/pyspark38/bin/python" \
/tmp/pycharm_project_376/collectFemaleInfo.py \
/tmp/spark/input/



-- cluster模式可以运行成功
spark-submit \
--master yarn \
--deploy-mode cluster \
--archives hdfs://hacluster/tmp/spark/myenv.tar.gz#pyspark_env \
--conf spark.pyspark.python=./pyspark_env/bin/python \
--conf spark.pyspark.driver.python=./pyspark_env/bin/python \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./pyspark_env/bin/python \
--conf spark.executorEnv.PYSPARK_PYTHON=./pyspark_env/bin/python \
--conf spark.executorEnv.PROJ_LIB=./pyspark_env/share/proj \
--conf spark.yarn.appMasterEnv.PROJ_LIB=./pyspark_env/share/proj \
/tmp/pycharm_project_376/collectFemaleInfo.py \
/tmp/spark/input/


-- client模式
spark-submit \
--master yarn \
--deploy-mode client \
--archives hdfs://hacluster/tmp/spark/myenv.tar.gz#pyspark_env \
--conf spark.pyspark.python=./pyspark_env/bin/python \
--conf spark.pyspark.driver.python=/opt/app/anaconda3/envs/pyspark38/bin/python \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./pyspark_env/bin/python \
--conf spark.executorEnv.PYSPARK_PYTHON=./pyspark_env/bin/python \
--conf spark.executorEnv.PROJ_LIB=./pyspark_env/share/proj \
--conf spark.yarn.appMasterEnv.PROJ_LIB=./pyspark_env/share/proj \
/tmp/pycharm_project_376/collectFemaleInfo.py \
/tmp/spark/input/

-- client模式
spark-submit \
--master yarn \
--deploy-mode cluster \
--archives hdfs://hacluster/tmp/spark/myenv.tar.gz#pyspark_env \
--conf spark.pyspark.python=./pyspark_env/bin/python \
--conf spark.pyspark.driver.python=/opt/app/anaconda3/envs/pyspark38/bin/python \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./pyspark_env/bin/python \
--conf spark.executorEnv.PYSPARK_PYTHON=./pyspark_env/bin/python \
--conf spark.executorEnv.PROJ_LIB=./pyspark_env/share/proj \
--conf spark.yarn.appMasterEnv.PROJ_LIB=./pyspark_env/share/proj \
/tmp/pycharm_project_376/collectFemaleInfo.py \
/tmp/spark/input/
'''

