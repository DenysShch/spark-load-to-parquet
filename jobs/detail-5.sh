
INIT="source /opt/client/bigdata_env &&  kinit -kt /opt/client/Spark/scripts/moveToParquet/auth/user.keytab sparkuser"
COMMAND="jps -lm | grep -i detail-schema-part-5.xml"

eval $INIT
OUTPUT=$(eval "$COMMAND")
EXITCODE=$?

if [ -z "$OUTPUT" ]
then
      eval $INIT
      sparkRun="spark-submit 
      --master yarn 
      --deploy-mode cluster 
      --driver-memory 12g 
      --supervise	
      --executor-memory 12g 
      --num-executors 16 
      --executor-cores 6
      --files /opt/client/Spark/scripts/moveToParquet/auth/user.keytab 
      --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties' 
      --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=./log4.properties' 
      --conf 'spark.network.timeout=200000s'
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
      /opt/client/Spark/scripts/moveToParquet/load-to-hive-parq-assembly-1.0.jar hdfs:///spark-scripts-config/load-to-hive/detail-schema-part-5.xml convert-detail-5
      " 
      eval $sparkRun
fi





