
INIT="source /opt/client/bigdata_env &&  kinit -kt /home/xdrloader/auth/user.keytab xdrloader"
COMMAND="jps -lm | grep -i detail-schema-part-3.xml"

eval $INIT
OUTPUT=$(eval "$COMMAND")
EXITCODE=$?

if [ -z "$OUTPUT" ]
then
      eval $INIT
      sparkRun="spark-submit 
      --master yarn 
      --deploy-mode cluster 
      --driver-memory 10g 
      --supervise	
      --executor-memory 6g 
      --num-executors 12
      --executor-cores 6 
      --files /home/xdrloader/auth/user.keytab 
      --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties' 
      --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=./log4.properties' 
      --conf 'spark.network.timeout=60000s' 
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' 
      /home/xdrloader/projects/moveToParquet/load-to-hive-parq-assembly-1.0.jar hdfs:///spark-scripts-config/load-to-hive/detail-schema-part-3.xml convert-to-parquet-DETAIL-3
      " 
      eval $sparkRun
fi





