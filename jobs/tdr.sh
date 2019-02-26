
INIT="source /opt/client/bigdata_env &&  kinit -kt /opt/client/Spark/scripts/moveToParquet/auth/user.keytab sparkuser"
COMMAND="jps -lm | grep -i tdr-schema.xml"

eval $INIT
OUTPUT=$(eval "$COMMAND")
EXITCODE=$?

if [ -z "$OUTPUT" ]
then
      eval $INIT
      sparkRun="spark-submit 
      --master yarn 
      --deploy-mode cluster 
      --driver-memory 16g
      --supervise	
      --executor-memory 10g
      --num-executors 12
      --executor-cores 6
      --files /opt/client/Spark/scripts/moveToParquet/auth/user.keytab 
      --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties' 
      --conf spark.driver.extraJavaOptions='-Dlog4j.configuration=./log4.properties' 
      --conf spark.network.timeout=20000s 
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer 
      /opt/client/Spark/scripts/moveToParquet/load-to-hive-parq-assembly-1.0.jar hdfs:///spark-scripts-config/load-to-hive/tdr-schema.xml convert-tdr
      " 
      eval $sparkRun
fi





