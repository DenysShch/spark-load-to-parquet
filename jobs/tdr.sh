
INIT="source /opt/client/bigdata_env &&  kinit -kt /home/xdrloader/auth/user.keytab xdrloader"
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
      --driver-memory 10g 
      --supervise	
      --executor-memory 5g 
      --num-executors 10 
      --executor-cores 4 
      --files /home/xdrloader/auth/user.keytab 
      --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties' 
      --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=./log4.properties' 
      --conf 'spark.network.timeout=6000s' 
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' 
      /home/xdrloader/projects/moveToParquet/load-to-hive-parq-assembly-1.0.jar hdfs:///spark-scripts-config/load-to-hive/tdr-schema.xml convert-to-parquet-TDR >> /home/xdrloader/projects/moveToParquet/log/parquet-convertor-log.txt
      " 
      eval $sparkRun
fi





