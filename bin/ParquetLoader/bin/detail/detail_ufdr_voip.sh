
HOME="/home/xdrloader/projects/parquetLoader"
INIT="source /opt/client/bigdata_env &&  kinit -kt /home/xdrloader/auth/user.keytab xdrloader"
COMMAND="jps -lm | grep -i DETAIL_UFDR_VOIP.json"

eval $INIT
OUTPUT=$(eval "$COMMAND")
EXITCODE=$?

if [ -z "$OUTPUT" ]
then
      eval $INIT
      sparkRun="spark-submit
      --name conv-to-parq[detail_ufdr_voip]
      --master yarn
      --queue parquet_loader
      --deploy-mode cluster
      --driver-memory 4g
      --executor-memory 10g
      --num-executors 4
      --executor-cores 2
      --files /home/xdrloader/auth/user.keytab
      --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties'
      --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=./log4.properties'
      --conf 'spark.network.timeout=60000s'
      --conf 'spark.yarn.maxAppAttempts=1'
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
      --conf 'spark.yarn.executor.memoryOverhead=1009'
      --files '$HOME/configs/DETAIL_UFDR_VOIP.json'
      $HOME/lib/parquetLoader.jar DETAIL_UFDR_VOIP.json
      "
      eval $sparkRun
fi
