HOME="/home/xdrloader/projects/parquetLoader"
INIT="source /opt/client/bigdata_env &&  kinit -kt /home/xdrloader/auth/user.keytab xdrloader"
COMMAND="jps -lm | grep -i DETAIL_UFDR_Other.json"
eval $INIT
OUTPUT=$(eval "$COMMAND")
EXITCODE=$?

if [ -z "$OUTPUT" ]
then
      eval $INIT
      sparkRun="spark-submit
      --name convert-to-parquet[detail_ufdr_other]
      --master yarn
      --queue parquet_loader
      --deploy-mode cluster
      --driver-memory 4g
      --executor-memory 6g
      --num-executors 12
      --executor-cores 4
      --files /home/xdrloader/auth/user.keytab
      --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties'
      --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=./log4.properties'
      --conf 'spark.network.timeout=60000s'
      --conf 'spark.yarn.maxAppAttempts=1'
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
      --files '$HOME/configs/DETAIL_UFDR_Other.json'
      $HOME/lib/parquetLoader.jar DETAIL_UFDR_Other.json
      "
      eval $sparkRun
fi
