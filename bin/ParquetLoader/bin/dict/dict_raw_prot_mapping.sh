
HOME="/home/xdrloader/projects/parquetLoader"
INIT="source /opt/client/bigdata_env &&  kinit -kt /home/xdrloader/auth/user.keytab xdrloader"
COMMAND="jps -lm | grep -i DICT_RAW_PROT_MAPPING.json"

eval $INIT
OUTPUT=$(eval "$COMMAND")
EXITCODE=$?

if [ -z "$OUTPUT" ]
then
      eval $INIT
      sparkRun="spark-submit
      --name conv-to-parq[dict_raw_prot_mapping]
      --master yarn
      --deploy-mode cluster
      --queue parquet_loader
      --driver-memory 2g
      --executor-memory 5g
      --num-executors 2
      --executor-cores 1
      --files /home/xdrloader/auth/user.keytab
      --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties'
      --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=./log4.properties'
      --conf 'spark.network.timeout=60000s'
      --conf 'spark.yarn.maxAppAttempts=1'
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
      --files '$HOME/configs/DICT_RAW_PROT_MAPPING.json'
      $HOME/lib/parquetLoader.jar DICT_RAW_PROT_MAPPING.json
      "
      eval $sparkRun
fi
