
HOME="/home/xdrloader/projects/parquetLoader"
INIT="source /opt/client/bigdata_env &&  kinit -kt /home/xdrloader/auth/user.keytab xdrloader"
COMMAND="jps -lm | grep -i TDR_H248_TONE.json"

eval $INIT
OUTPUT=$(eval "$COMMAND")
EXITCODE=$?

if [ -z "$OUTPUT" ]
then
      eval $INIT
      sparkRun="spark-submit
      --name conv-to-parq[tdr_h248_tone]
      --master yarn
      --queue parquet_loader
      --deploy-mode cluster
      --driver-memory 4g
      --executor-memory 5g
      --num-executors 2
      --executor-cores 2
      --files /home/xdrloader/auth/user.keytab
      --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties'
      --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=./log4.properties'
      --conf 'spark.network.timeout=60000s'
      --conf 'spark.yarn.maxAppAttempts=1'
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
      --files '$HOME/configs/TDR_H248_TONE.json'
      $HOME/lib/parquetLoader.jar TDR_H248_TONE.json
      "
      eval $sparkRun
fi
