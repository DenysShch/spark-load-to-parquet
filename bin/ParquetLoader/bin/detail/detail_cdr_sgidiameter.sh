
HOME="/home/xdrloader/projects/parquetLoader"
INIT="source /opt/client/bigdata_env &&  kinit -kt /home/xdrloader/auth/user.keytab xdrloader"
COMMAND="jps -lm | grep -i DETAIL_CDR_SGiDiameter.json"

eval $INIT
OUTPUT=$(eval "$COMMAND")
EXITCODE=$?

if [ -z "$OUTPUT" ]
then
      eval $INIT
      sparkRun="spark-submit
      --name conv-to-parq[detail_cdr_sgidiameter]
      --master yarn
      --deploy-mode cluster
      --queue parquet_loader
      --driver-memory 4g
      --executor-memory 5g
      --num-executors 5
      --executor-cores 4
      --files /home/xdrloader/auth/user.keytab
      --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties'
      --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=./log4.properties'
      --conf 'spark.network.timeout=60000s'
      --conf 'spark.yarn.maxAppAttempts=1'
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
      --files '$HOME/configs/DETAIL_CDR_SGiDiameter.json'
      $HOME/lib/parquetLoader.jar DETAIL_CDR_SGiDiameter.json
      "
      eval $sparkRun
fi
