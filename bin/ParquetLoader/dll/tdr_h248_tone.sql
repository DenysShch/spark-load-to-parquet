CREATE TABLE
        tdr.tdr_h248_tone(
                             cdrid string,
                             termcdrid string,
                             starttime string,
                             millisec string,
                             result string,
                             ni string,
                             mgwdpc string,
                             mscdpc string,
                             toneid string,
                             mgwipaddr string,
                             mscipaddr string,
                             failcause string,
                             mgwport string,
                             mscport string,
                             layer1id string,
                             layer2id string,
                             layer3id string,
                             layer4id string,
                             layer5id string,
                             layer6id string,
                             na string
)
    PARTITIONED BY (
    dt string
)
STORED AS PARQUET;