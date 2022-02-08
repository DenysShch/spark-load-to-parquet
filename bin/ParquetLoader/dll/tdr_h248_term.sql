CREATE TABLE
    tdr.tdr_h248_term(
        CDRID   string,
        REFID   string,
        IMSI    string,
        AIUCDRTYPE      int,
        CNTID   string,
        STARTTIME       bigint,
        MILLISEC        int,
        REMOTECDRID     string,
        RESULT  int,
        NI      int,
        MGWDPC  int,
        MSCDPC  int,
        MGWIPADDR       bigint,
        MSCIPADDR       bigint,
        REFINTERFACE    int,
        TERMTYPE        int,
        TERMID  string,
        PORT    bigint,
        DSTID   string,
        DSTPORT int,
        CODECTYPE       int,
        PACKAGINTIME    int,
        ENDTIME bigint,
        CAUSE   int,
        MGWPORT int,
        MSCPORT int,
        LAYER1ID        int,
        LAYER2ID        int,
        LAYER3ID        int,
        LAYER4ID        int,
        LAYER5ID        int,
        LAYER6ID        int,
        na string
)
    PARTITIONED BY (
    dt string
)
STORED AS PARQUET;