CREATE TABLE
    tdr.tdr_aiu_moss(
        TDRID   string
        ,REFID   string
        ,STARTTIME   bigint
        ,MILLISEC    int
        ,SRVSTAT int
        ,CDRSTAT int
        ,POOLID  bigint
        ,NI  int
        ,OPC int
        ,DPC int
        ,LINKTYPE    int
        ,LNKOPC  int
        ,LNKDPC  int
        ,LNKSRCIP    bigint
        ,LNKSRCPORT  int
        ,LNKDESTIP   bigint
        ,LNKDESTPORT int
        ,ACCESS_TYPE int
        ,IMSI    string
        ,IMEI    string
        ,TMSI    string
        ,MSISDN  string
        ,MCC string
        ,MNC string
        ,LAC string
        ,CI  string
        ,SSCODE  int
        ,SRVTYPE int
        ,OP_RSP_TIME bigint
        ,END_TIME    bigint
        ,USSD_DATA_STRING    string
        ,PD  int
        ,FIRFAILMSG  int
        ,CAUSE   int
        ,VASSERVICETYPE  int
        ,VOUCHERint   string
        ,RELOADSUBMSISDN string
        ,RESERVED1   string
        ,RESERVED2   string
        ,RESERVED3   string
        ,RESERVED4   string
        ,RESERVED5   bigint
        ,RESERVED6   bigint
        ,RESERVED7   bigint
        ,RESERVED8   bigint
        ,FILELOCATION    string
        ,OFFSET_DSI  string
        ,PROBEID bigint
        ,ERRORCOMPONENTTYPE  int
        ,REJECTTYPE  int
        ,SSCAUSECODE int
        ,FIRFAILTIME bigint
        ,LAYER1ID    int
        ,LAYER2ID    int
        ,LAYER3ID    int
        ,LAYER4ID    int
        ,LAYER5ID    int
        ,LAYER6ID    int
        ,FIRSTLAC    string
        ,FIRSTCI string
        ,LAST_ACCESS_TYPE    int
        ,E2EFIRFAILPROT  bigint
        ,E2EFIRFAILPD    int
        ,E2EFIRFAILMSG   int
        ,E2EFIRFAILCAUSE bigint
        ,FIRST_RAT   int
        ,LAST_LAC    string
        ,LAST_CI string
        ,OLD_TMSI    string
        ,PREPAID_FLAG    int
        ,na string

)
    PARTITIONED BY (
    dt string
)
STORED AS PARQUET;