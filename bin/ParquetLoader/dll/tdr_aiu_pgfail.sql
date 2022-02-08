CREATE TABLE
    tdr.tdr_aiu_pgfail(
     TDRID   string
    ,REFID   string
    ,ZONEID  bigint
    ,POOLID  bigint
    ,STARTTIME       bigint
    ,MILLISEC        int
    ,SRVSTAT int
    ,NI      int
    ,OPC     int
    ,DPC     int
    ,LINKSETID       int
    ,ACCESS_TYPE     int
    ,SRVTYPE int
    ,MSISDN  string
    ,IMSI    string
    ,TMSI    string
    ,MCC     string
    ,MNC     string
    ,LAC     string
    ,FIRPAGING_TIME  bigint
    ,SECPAGING_TIME  bigint
    ,THIRDPAGING_TIME        bigint
    ,FOURTHPAGING_TIME       bigint
    ,FIFTHPAGING_TIME        bigint
    ,ENCRYPT_VERSION int
    ,RESERVED1       string
    ,RESERVED2       string
    ,RESERVED3       string
    ,RESERVED4       string
    ,RESERVED5       bigint
    ,RESERVED6       bigint
    ,RESERVED7       bigint
    ,RESERVED8       bigint
    ,FILELOCATION    string
    ,OFFSET_DSI      string
    ,PROBEID bigint
    ,GROUPID int
    ,LNKOPC  int
    ,LNKDPC  int
    ,LNKSRCIP        bigint
    ,LNKSRCPORT      int
    ,LNKDESTIP       bigint
    ,LNKDESTPORT     int
    ,PAGINGRSP_TIME  bigint
    ,AUTH_REQ_TIME   bigint
    ,AUTH_RSP_TIME   bigint
    ,IDENTITY_REQ_TIME       bigint
    ,IDENTITY_RSP_TIME       bigint
    ,CIPH_REQ_TIME   bigint
    ,CIPH_RSP_TIME   bigint
    ,END_TIME        bigint
    ,PD      int
    ,FIRFAILMSG      int
    ,CAUSE   int
    ,CI      string
    ,LAYER1ID        int
    ,LAYER2ID        int
    ,LAYER3ID        int
    ,LAYER4ID        int
    ,LAYER5ID        int
    ,LAYER6ID        int
    ,CSFB_REF_FLAG   int
    ,E2EFIRFAILPROT  bigint
    ,E2EFIRFAILPD    int
    ,E2EFIRFAILMSG   int
    ,E2EFIRFAILCAUSE bigint
    ,FAILTYPE        int
    ,IMEI    string
    ,OLD_TMSI        string
    ,PREPAID_FLAG    int
    ,OPPRELCGI       string
    ,GS_PAGING_IND   int
    ,SGSN_int     string
    ,GS_CAUSE        int
    ,na              string
)
    PARTITIONED BY (
    dt string
)
STORED AS PARQUET;