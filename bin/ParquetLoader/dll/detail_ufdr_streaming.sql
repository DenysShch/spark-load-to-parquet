CREATE TABLE
    detail.detail_ufdr_streaming(
        INTERFACEID int,
        BEGIN_TIME  bigint,
        BEGIN_TIME_MSEL int,
        END_TIME    bigint,
        END_TIME_MSEL   int,
        PROT_CATEGORY   int,
        PROT_TYPE   int,
        L7_CARRIER_PROT int,
        MSISDN  string,
        IMSI    string,
        IMEI    string,
        ROAM_DIRECTION  int,
        MS_IP   string,
        SERVER_IP   string,
        MS_PORT int,
        SERVER_PORT int,
        APN string,
        SGSN_USER_IP    string,
        GGSN_USER_IP    string,
        MCC string,
        MNC string,
        RAT int,
        LAC string,
        RAC string,
        SAC string,
        CI  string,
        BROWSER_TYPE    int,
        L4_UL_THROUGHPUT    string,
        L4_DW_THROUGHPUT    string,
        L4_UL_GOODPUT   string,
        L4_DW_GOODPUT   string,
        L4_UL_PACKETS   bigint,
        L4_DW_PACKETS   bigint,
        TCP_CONN_STATES int,
        TCP_RTT bigint,
        TCP_UL_OUTOFSEQU    bigint,
        TCP_DW_OUTOFSEQU    bigint,
        TCP_UL_RETRANS  bigint,
        TCP_DW_RETRANS  bigint,
        TCP_WIN_SIZE    bigint,
        HOST    string,
        STREAMING_URL   string,
        STREAMING_DW_PACKETS    string,
        STREAMING_DOWNLOAD_DELAY    bigint,
        VIDEO_FRAME_RATE    int,
        VIDEO_CODEC_ID  string,
        VIDEO_WIDTH int,
        VIDEO_HEIGHT    int,
        AUDIO_DATA_RATE bigint,
        AUDIO_CODEC_ID  string,
        STREAMING_FILESIZE  string,
        STREAMING_DURATIOIN bigint,
        MEDIA_FILE_TYPE int,
        TAC string,
        ECI string,
        TCP_RTT_STEP1   bigint,
        TCP_UL_RETRANS_WITHPL   bigint,
        TCP_DW_RETRANS_WITHPL   bigint,
        TCP_UL_PACKAGES_WITHPL  bigint,
        TCP_DW_PACKAGES_WITHPL  bigint,
        RAN_NE_USER_IP  string,
        HOMEMCC string,
        HOMEMNC string,
        USER_AGENT  string,
        DATATRANS_UL_DURATION   string,
        MS_WIN_STAT_TOTAL_NUM   int,
        MS_WIN_STAT_SMALL_NUM   int,
        MS_ACK_TO_1STGET_DELAY  int,
        SERVER_ACK_TO_1STDATA_DELAY int,
        AVG_UL_RTT  bigint,
        AVG_DW_RTT  bigint,
        UL_RTT_LONG_NUM int,
        DW_RTT_LONG_NUM int,
        UL_RTT_STAT_NUM int,
        DW_RTT_STAT_NUM int,
        USER_PROBE_UL_LOST_PKT  int,
        SERVER_PROBE_UL_LOST_PKT    int,
        SERVER_PROBE_DW_LOST_PKT    int,
        USER_PROBE_DW_LOST_PKT  int,
        STREAMING_TYPE  int,
        CHARGING_CHARACTERISTICS    string,
        DL_SERIOUS_OUT_OF_ORDER_NUM bigint,
        DL_SLIGHT_OUT_OF_ORDER_NUM  bigint,
        DL_FLIGHT_TOTAL_SIZE    bigint,
        DL_FLIGHT_TOTAL_NUM bigint,
        DL_MAX_FLIGHT_SIZE  bigint,
        UL_SERIOUS_OUT_OF_ORDER_NUM bigint,
        UL_SLIGHT_OUT_OF_ORDER_NUM  bigint,
        UL_FLIGHT_TOTAL_SIZE    bigint,
        UL_FLIGHT_TOTAL_NUM bigint,
        UL_MAX_FLIGHT_SIZE  bigint,
        USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS bigint,
        SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS   bigint,
        DL_CONTINUOUS_RETRANSMISSION_DELAY  string,
        USER_HUNGRY_DELAY   string,
        SERVER_HUNGRY_DELAY string,
        CHARGE_ID   bigint,
        SV  string,
        DNS_RETRANS_NUM int,
        DNS_FAIL_CODE   int,
        SUB_PROT_TYPE   int,
        APP_ID  int,
        DATATRANS_DW_GOODPUT    string,
        DATATRANS_DW_TOTAL_DURATION bigint,
        FAIL_CLASS  string,
        START_DOWNLOAD_THROUGHPUT   string,
        L7_UL_GOODPUT_FULL_MSS  string,
        AVG_UL_RTT_MICRO_SEC    int,
        AVG_DW_RTT_MICRO_SEC    int,
        STREAMING_CACHE_HOST    string,
        STREAMING_CACHE_URL string,
        CLIENTHELLO_COUNTS  int,
        FINISH_COUNTS   int,
        TOTALDELAYS_OF_SHAKEHANDS   bigint,
        BYTESOF_2DRECTION_DURHANDSHAKE  bigint,
        HTTPS_FAILCODE  int,
        SERVICE_VALID_FLAG  int,
        VIDEO_ESTIMATE_DATA_RATE    bigint,
        VIDEO_START_FLAG    int,
        VIDEO_START_DELAY   bigint,
        VIDEO_START_DL_GOODPUT  bigint,
        L7_DL_GOODPUT_FULL_MSS  string,
        DATATRANS_DL_DURATION   string,
        TRAFFIC_CLASS_NEG   int,
        THP_NEG int,
        QCI_NEG bigint,
        AVG_UL_JITTER   string,
        AVG_DL_JITTER   string,
        MAX_UL_JITTER   bigint,
        MAX_DL_JITTER   bigint,
        TOTAL_TCP_RTT   string,
        TOTAL_TCP_RTT_STEP1 string,
        NA string
)
    PARTITIONED BY (
    dt string
)
STORED AS PARQUET;