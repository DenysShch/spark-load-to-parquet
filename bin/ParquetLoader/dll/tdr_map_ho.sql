CREATE TABLE
    tdr.tdr_map_ho(
        tdrid string,
        horefid string,
        starttime int,
        millisec int,
        srvstat string,
        cdrstat string,
        inter_type string,
        ni int,
        opc int,
        ossn int,
        ogt string,
        dpc int,
        dssn int,
        dgt string,
        linksetid string,
        srvtype string,
        imsi string,
        tmsi string,
        msisdn string,
        ho_numeric string,
        mcc int,
        mnc int,
        targetlac string,
        targetci string,
        ho_cause int,
        hofailcautype string,
        hofailmes string,
        hofailcause string,
        end_time int,
        rel_type int,
        cause int,
        encrypt_version string,
        reserved1 string,
        reserved2 string,
        reserved3 string,
        reserved4 int,
        reserved5 int,
        reserved6 string,
        reserved7 int,
        reserved8 string,
        filelocation string,
        offset_dsi int,
        probeid int,
        groupid int,
        sig_collection_type int,
        lnkopc int,
        lnkdpc int,
        lnksrcip int,
        lnksrcport int,
        lnkdestip int,
        lnkdestport int,
        fwas int,
        ses string,
        pas int,
        pd int,
        firfailtime string,
        layer1id string,
        layer2id string,
        layer3id string,
        layer4id string,
        layer5id string,
        layer6id string,
        prepaid_flag string,
        na string
)
    PARTITIONED BY (
    dt string
)
STORED AS PARQUET;