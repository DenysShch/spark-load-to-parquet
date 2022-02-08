CREATE TABLE
    tdr.tdr_aiu_ho(
        tdrid string,
        sysid int,
        orgcdrid string,
        abiscdrid string,
        acdrid string,
        horefid string,
        zoneid bigint,
        poolid bigint,
        sccpid string,
        starttime bigint,
        millisec int,
        abis int,
        srvstat int,
        cdrstat int,
        ho_type int,
        inter_type int,
        ni int,
        opc int,
        dpc int,
        linksetid int,
        access_type int,
        imsi string,
        tmsi string,
        msisdn string,
        imei string,
        mcc string,
        mnc string,
        orglac string,
        orgci string,
        tmnc string,
        targetlac string,
        targetci string,
        lastlac string,
        lastci string,
        orgrncid string,
        tarrncid string,
        horefnum bigint,
        codec int,
        ipaddr bigint,
        port int,
        hold int,
        ho_cause int,
        firsttei int,
        firstchannel int,
        sumrxlevlul bigint,
        sumrxlevldl bigint,
        sumrxqualul bigint,
        sumrxqualdl bigint,
        sumbspwr bigint,
        summspwr bigint,
        sumta bigint,
        totalmrno int,
        totalmrcount int,
        totalfullratemrno int,
        totalhalfratemrno int,
        sumrxquaul0 int,
        sumrxquaul1 int,
        sumrxquaul2 int,
        sumrxquaul3 int,
        sumrxquaul4 int,
        sumrxquaul5 int,
        sumrxquaul6 int,
        sumrxquaul7 int,
        sumrxquadl0 int,
        sumrxquadl1 int,
        sumrxquadl2 int,
        sumrxquadl3 int,
        sumrxquadl4 int,
        sumrxquadl5 int,
        sumrxquadl6 int,
        sumrxquadl7 int,
        ta01count int,
        rxlevel_down int,
        weak_coverage int,
        over_coverage int,
        imbalance int,
        interference int,
        ho_required_time bigint,
        ho_request_time bigint,
        ho_ack_time bigint,
        ho_cmd_time bigint,
        ho_detect_time bigint,
        ho_compt_time bigint,
        assn_time bigint,
        assn_cmpt_time bigint,
        alert_time bigint,
        answer_time bigint,
        direction int,
        hochartype int,
        ho_in_fail_time bigint,
        ho_out_fail_time bigint,
        ho_out_reject_time bigint,
        clear_req_time bigint,
        end_time bigint,
        pd int,
        firfailmsg int,
        cause int,
        encrypt_version int,
        reserved1 string,
        reserved2 string,
        reserved3 string,
        reserved4 string,
        reserved5 bigint,
        reserved6 bigint,
        reserved7 bigint,
        reserved8 bigint,
        filelocation string,
        offset_dsi bigint,
        homemcc string,
        homemnc string,
        homeproid bigint,
        homeareaid bigint,
        probeid bigint,
        probeid2 bigint,
        groupid int,
        lnkopc int,
        lnkdpc int,
        lnksrcip bigint,
        lnksrcport int,
        lnkdestip bigint,
        lnkdestport int,
        setup_time bigint,
        aiufirstxdrtype int,
        afirstxdrid string,
        mschotype int,
        hocallstate int,
        callsrvtype int,
        hointype int,
        initialmsc int,
        initialbsc int,
        initialmcc string,
        initialmnc string,
        initiallac string,
        initialci string,
        initialpoolid bigint,
        initialaccess_type int,
        hosourcemsc int,
        hosourceni int,
        hosourcepoolid bigint,
        hotargetmsc int,
        hotargetni int,
        hotargetpoolid bigint,
        callduration bigint,
        firfailtime bigint,
        layer1id int,
        layer2id int,
        layer3id int,
        layer4id int,
        layer5id int,
        layer6id int,
        initialchartype int,
        tchtime bigint,
        firsthotime bigint,
        rabrel_time bigint,
        dropcause int,
        drd int,
        prepaid_flag int,
        na  string
)
    PARTITIONED BY (
    dt string
)
STORED AS PARQUET;