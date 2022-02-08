CREATE TABLE
    tdr.tdr_h248_ctx(
    cdrid string,
    starttime int,
    millisec int,
    callerno string,
    calledno string,
    end_time int,
    reserved1 string,
    reserved2 string,
    reserved3 string,
    reserved4 string,
    filelocation string,
    offset_dsi string,
    probeid string,
    imsi string,
    refid string,
    na string
)
    PARTITIONED BY (
    dt string
)
STORED AS PARQUET;