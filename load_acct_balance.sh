#. ../../env.sh

#INCRMNTL_DATA_DIR=/u01/landing_dir/dryrun/nifi/grp1IncrTargets/BRM   #incremental files folder location.

script_path=/u01/landing_dir/2_incremental
input_file_path=$script_path/input_files   ## internal to script

inputfile="TC_ACCOUNT_BALANCE.unl"

start_time=`date '+%s'`

credential=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "STG_CREDENTIAL" |cut -d'=' -f2`
INCRMNTL_DATA_DIR=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "INCREMENTAL_DATA_DIR"  |cut -d'=' -f2`    ### INCREMENTAL FILE FOLDER LOCATION.
stg_user=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "STG_USER" |cut -d'=' -f2`
pin_user=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "PIN_USER" |cut -d'=' -f2`
input_file_path=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "BALANCE_INP_FILE_PATH" |cut -d'=' -f2` ##input file path for script.
inputfile=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "BALANCE_INP_FILE_NAME"  |cut -d'=' -f2` ##input file name for script .
bal_out_file_path=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "BALANCE_OUT_FILE_PATH"  |cut -d'=' -f2` ## Ouput  file path for script.
#echo "credential : $credential"

sql_query_run()
{
echo "Query underprocessing : $1"
 ###>>$pinlog_file
sql_result=`sqlplus -s $credential <<EOF
        set pagesize 0
        set linesize 32767
        set feedback off
        set verify off
        SET DEFINE OFF
        set heading off
        set serveroutput on
        set pages 0 echo off feed off
        $1
EOF`
}


truncate_table()
{

cnt_qry="select count(table_name) from all_tables where table_name='$1';"
sql_query_run "$cnt_qry"
if [ $sql_result -gt 0  ]; then
        sql_query_run "truncate table ${stg_user}.$1;"

else
        echo "table doesnot exists, Creating it."
                crt_tbl="CREATE TABLE ${stg_user}.TC_ACCT_BALANCE_DTAC (ACCTBALANCEID NUMBER, APPLYTIME VARCHAR2(20 BYTE), EXPIRETIME VARCHAR2(20 BYTE), ACCOUNTKEY NUMBER,    AMOUNT0 NUMBER, AMOUNT1 NUMBER, ACCTBALANCETYPE NUMBER, HOMECBP NUMBER, INTERFLAGSET NUMBER,BRM_RES_ID NUMBER ) ;"
                sql_query_run "$crt_tbl"
fi

}

drop_index()
{

cnt_qry="select count(index_name) from all_ind_columns where index_name='$1';"
sql_query_run "$cnt_qry"

if [ $sql_result -gt 0  ]; then
        sql_query_run "drop index $1;"
fi

}




#####################
#
#Main function starts here.
#####################

echo "-------------- start time $start_time ----------------"
if [ $# -eq 1 ]; then
        DIR_NUM=$1
else
        echo    "please pass brm directory number along with scriptname as first parameter."
        echo "for example pass 1 for BRM1 or 2 for  BRM2 or 3 for BRM3 or 4 for BRM4 directory"
        exit 1
fi


truncate_table "TC_ACCT_BALANCE_DTAC"
drop_index "DTAC_BAL_BALS_ID"

inp_file="$input_file_path/${DIR_NUM}-$inputfile"

if [ -f ${INCRMNTL_DATA_DIR}$DIR_NUM/valid/$inputfile  ]; then
 cp ${INCRMNTL_DATA_DIR}$DIR_NUM/valid/$inputfile $inp_file    ## copy into script input files folder to manipulate as per requirement
elif [ -f ${INCRMNTL_DATA_DIR}$DIR_NUM/$inputfile  ]; then
 cp ${INCRMNTL_DATA_DIR}$DIR_NUM/$inputfile $inp_file
else
        echo "$inputfile file is not present on $INCRMNTL_DATA_DIR$DIR_NUM, please validate manually"
        exit 1
fi

for file in `find $input_file_path -name ${DIR_NUM}-$inputfile`
do
        sed -i "s/010000000000000$/1|0/g;s/000000000000000$/0|0/g" $file
        sed -i -e "1d" $file
        echo "loading data for file $file"
        sqlldr $credential data=$file control=ctl_files/tc_acct_balance_dtac.ctl log=tc_acct_balance_dtac.log SILENT=FEEDBACK DIRECT=TRUE PARALLEL=TRUE &
done
wait

### Create indexes and enrich balance table.
sql_query_run "CREATE INDEX dtac_bal_bals_id ON ${stg_user}.TC_ACCT_BALANCE_DTAC (ACCTBALANCETYPE);"

#sql_query_run "merge into ${stg_user}.TC_ACCT_BALANCE_DTAC a using ${stg_user}.balance_map b on (a.ACCTBALANCETYPE = b.LEGACY_ID) when matched then update set a.brm_res_id = b.brm_id;"

##sql_query_run "update ${stg_user}.TC_ACCT_BALANCE_DTAC set  amount0 = (amount0/10000), amount1 = (amount1/10000) where ACCTBALANCETYPE in (2200,2513,5054,5151,5003,5006,5007,5052,5101,5152,2520,5051,5150,5155,2505,2509,5005,5200,6057,6058,2500,2512,5053,5201,5204,2501,2502,2511,5002,5004,5008,5202,2510,5102,6016,6060,2000,2503,5050,6059,8180);"

sql_query_run "update ${stg_user}.TC_ACCT_BALANCE_DTAC set  amount0 = (amount0/10000), amount1 = (amount1/10000) where ACCTBALANCETYPE in (2200,2513,2520,2505,2509,2500,2512,2501,2502,2511,2510,2000,2503);"

sql_query_run "update ${stg_user}.TC_ACCT_BALANCE_DTAC set applytime=REPLACE(REPLACE(applytime,'T',''),'Z',''),expiretime=replace(REPLACE(expiretime,'T',''),'Z','');"
sql_query_run "commit;"

#sql_query_run "SELECT b.poid_id0||'|'||ROW_NUMBER() OVER (PARTITION BY ACCOUNTKEY, c.brm_res_id ORDER BY ACCOUNTKEY, c.brm_res_id)         ||'|'||c.brm_res_id||'|'|| case when INTERFLAGSET =0 then amount0*-1                        when INTERFLAGSET =1 then amount1*-1           end             ||'|'||         round((to_date(applytime, 'YYYY-MM-DD HH24:MI:SS')-to_date('19700101', 'YYYYMMDD')) * 86400)||'|'||         round((to_date(expiretime, 'YYYY-MM-DD HH24:MI:SS')-to_date('19700101', 'YYYYMMDD')) * 86400)         FROM ${pin_user}.account_t a, ${pin_user}.bal_grp_t b, ${stg_user}.TC_ACCT_BALANCE_DTAC c         WHERE a.ACCESS_CODE2 = c.accountkey AND a.BAL_GRP_OBJ_ID0 = b.poid_id0;"

sql_query_run "SELECT /*+ PARALLEL(4) */ b.poid_id0||'|'||ROW_NUMBER() OVER (PARTITION BY ACCOUNTKEY, map.brm_id ORDER BY ACCOUNTKEY, map.brm_id)         ||'|'||map.brm_id||'|'||  round( case when INTERFLAGSET =0 then amount0*-1                        when INTERFLAGSET =1 then amount1*-1           end ,map.rounding)   ||'|'||         round((to_date(applytime, 'YYYY-MM-DD HH24:MI:SS')-to_date('19700101', 'YYYYMMDD')) * 86400)||'|'||         round((to_date(expiretime, 'YYYY-MM-DD HH24:MI:SS')-to_date('19700101', 'YYYYMMDD')) * 86400)         FROM ${pin_user}.account_t a, ${pin_user}.bal_grp_t b, ${stg_user}.TC_ACCT_BALANCE_DTAC c, ${stg_user}.balance_map map, ${pin_user}.config_beid_balances_t beid     WHERE a.ACCESS_CODE2 = c.accountkey AND a.BAL_GRP_OBJ_ID0 = b.poid_id0 and c.ACCTBALANCETYPE = map.LEGACY_ID and beid.rec_id= map.brm_id;"

echo "$sql_result" >$bal_out_file_path/bal_grp_sub_bals.lst

#sql_query_run "SELECT unique b.poid_id0||'|'||c.brm_res_id FROM ${pin_user}.account_t a, ${pin_user}.bal_grp_t b, ${stg_user}.TC_ACCT_BALANCE_DTAC c  WHERE a.ACCESS_CODE2 = c.accountkey AND a.BAL_GRP_OBJ_ID0 = b.poid_id0;"

sql_query_run "SELECT /*+ PARALLEL(4) */ unique b.poid_id0||'|'||map.brm_id FROM ${pin_user}.account_t a, ${pin_user}.bal_grp_t b, ${stg_user}.TC_ACCT_BALANCE_DTAC c,${stg_user}.balance_map map, ${pin_user}.config_beid_balances_t  WHERE a.ACCESS_CODE2 = c.accountkey AND a.BAL_GRP_OBJ_ID0 = b.poid_id0 and c.ACCTBALANCETYPE = map.LEGACY_ID and beid.rec_id= map.brm_id;"

echo "$sql_result" >$bal_out_file_path/bal_grp_bals.lst


####split files into 1 lakh record eacho
split -l 1000000 -d --additional-suffix=.dat $bal_out_file_path/bal_grp_sub_bals.lst $bal_out_file_path/bal_grp_sub_bals${DIR_NUM}_
split -l 1000000 -d --additional-suffix=.dat $bal_out_file_path/bal_grp_bals.lst $bal_out_file_path/bal_grp_bals${DIR_NUM}_

wait


rm -f $bal_out_file_path/bal_grp_sub_bals.lst
rm -f $bal_out_file_path/bal_grp_bals.lst

end_time=`date '+%s'`
echo "--------------end_time: $end_time ----------------"
letancy_time="$((end_time-start_time))"

echo "total time taken in seconds $letancy_time"


echo "Execution completed............."
