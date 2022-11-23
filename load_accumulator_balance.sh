#. ../../env.sh

script_path=/u01/landing_dir/2_incremental
input_file_path=$script_path/input_files   ## internal to script

inputfile="CBE_SUBSCRIBER_CUM.unl"

start_time=`date '+%s'`


credential=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "STG_CREDENTIAL" |cut -d'=' -f2`
INCRMNTL_DATA_DIR=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "INCREMENTAL_DATA_DIR"  |cut -d'=' -f2`    ### INCREMENTAL FILE FOLDER LOCATION.
stg_user=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "STG_USER" |cut -d'=' -f2`
pin_user=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "PIN_USER" |cut -d'=' -f2`
input_file_path=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "ACCUMULATOR_INP_FILE_PATH" |cut -d'=' -f2` ##input file path for script.
inputfile=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "ACCUMULATOR_INP_FILE_NAME"  |cut -d'=' -f2` ##input file name for script .
accumu_out_file_path=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "ACCUMULATOR_OUT_FILE_PATH"  |cut -d'=' -f2` ##Output file path for script .

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

                crt_tbl="CREATE TABLE ${stg_user}.$1 (SUBSKEY NUMBER, CUMULATEID NUMBER, APPLYTIME VARCHAR2(20 BYTE), EXPIRETIME VARCHAR2(20 BYTE), AMOUNT NUMBER, PREAMOUNT NUMBER,INTERFLAGSET NUMBER, HOMECPB NUMBER,BRM_RES_ID NUMBER ) ;"
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

echo "--------------start_time: $start_time ----------------"
if [ $# -eq 1 ]; then
        if [ $1 -gt 0 ] && [ $1 -lt 5 ]; then
                DIR_NUM=$1
        else
                echo "please pass brm directory number along with scriptname as first parameter."
                echo "for example pass 1 for BRM1 or 2 for  BRM2 or 3 for BRM3 or 4 for BRM4 directory"
                exit 1
        fi
else
        echo "please pass brm directory number along with scriptname as first parameter."
        echo "for example pass 1 for BRM1 or 2 for  BRM2 or 3 for BRM3 or 4 for BRM4 directory"
        exit 1
fi

inp_file="$input_file_path/${DIR_NUM}-$inputfile"

truncate_table "TC_ACCUMULATOR_BALANCE_DTAC"
drop_index "DTAC_ACCU_BALS_ID"


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
        echo "loading data for file $file"
        sed -i -e "1d" $file
        sed -i "s/|010000000000000|/|1|/g;s/|000000000000000|/|0|/g" $file
        #sed -i "s/|null|/|0|/g" $file
        sqlldr $credential data=$file control=ctl_files/subs_cum.ctl log=subs_accumulator.log SILENT=FEEDBACK DIRECT=TRUE PARALLEL=TRUE &
done
wait


### Create indexes and enrich balance table.
sql_query_run "CREATE INDEX DTAC_ACCU_BALS_ID ON ${stg_user}.TC_ACCUMULATOR_BALANCE_DTAC (ACCTBALANCETYPE);"
#sql_query_run "update ${stg_user}.TC_ACCUMULATOR_BALANCE_DTAC set applytime=replace(applytime,'T',''),expiretime=replace(expiretime,'T','');"
#sql_query_run "update ${stg_user}.TC_ACCUMULATOR_BALANCE_DTAC set applytime=replace(applytime,'Z',''),expiretime=replace(expiretime,'Z','');"
sql_query_run "update ${stg_user}.TC_ACCUMULATOR_BALANCE_DTAC set applytime=REPLACE(REPLACE(applytime,'T',''),'Z',''),expiretime=replace(REPLACE(expiretime,'T',''),'Z','');"
sql_query_run "commit;"
###     merge into ${stg_user}.TC_ACCUMULATOR_BALANCE_DTAC a  using ACCUMULATOR_MAP b  on (a.ACCTBALANCETYPE = b.LEGACY_ID)         when matched then update         set a.brm_res_id = b.brm_id;

sql_query_run "SELECT /*+ PARALLEL(4) */ b.poid_id0||'|'||ROW_NUMBER() OVER (PARTITION BY SUBSKEY, map.BRM_ID ORDER BY SUBSKEY, map.BRM_ID)   ||'|'||map.BRM_ID||'|'||             amount   ||'|'||         round((to_date(applytime, 'YYYY-MM-DD HH24:MI:SS')-to_date('19700101', 'YYYYMMDD')) * 86400) ||'|'||         round((to_date(expiretime, 'YYYY-MM-DD HH24:MI:SS')-to_date('19700101', 'YYYYMMDD')) * 86400) FROM ${pin_user}.service_t a, ${pin_user}.bal_grp_t b, ${stg_user}.TC_ACCUMULATOR_BALANCE_DTAC c,  ${stg_user}.ACCUMULATOR_MAP map   WHERE a.aac_access = c.SUBSKEY AND a.BAL_GRP_OBJ_ID0 = b.poid_id0 and c.cumulateid = map.LEGACY_ID and (map.type = ('Master') or map.type is null) ;"

echo "$sql_result" >${accumu_out_file_path}/bal_grp_sub_bals_${DIR_NUM}.lst

sql_query_run "SELECT /*+ PARALLEL(4) */ unique b.poid_id0||'|'||map.BRM_ID FROM ${pin_user}.service_t a, ${pin_user}.bal_grp_t b, ${stg_user}.TC_ACCUMULATOR_BALANCE_DTAC c, ${stg_user}.ACCUMULATOR_MAP map   WHERE a.aac_access = c.SUBSKEY AND a.BAL_GRP_OBJ_ID0 = b.poid_id0 and c.CUMULATEID = map.LEGACY_ID and (map.type = ('Master') or map.type is null);"

echo "$sql_result" >${accumu_out_file_path}/bal_grp_bals_${DIR_NUM}.lst

#split files into 1 lakh record.

#split files into 10 lakh record eacho
split -l 1000000 -d --additional-suffix=.dat ${accumu_out_file_path}/bal_grp_sub_bals_${DIR_NUM}.lst ${accumu_out_file_path}/$bal_grp_sub_bals${DIR_NUM}_
split -l 1000000 -d --additional-suffix=.dat ${accumu_out_file_path}/bal_grp_bals_${DIR_NUM}.lst ${accumu_out_file_path}/bal_grp_bals${DIR_NUM}_
wait

rm -f ${accumu_out_file_path}/bal_grp_bals_${DIR_NUM}.lst
rm -f ${accumu_out_file_path}/bal_grp_sub_bals_${DIR_NUM}.lst

end_time=`date '+%s'`
echo "--------------end_time: $end_time ----------------"
letancy_time="$((end_time-start_time))"

echo "total time taken in seconds $letancy_time"

echo "processing completed--------------------------------"
