#######################
#
#
#######################
script_path=/u01/landing_dir/2_incremental
sdate=`date +%Y%m%d%H%M%S`

credential=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "STG_CREDENTIAL" |cut -d'=' -f2`
log_file=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "ACCUMULATOR_LOADING_LOG" |cut -d'=' -f2`
stg_user=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "STG_USER" |cut -d'=' -f2`
pin_user=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "PIN_USER" |cut -d'=' -f2`
accumu_out_file_path=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "ACCUMULATOR_OUT_FILE_PATH"  |cut -d'=' -f2` ##Output file path for script .

start_time=`date '+%s'`

sql_query_run()
{
echo "Query underprocessing : $1"
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



if [ $# -eq 1 ]; then
        DIR_NUM=$1
        echo "----------- Loading process starts at $sdate -----------" | tee -a $log_file
else
        echo    "please pass brm directory number along with scriptname as first parameter: $sdate." | tee -a $log_file
        exit 1
fi

for file in `find ${accumu_out_file_path} -name bal_grp_bals${DIR_NUM}_*dat`
  do
    echo " File under process : $file" | tee -a $log_file
    sqlldr $credential data=$file rows=50000 errors=1000000 control=$script_path/ctl_files/bal_grp_bals_t.ctl log=$file.log SILENT=FEEDBACK &

  done
wait

for file in `find ${accumu_out_file_path} -name bal_grp_sub_bals${DIR_NUM}_*dat`
  do
    echo " File under process : $file" | tee -a $log_file
    sqlldr $credential data=$file rows=50000 errors=1000000 control=$script_path/ctl_files/bal_grp_sub_bals_t.ctl log=$file.log SILENT=FEEDBACK &
  done

wait

echo "----- Loading Ends at `date +%Y%m%d%H%M%S` -----" | tee -a $log_file

echo "Loading statistics" | tee -a $log_file
echo "FileName|No_Of_records_inFile|Load_count|failed_count|skip_count|loading time taken|connection lost error code"  | tee -a $log_file
for file in `find ${accumu_out_file_path} -name bal_grp*_bals${DIR_NUM}_*dat`
do
#               echo " File Stats : $file" | tee -a $log_file
                rec_count=`cat $file | wc -l`
                load_count=`grep " successfully loaded." ${file}.log | awk '{$1=$1};1'| cut -d' ' -f1`
                failed_count=`grep " not loaded due to data errors" ${file}.log | awk '{$1=$1};1'| cut -d' ' -f1`
                difference=$(( $rec_count - $load_count - $failed_count ))

                loading_time_taken=`grep "Elapsed time was:" ${file}.log | awk -F' ' '{print $4}'`
                Sql_error=`grep "the load was aborted because SQL Loader cannot continue" ${file}.log | awk -F':' '{print $1}'`
                echo "${file}|${rec_count}|${load_count}|${failed_count}|${difference}|${loading_time_taken}|${Sql_error}"  | tee -a $log_file

done

echo "stats ends at `date +%Y%m%d%H%M%S` -----" | tee -a $log_file


#move bad files into bad_files folder, if any present
mv $script_path/ctl_files/bal_grp_sub_bals${DIR_NUM}_*.bad $script_path/bad_files/accumulator/ 2>/dev/null
mv $script_path/ctl_files/bal_grp_bals${DIR_NUM}_*bad $script_path/bad_files/accumulator/       2>/dev/null

########################
#
#Credit limit fix post run
########################

sql_query_run "insert into ${pin_user}.cfg_credit_profile_t (OBJ_ID0, REC_ID, CREDIT_FLOOR, CREDIT_LIMIT, CREDIT_THRESHOLDS,CREDIT_THRESHOLDS_FIXED) select frm.obj_id0,(select max(rec_id) from ${pin_user}.cfg_Credit_profile_t)+rownum rec_id,CREDIT_FLOOR, CREDIT_LIMIT, CREDIT_THRESHOLDS ,CREDIT_THRESHOLDS_FIXED from  ( select distinct 201 obj_id0,CREDIT_FLOOR, CREDIT_LIMIT, CREDIT_THRESHOLDS,b.CREDIT_THRESHOLDS_FIXED from ${pin_user}.config_tab_package_descr_t a,${pin_user}.config_tab_pkg_credit_limit_t b where a.rec_id=b.rec_id2 and not exists (select 1 from ${pin_user}.cfg_Credit_profile_t where NVL(CREDIT_FLOOR,0)=NVL(b.CREDIT_FLOOR,0) and CREDIT_LIMIT=b.CREDIT_LIMIT and CREDIT_THRESHOLDS=b.CREDIT_THRESHOLDS)) frm;"

sql_query_run "merge /*+ PARALLEL(4) */ into ${pin_user}.bal_grp_bals_t h using (select frm2.* from (select bal_grp_poid,b.rec_id resource_id, frm.name deal_name,b.CREDIT_FLOOR, b.CREDIT_LIMIT, b.CREDIT_THRESHOLDS, b.credit_thresholds_fixed, c.rec_id, ROW_NUMBER() OVER (PARTITION BY BAL_GRP_POID, b.rec_id ORDER BY b.credit_floor desc) rn from ${pin_user}.config_tab_package_descr_t a,${pin_user}.config_tab_pkg_credit_limit_t b, ${pin_user}.cfg_credit_profile_t c, (select distinct a.poid_id0 bal_grp_poid, c.name from ${pin_user}.bal_grp_t a, ${pin_user}.purchased_product_t b, ${pin_user}.deal_t c where a.account_obj_id0=b.account_obj_id0 and b.deal_obj_id0=c.poid_id0 and b.status=1 and a.poid_db>=1) frm where a.package_name=frm.name and a.rec_id=b.rec_id2 and NVL(b.CREDIT_FLOOR,-99.99)=NVL(c.CREDIT_FLOOR,-99.99) and b.CREDIT_LIMIT=nvl(c.CREDIT_LIMIT,10) and b.CREDIT_THRESHOLDS=c.CREDIT_THRESHOLDS and '|'|| b.credit_thresholds_fixed = NVL(c.credit_thresholds_fixed,'')  order by BAL_GRP_POID, RESOURCE_ID) frm2 where rn=1) frm1 on (h.obj_id0=frm1.bal_grp_poid and h.rec_id=frm1.resource_id) when matched then update set h.credit_profile=frm1.rec_id;"


sql_query_run "update /*+ parallel(8) */ ${pin_user}.bal_grp_bals_t set credit_profile=2 where credit_profile=0 and (rec_id not in (840,764) and rec_id like '1%');"
sql_query_run "commit;"

sql_query_run "update /*+ parallel(8) */ ${pin_user}.bal_grp_bals_t set credit_profile=4 where rec_id in (764) and credit_profile!=4;"

sql_query_run "update /*+ parallel(8) */ ${pin_user}.bal_grp_sub_bals_t set current_bal=current_bal*-1 where rec_id2 > 20000000 and current_bal>0;"

sql_query_run "commit;"

echo "Postrun ends at `date +%Y%m%d%H%M%S` -----" | tee -a $log_file

end_time=`date '+%s'`


edate=`date +%Y%m%d%H%M%S`

echo "--------------end_time: $edate ----------------"
letancy_time="$((end_time-start_time))"

echo "total time taken in loading seconds $letancy_time"
echo "Loading completed--------------------------------"