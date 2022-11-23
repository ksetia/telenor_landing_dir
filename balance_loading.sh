#
#######################
sdate=`date +%Y%m%d%H%M%S`
script_path=/u01/landing_dir/2_incremental
credential=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "STG_CREDENTIAL" |cut -d'=' -f2`
log_file=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "BALANCE_LOADING_LOG" |cut -d'=' -f2`
bal_out_file_path=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "BALANCE_OUT_FILE_PATH"  |cut -d'=' -f2` ## Ouput  file path for script.
stg_user=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "STG_USER" |cut -d'=' -f2`
pin_user=`egrep -v "^#" /u01/landing_dir/2_incremental/scripts.config | grep "PIN_USER" |cut -d'=' -f2`

start_time=`date '+%s'`

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



if [ $# -eq 1 ]; then
        DIR_NUM=$1
        echo "----------- Loading process starts at $sdate -----------" | tee -a $log_file
else
        echo    "please pass brm directory number along with scriptname as first parameter: $sdate." | tee -a $log_file
        exit 1
fi

echo "----- Loading StartTime: `date '+%s'` ------"  | tee -a $log_file
for file in `find ${bal_out_file_path} -name bal_grp_bals${DIR_NUM}_*dat`
  do
        echo " File under process : $file" | tee -a $log_file
    sqlldr $credential data=$file errors=2000000 control=$script_path/ctl_files/bal_grp_bals_t.ctl log=$file.log SILENT=FEEDBACK &
  done

wait

for file in `find ${bal_out_file_path} -name bal_grp_sub_bals${DIR_NUM}_*dat`
  do
    echo " File under process : $file" | tee -a $log_file
    sqlldr $credential data=$file errors=2000000 control=$script_path/ctl_files/bal_grp_sub_bals_t.ctl log=$file.log SILENT=FEEDBACK &
  done

wait
echo "----- Loading EndTime : `date '+%s'` ------"  | tee -a $log_file

echo "----- Loading Stats Gathering starttime : `date '+%s'` ------"  | tee -a $log_file

echo "Loading statistics" | tee -a $log_file
echo "FileName|No_Of_records_inFile|Load_count|failed_count|skip_count|Loading_time_taken|sql_error"  | tee -a $log_file
#loading_time_taken=`grep "Elapsed time was:" ${data_files_path}/${log_file} | awk -F' ' '{print $4}'`
for file in `find ${bal_out_file_path} -name bal_grp*_bals${DIR_NUM}_*dat`
do
        rec_count=`cat $file | wc -l`
        load_count=`grep " successfully loaded." ${file}.log | awk '{$1=$1};1'| cut -d' ' -f1`
        failed_count=`grep " not loaded due to data errors" ${file}.log | awk '{$1=$1};1'| cut -d' ' -f1`
        difference=$(( $rec_count - $load_count - $failed_count ))
        loading_time_taken=`grep "Elapsed time was:" ${file}.log | awk -F' ' '{print $4}'`
        Sql_error=`grep "the load was aborted because SQL Loader cannot continue" ${file}.log | awk -F':' '{print $1}'`

        echo "${file}|${rec_count}|${load_count}|${failed_count}|${difference}|${loading_time_taken}|${Sql_error}"  | tee -a $log_file

done
echo "----- Loading Stats Gathering endtime : `date '+%s'` ------"  | tee -a $log_file

        mv $script_path/ctl_files/bal_grp_sub_bals${DIR_NUM}_*.bad $script_path/bad_files/balance/ 2>/dev/null
        mv $script_path/ctl_files/bal_grp_bals${DIR_NUM}_*bad $script_path/bad_files/balance/ 2>/dev/null

echo "--------------Loading end_time: $edate ----------------"  | tee -a $log_file

echo "----- PostRun starttime : `date '+%s'` ------"  | tee -a $log_file

        sql_query_run "update ${pin_user}.bal_grp_sub_bals_t set valid_to = 0 where valid_to = 2114380800;"
        sql_query_run "update /*+ PARALLEL(4) */ ${pin_user}.BAL_GRP_SUB_BALS_T s set SUBTYPE =(select case when rec_id2=765 then 1 else 0 end from ${pin_user}.BAL_GRP_SUB_BALS_T sg where sg.obj_id0 = s.obj_id0 and sg.rec_id2 = s.rec_id2 and sg.rec_id = s.rec_id) where obj_id0 in (select obj_id0 from ${pin_user}.BAL_GRP_SUB_BALS_T);"
        sql_query_run "update /*+ PARALLEL(4) */ ${pin_user}.bal_grp_sub_bals_t_mig set rec_id=rec_id+999 where rec_id2=765;"
        sql_query_run "update /*+ PARALLEL(4) */ ${pin_user}.bal_grp_sub_bals_t_mig set rec_id2 =764 where rec_id2 =765;"
        sql_query_run "commit;"
        sql_query_run "update ${pin_user}.bal_grp_bals_t set rec_id=764 where obj_id0 in ( select obj_id0 from bal_grp_bals_t where rec_id=765 minus select obj_id0 from bal_grp_bals_t where rec_id =764 and obj_id0 in ( select obj_id0 from bal_grp_bals_t where rec_id=765)) and rec_id=765;"
        sql_query_run "delete from ${pin_user}.bal_grp_bals_t where rec_id=765;"
        sql_query_run "commit;"

echo "----- PostRun endtime : `date '+%s'` ------"  | tee -a $log_file



edate=`date +%Y%m%d%H%M%S`
end_time=`date '+%s'`
echo "Loading completed--------------------------------" | tee -a $log_file
echo "--------------Loading process end_time: $edate ----------------"  | tee -a $log_file
letancy_time="$((end_time-start_time))"

echo "total time taken in loading seconds $letancy_time" | tee -a $log_file
