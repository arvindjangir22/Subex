use rocai_analysis3;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

alter table rocai_analysis3.prof_acs_commission_basic_msisdn_daily_cco drop if exists partition(part_date='${hiveconf:cur_date}');

Insert into rocai_analysis3.prof_acs_commission_basic_msisdn_daily_cco Partition (part_date)
Select msisdn_new as calling_nbr,
max(nvl(reserve2,'Prepaid')) as plan_code,
sum(transaction_amt)/1000000 as acs_commission_chg, part_date
From rocai_analysis.u_z_acc_bal_alter_without_dups_msisdn_orc
Where acct_book_type = 'ACS Commission' 
and part_date='${hiveconf:cur_date}'
group by msisdn_new,part_date;