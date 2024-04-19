-- Old Profiler Table name Alter table rocai_analysis3.prof_voice_basic_msisdn_daily_cco rename to rocai_analysis3.prof_voice_basic_msisdn_daily_cco_backup_20220907;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


DROP TABLE IF EXISTS rocai_analysis2.tmp_prof_voice_basic_msisdn_daily_cco;
CREATE TABLE rocai_analysis2.tmp_prof_voice_basic_msisdn_daily_cco stored AS orc AS 
WITH tmp AS (
SELECT '${hiveconf:cur_date}' AS tmp_part_date,
zwl_headline_onnet_voice_tariff
FROM rocai_analysis2.headline_tariff_inc_usd
WHERE start_date <= '${hiveconf:cur_date}' AND end_date >= '${hiveconf:cur_date}')
SELECT
calling_nbr,
voice_flag,
voice_total_calls,
voice_usage_mins,
nvl((unbilled_voice_usage_charged_ids /(1000000 * zwl_headline_onnet_voice_tariff)),0) + nvl(unbilled_voice_usage_mins, 0) AS unbilled_voice_usage_mins,
plan_code,
acct_main_balance_chg_zwl,
acct_main_balance_chg_usd,
acct_Debit_Balance_chg,
acct_Debit_Balance_chg_usd,
acct_main_balance_chg_onnet_zwl,
acct_main_balance_chg_offnet_zwl,
acct_main_balance_chg_onnet_usd,
acct_main_balance_chg_offnet_usd,
acct_debit_balance_chg_onnet_zwl,
acct_debit_balance_chg_offnet_zwl,
acct_debit_balance_chg_onnet_usd,
acct_debit_balance_chg_offnet_usd,
part_date
FROM
(
SELECT
(
CASE
WHEN substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') THEN calling_nbr_norm
ELSE billing_nbr_norm
END
) AS calling_nbr,
1 AS voice_flag,
count(DISTINCT voi_id) AS voice_total_calls,
sum(nvl(duration, 0)) / 60 AS voice_usage_mins,
sum(nvl(case when acct_res_id_1 in ('131','372') then receivable_charge_1 else '0' end,0)+
nvl(case when acct_res_id_2 in ('131','372') then receivable_charge_2 else '0' end,0)+
nvl(case when acct_res_id_3 in ('131','372') then receivable_charge_3 else '0' end,0)+
nvl(case when acct_res_id_4 in ('131','372') then receivable_charge_4 else '0' end,0)+
nvl(case when acct_res_id_5 in ('131','372') then receivable_charge_5 else '0' end,0)+
nvl(case when acct_res_id_6 in ('131','372') then receivable_charge_6 else '0' end,0)+
nvl(case when acct_res_id_7 in ('131','372') then receivable_charge_7 else '0' end,0)+
nvl(case when acct_res_id_8 in ('131','372') then receivable_charge_8 else '0' end,0)+
nvl(case when acct_res_id_9 in ('131','372') then receivable_charge_9 else '0' end,0)) as unbilled_voice_usage_charged_ids,
b.zwl_headline_onnet_voice_tariff,
sum(nvl(case when(acct_res_id_1 not in ('1','2','10','54','70','74','75','76','77','78','131','302','305','308','311','314','372','386','378','385','377','384','376','398','423','425','424','426','421','422','427','428','429','431','430','432','456','457','452','453','448','449','480','489','486','485','484','74','77','78','397','396','279')) then cast(receivable_charge_1 as int) else '0' end,0)+
nvl(case when(acct_res_id_2 not in ('1','2','10','54','70','74','75','76','77','78','131','302','305','308','311','314','372','386','378','385','377','384','376','398','423','425','424','426','421','422','427','428','429','431','430','432','456','457','452','453','448','449','480','489','486','485','484','74','77','78','397','396','279')) then cast(receivable_charge_2 as int) else '0' end,0)+
nvl(case when(acct_res_id_3 not in ('1','2','10','54','70','74','75','76','77','78','131','302','305','308','311','314','372','386','378','385','377','384','376','398','423','425','424','426','421','422','427','428','429','431','430','432','456','457','452','453','448','449','480','489','486','485','484','74','77','78','397','396','279')) then cast(receivable_charge_3 as int) else '0' end,0)+
nvl(case when(acct_res_id_4 not in ('1','2','10','54','70','74','75','76','77','78','131','302','305','308','311','314','372','386','378','385','377','384','376','398','423','425','424','426','421','422','427','428','429','431','430','432','456','457','452','453','448','449','480','489','486','485','484','74','77','78','397','396','279')) then cast(receivable_charge_4 as int) else '0' end,0)+
nvl(case when(acct_res_id_5 not in ('1','2','10','54','70','74','75','76','77','78','131','302','305','308','311','314','372','386','378','385','377','384','376','398','423','425','424','426','421','422','427','428','429','431','430','432','456','457','452','453','448','449','480','489','486','485','484','74','77','78','397','396','279')) then cast(receivable_charge_5 as int) else '0' end,0)+
nvl(case when(acct_res_id_6 not in ('1','2','10','54','70','74','75','76','77','78','131','302','305','308','311','314','372','386','378','385','377','384','376','398','423','425','424','426','421','422','427','428','429','431','430','432','456','457','452','453','448','449','480','489','486','485','484','74','77','78','397','396','279')) then cast(receivable_charge_6 as int) else '0' end,0)+
nvl(case when(acct_res_id_7 not in ('1','2','10','54','70','74','75','76','77','78','131','302','305','308','311','314','372','386','378','385','377','384','376','398','423','425','424','426','421','422','427','428','429','431','430','432','456','457','452','453','448','449','480','489','486','485','484','74','77','78','397','396','279')) then cast(receivable_charge_7 as int) else '0' end,0)+
nvl(case when(acct_res_id_8 not in ('1','2','10','54','70','74','75','76','77','78','131','302','305','308','311','314','372','386','378','385','377','384','376','398','423','425','424','426','421','422','427','428','429','431','430','432','456','457','452','453','448','449','480','489','486','485','484','74','77','78','397','396','279')) then cast(receivable_charge_8 as int) else '0' end,0)+
nvl(case when(acct_res_id_9 not in ('1','2','10','54','70','74','75','76','77','78','131','302','305','308','311','314','372','386','378','385','377','384','376','398','423','425','424','426','421','422','427','428','429','431','430','432','456','457','452','453','448','449','480','489','486','485','484','74','77','78','397','396','279')) then cast(receivable_charge_9 as int) else '0' end,0))/60 as unbilled_voice_usage_mins,
max(
CASE WHEN cur_price_plan_code IN ('414', '415') AND (plan_code NOT LIKE '%Spouses%' AND plan_code NOT LIKE '%Staff%' AND plan_code NOT LIKE '%Merchant%'
 AND plan_code NOT LIKE '%Dealer%') AND (billing_nbr LIKE '26377%' OR billing_nbr LIKE '26378%') THEN 'Hybrid'
WHEN cur_price_plan_code NOT IN ('414', '415') AND (plan_code NOT LIKE '%Spouses%' AND plan_code NOT LIKE '%Staff%' AND plan_code NOT LIKE '%Merchant%'
 AND plan_code NOT LIKE '%Dealer%') AND (billing_nbr LIKE '26377%' OR billing_nbr LIKE '26378%') THEN nvl(prepay_flag, 'Prepaid')
WHEN ( plan_code LIKE '%Spouses%' OR plan_code LIKE '%Staff%' OR plan_code LIKE '%Merchant%' OR plan_code LIKE '%Dealer%') AND (billing_nbr LIKE '26377%' OR billing_nbr LIKE '26378%') THEN 'Staff & Spouse' ELSE 'Others' END) AS plan_code,

sum(nvl(case when (acct_res_id_1=1) then receivable_charge_1 else '0' end,0)
+nvl(case when (acct_res_id_2=1) then receivable_charge_2 else '0' end,0)
+nvl(case when (acct_res_id_3=1) then receivable_charge_3 else '0' end,0)
+nvl(case when (acct_res_id_4=1) then receivable_charge_4 else '0' end,0)
+nvl(case when (acct_res_id_5=1) then receivable_charge_5 else '0' end,0)
+nvl(case when (acct_res_id_6=1) then receivable_charge_6 else '0' end,0)
+nvl(case when (acct_res_id_7=1) then receivable_charge_7 else '0' end,0)
+nvl(case when (acct_res_id_8=1) then receivable_charge_8 else '0' end,0)
+nvl(case when (acct_res_id_9=1) then receivable_charge_9 else '0' end,0))/1000000 as acct_main_balance_chg_zwl,

sum(nvl(case when (acct_res_id_1=398) then receivable_charge_1 else '0' end,0)
+nvl(case when (acct_res_id_2=398) then receivable_charge_2 else '0' end,0)
+nvl(case when (acct_res_id_3=398) then receivable_charge_3 else '0' end,0)
+nvl(case when (acct_res_id_4=398) then receivable_charge_4 else '0' end,0)
+nvl(case when (acct_res_id_5=398) then receivable_charge_5 else '0' end,0)
+nvl(case when (acct_res_id_6=398) then receivable_charge_6 else '0' end,0)
+nvl(case when (acct_res_id_7=398) then receivable_charge_7 else '0' end,0)
+nvl(case when (acct_res_id_8=398) then receivable_charge_8 else '0' end,0)
+nvl(case when (acct_res_id_9=398) then receivable_charge_9 else '0' end,0))/1000000 as acct_main_balance_chg_usd,

sum(nvl(case when (acct_res_id_1=2 or acct_res_id_1=10) then receivable_charge_1 else '0' end,0)
+nvl(case when (acct_res_id_2=2 or acct_res_id_2=10) then receivable_charge_2 else '0' end,0)
+nvl(case when (acct_res_id_3=2 or acct_res_id_3=10) then receivable_charge_3 else '0' end,0)
+nvl(case when (acct_res_id_4=2 or acct_res_id_4=10) then receivable_charge_4 else '0' end,0)
+nvl(case when (acct_res_id_5=2 or acct_res_id_5=10) then receivable_charge_5 else '0' end,0)
+nvl(case when (acct_res_id_6=2 or acct_res_id_6=10) then receivable_charge_6 else '0' end,0)
+nvl(case when (acct_res_id_7=2 or acct_res_id_7=10) then receivable_charge_7 else '0' end,0)
+nvl(case when (acct_res_id_8=2 or acct_res_id_8=10) then receivable_charge_8 else '0' end,0)
+nvl(case when (acct_res_id_9=2 or acct_res_id_9=10) then receivable_charge_9 else '0' end,0))/1000000 as acct_Debit_Balance_chg,


sum(nvl(case when (acct_res_id_1=480) then receivable_charge_1 else '0' end,0)
+nvl(case when (acct_res_id_2=480) then receivable_charge_2 else '0' end,0)
+nvl(case when (acct_res_id_3=480) then receivable_charge_3 else '0' end,0)
+nvl(case when (acct_res_id_4=480) then receivable_charge_4 else '0' end,0)
+nvl(case when (acct_res_id_5=480) then receivable_charge_5 else '0' end,0)
+nvl(case when (acct_res_id_6=480) then receivable_charge_6 else '0' end,0)
+nvl(case when (acct_res_id_7=480) then receivable_charge_7 else '0' end,0)
+nvl(case when (acct_res_id_8=480) then receivable_charge_8 else '0' end,0)
+nvl(case when (acct_res_id_9=480) then receivable_charge_9 else '0' end,0))/1000000 as acct_Debit_Balance_chg_usd,


sum(nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_1=1 then receivable_charge_1 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_2=1 then receivable_charge_2 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_3=1 then receivable_charge_3 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_4=1 then receivable_charge_4 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_5=1 then receivable_charge_5 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_6=1 then receivable_charge_6 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_7=1 then receivable_charge_7 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_8=1 then receivable_charge_8 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_9=1 then receivable_charge_9 else '0' end,0))/ 1000000 as acct_main_balance_chg_onnet_zwl,

sum(nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_1=1 then receivable_charge_1 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_2=1 then receivable_charge_2 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_3=1 then receivable_charge_3 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_4=1 then receivable_charge_4 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_5=1 then receivable_charge_5 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_6=1 then receivable_charge_6 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_7=1 then receivable_charge_7 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_8=1 then receivable_charge_8 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_9=1 then receivable_charge_9 else '0' end,0))/ 1000000 as acct_main_balance_chg_offnet_zwl,

sum(nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_1=398 then receivable_charge_1 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_2=398 then receivable_charge_2 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_3=398 then receivable_charge_3 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_4=398 then receivable_charge_4 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_5=398 then receivable_charge_5 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_6=398 then receivable_charge_6 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_7=398 then receivable_charge_7 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_8=398 then receivable_charge_8 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_9=398 then receivable_charge_9 else '0' end,0))/ 1000000 as acct_main_balance_chg_onnet_usd,

sum(nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_1=398 then receivable_charge_1 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_2=398 then receivable_charge_2 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_3=398 then receivable_charge_3 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_4=398 then receivable_charge_4 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_5=398 then receivable_charge_5 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_6=398 then receivable_charge_6 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_7=398 then receivable_charge_7 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_8=398 then receivable_charge_8 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_9=398 then receivable_charge_9 else '0' end,0))/ 1000000 as acct_main_balance_chg_offnet_usd,

sum(nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and (acct_res_id_1=2 or acct_res_id_1=10) then receivable_charge_1 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and (acct_res_id_2=2 or acct_res_id_2=10) then receivable_charge_2 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and (acct_res_id_3=2 or acct_res_id_3=10) then receivable_charge_3 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and (acct_res_id_4=2 or acct_res_id_4=10) then receivable_charge_4 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and (acct_res_id_5=2 or acct_res_id_5=10) then receivable_charge_5 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and (acct_res_id_6=2 or acct_res_id_6=10) then receivable_charge_6 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and (acct_res_id_7=2 or acct_res_id_7=10) then receivable_charge_7 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and (acct_res_id_8=2 or acct_res_id_8=10) then receivable_charge_8 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and (acct_res_id_9=2 or acct_res_id_9=10) then receivable_charge_9 else '0' end,0))/ 1000000 as acct_debit_balance_chg_onnet_zwl,

sum(nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_1=2 or acct_res_id_1=10) then receivable_charge_1 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_2=2 or acct_res_id_2=10) then receivable_charge_2 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_3=2 or acct_res_id_3=10) then receivable_charge_3 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_4=2 or acct_res_id_4=10) then receivable_charge_4 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_5=2 or acct_res_id_5=10) then receivable_charge_5 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_6=2 or acct_res_id_6=10) then receivable_charge_6 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_7=2 or acct_res_id_7=10) then receivable_charge_7 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_8=2 or acct_res_id_8=10) then receivable_charge_8 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_9=2 or acct_res_id_9=10) then receivable_charge_9 else '0' end,0))/ 1000000 as acct_debit_balance_chg_offnet_zwl,

sum
(nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_1=480 then receivable_charge_1 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_2=480 then receivable_charge_2 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_3=480 then receivable_charge_3 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_4=480 then receivable_charge_4 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_5=480 then receivable_charge_5 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_6=480 then receivable_charge_6 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_7=480 then receivable_charge_7 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_8=480 then receivable_charge_8 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_9=480 then receivable_charge_9 else '0' end,0))/ 1000000 as acct_debit_balance_chg_onnet_usd,

sum
(nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_1=480 then receivable_charge_1 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_2=480 then receivable_charge_2 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_3=480 then receivable_charge_3 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_4=480 then receivable_charge_4 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_5=480 then receivable_charge_5 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_6=480 then receivable_charge_6 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_7=480 then receivable_charge_7 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_8=480 then receivable_charge_8 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_9=480 then receivable_charge_9 else '0' end,0))/ 1000000 as acct_debit_balance_chg_offnet_usd,
part_date
FROM
rocai_analysis.u_z_cvbs_rated_voice_orc a
LEFT JOIN tmp b ON a.part_date = b.tmp_part_date
WHERE
part_date = '${hiveconf:cur_date}'
AND plan_name NOT LIKE '%Test%'
GROUP BY (CASE WHEN substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') THEN calling_nbr_norm ELSE billing_nbr_norm END),
part_date,b.zwl_headline_onnet_voice_tariff)c;




ALTER TABLE rocai_analysis3.prof_voice_basic_msisdn_daily_cco DROP IF EXISTS PARTITION(part_date = '${hiveconf:cur_date}');

INSERT INTO TABLE rocai_analysis3.prof_voice_basic_msisdn_daily_cco PARTITION (part_date)
SELECT
calling_nbr,
voice_flag,
voice_total_calls,
voice_usage_mins,
unbilled_voice_usage_mins,
plan_code,
(nvl(acct_main_balance_chg_zwl, 0) +(nvl(acct_main_balance_chg_usd, 0) * rtgs_interbank_exchange)) AS acct_main_balance_chg,
((nvl(acct_Debit_Balance_chg_usd,0) * rtgs_interbank_exchange) + acct_Debit_Balance_chg) as acct_Debit_Balance_chg,
acct_main_balance_chg_zwl,
(nvl(acct_main_balance_chg_usd, 0) * rtgs_interbank_exchange) as acct_main_balance_chg_usd,
acct_main_balance_chg_onnet_zwl,
acct_main_balance_chg_offnet_zwl,
nvl(acct_main_balance_chg_onnet_usd,0) * rtgs_interbank_exchange as acct_main_balance_chg_onnet_usd,
nvl(acct_main_balance_chg_offnet_usd,0) * rtgs_interbank_exchange as acct_main_balance_chg_offnet_usd,
acct_Debit_Balance_chg as acct_debit_balance_chg_zwl,
(nvl(acct_Debit_Balance_chg_usd,0) * rtgs_interbank_exchange) as acct_debit_balance_chg_usd,
acct_debit_balance_chg_onnet_zwl,
acct_debit_balance_chg_offnet_zwl,
nvl(acct_debit_balance_chg_onnet_usd,0) * rtgs_interbank_exchange AS acct_debit_balance_chg_onnet_usd,
nvl(acct_debit_balance_chg_offnet_usd,0) * rtgs_interbank_exchange as acct_debit_balance_chg_offnet_usd,
'0' AS dummy_1,
'0' AS dummy_2,
'0' AS dummy_3,
'0' AS dummy_4,
'0' AS dummy_5,
'0' AS dummy_6,
'0' AS dummy_7,
'0' AS dummy_8,
'0' AS dummy_9,
'0' AS dummy_10,
a.part_date
FROM
rocai_analysis2.tmp_prof_voice_basic_msisdn_daily_cco AS a
LEFT JOIN (SELECT * FROM rocai_analysis2.rtgs_usd_conversion_rate WHERE part_date = '${hiveconf:cur_date}') AS b ON a.part_date = b.part_date;


