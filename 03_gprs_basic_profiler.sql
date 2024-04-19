-- Old Profiler Backup in Alter table rocai_analysis3.prof_gprs_basic_msisdn_daily_cco rename to rocai_analysis3.prof_gprs_basic_msisdn_daily_cco_backup_20220907;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS rocai_analysis2.tmp_prof_gprs_basic_msisdn_daily_cco;

CREATE TABLE rocai_analysis2.tmp_prof_gprs_basic_msisdn_daily_cco stored AS orc AS
SELECT ( CASE WHEN substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') THEN calling_nbr_norm ELSE billing_nbr_norm END) AS calling_nbr,
1 AS data_flag,
count(DISTINCT gpr_id) AS data_total_sessions,
sum(nvl(byte_total, 0)) /(1024 * 1024 * 1024) AS data_volume_gb,
max(CASE WHEN cur_price_plan_code IN ('414', '415') AND (plan_code NOT LIKE '%Spouses%' AND plan_code NOT LIKE '%Staff%' AND plan_code NOT LIKE '%Merchant%'
AND plan_code NOT LIKE '%Dealer%') AND substr(billing_nbr, 1, 5) IN ('26377', '26378') THEN 'Hybrid'
WHEN cur_price_plan_code NOT IN ('414', '415') AND (plan_code NOT LIKE '%Spouses%' AND plan_code NOT LIKE '%Staff%'AND plan_code NOT LIKE '%Merchant%' AND plan_code NOT LIKE '%Dealer%') AND substr(billing_nbr, 1, 5) IN ('26377', '26378') THEN nvl(prepay_flag, 'Prepaid')
WHEN (plan_code LIKE '%Spouses%' OR plan_code LIKE '%Staff%'OR plan_code LIKE '%Merchant%' OR plan_code LIKE '%Dealer%')
AND substr(billing_nbr, 1, 5) IN ('26377', '26378') THEN 'Staff & Spouse' ELSE 'Others' END) AS plan_code,

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

sum(
 nvl(case when (acct_res_id_1=480) then receivable_charge_1 else '0' end,0)
+nvl(case when (acct_res_id_2=480) then receivable_charge_2 else '0' end,0)
+nvl(case when (acct_res_id_3=480) then receivable_charge_3 else '0' end,0)
+nvl(case when (acct_res_id_4=480) then receivable_charge_4 else '0' end,0)
+nvl(case when (acct_res_id_5=480) then receivable_charge_5 else '0' end,0)
+nvl(case when (acct_res_id_6=480) then receivable_charge_6 else '0' end,0)
+nvl(case when (acct_res_id_7=480) then receivable_charge_7 else '0' end,0)
+nvl(case when (acct_res_id_8=480) then receivable_charge_8 else '0' end,0)
+nvl(case when (acct_res_id_9=480) then receivable_charge_9 else '0' end,0))/1000000 as acct_Debit_Balance_chg_usd,

sum(nvl(case when (acct_res_id_1 not in ('1','2','10','398','480')) then receivable_charge_1 else '0' end,0)
+nvl(case when (acct_res_id_2 not in ('1','2','10','398','480')) then receivable_charge_2 else '0' end,0)
+nvl(case when (acct_res_id_3 not in ('1','2','10','398','480')) then receivable_charge_3 else '0' end,0)
+nvl(case when (acct_res_id_4 not in ('1','2','10','398','480')) then receivable_charge_4 else '0' end,0)
+nvl(case when (acct_res_id_5 not in ('1','2','10','398','480')) then receivable_charge_5 else '0' end,0)
+nvl(case when (acct_res_id_6 not in ('1','2','10','398','480')) then receivable_charge_6 else '0' end,0)
+nvl(case when (acct_res_id_7 not in ('1','2','10','398','480')) then receivable_charge_7 else '0' end,0)
+nvl(case when (acct_res_id_8 not in ('1','2','10','398','480')) then receivable_charge_8 else '0' end,0)
+nvl(case when (acct_res_id_9 not in ('1','2','10','398','480')) then receivable_charge_9 else '0' end,0)) as bundle_usage,
part_date
FROM rocai_analysis.u_z_cvbs_rated_gprs_orc
WHERE part_date = '${hiveconf:cur_date}'
GROUP BY (CASE WHEN substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') THEN calling_nbr_norm ELSE billing_nbr_norm END),part_date;


ALTER TABLE rocai_analysis3.prof_gprs_basic_msisdn_daily_cco DROP IF EXISTS PARTITION(part_date = '${hiveconf:cur_date}');

INSERT INTO TABLE rocai_analysis3.prof_gprs_basic_msisdn_daily_cco PARTITION (part_date)
SELECT
calling_nbr,
data_flag,
data_total_sessions,
data_volume_gb,
plan_code,
(acct_main_balance_chg_zwl + (acct_main_balance_chg_usd * rtgs_interbank_exchange)) AS acct_main_balance_chg,
(acct_Debit_Balance_chg_usd * rtgs_interbank_exchange + acct_Debit_Balance_chg) as acct_Debit_Balance_chg,
nvl(acct_main_balance_chg_zwl,0) as acct_main_balance_chg_zwl,
nvl(acct_main_balance_chg_usd, 0) * rtgs_interbank_exchange AS acct_main_balance_chg_usd,
nvl(acct_Debit_Balance_chg,0) AS acct_debit_balance_chg_zwl,
nvl(acct_Debit_Balance_chg_usd,0) * rtgs_interbank_exchange AS acct_debit_balance_chg_usd,
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
rocai_analysis2.tmp_prof_gprs_basic_msisdn_daily_cco a
LEFT JOIN (SELECT * FROM rocai_analysis2.rtgs_usd_conversion_rate
WHERE part_date = '${hiveconf:cur_date}')c ON a.part_date = c.part_date
WHERE a.part_date = '${hiveconf:cur_date}' and (a.acct_main_balance_chg_zwl>0 or a.acct_main_balance_chg_usd>0 or a.acct_debit_balance_chg>0 or a.bundle_usage>0 or a.acct_debit_balance_chg_usd>0);

