-- Old Profiler name is Alter table rocai_analysis3.prof_sms_basic_msisdn_daily_cco rename to rocai_analysis3.prof_sms_basic_msisdn_daily_cco_bkp_20220906;

-- Changes made on 14/sep/2023 with addition of receivable_charge columns(3 to 5)

USE rocai_analysis3;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS rocai_analysis3.tmp_prof_sms_basic_msisdn_daily_cco;
CREATE TABLE rocai_analysis3.tmp_prof_sms_basic_msisdn_daily_cco stored AS orc AS
SELECT
(CASE WHEN substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') THEN calling_nbr_norm ELSE billing_nbr_norm END) AS calling_nbr,
1 AS sms_flag,
count(DISTINCT sms_id) AS sms_total_msgs,
max(CASE WHEN cur_price_plan_code IN ('414', '415')
AND (plan_code NOT LIKE '%Spouses%' AND plan_code NOT LIKE '%Staff%' AND plan_code NOT LIKE '%Merchant%' AND plan_code NOT LIKE '%Dealer%')
AND (billing_nbr LIKE '26377%'OR billing_nbr LIKE '26378%') THEN 'Hybrid'
WHEN cur_price_plan_code NOT IN ('414', '415')
AND (plan_code NOT LIKE '%Spouses%'AND plan_code NOT LIKE '%Staff%' AND plan_code NOT LIKE '%Merchant%' AND plan_code NOT LIKE '%Dealer%')
AND (billing_nbr LIKE '26377%' OR billing_nbr LIKE '26378%' ) THEN nvl(prepay_flag, 'Prepaid')
WHEN (plan_code LIKE '%Spouses%' OR plan_code LIKE '%Staff%' OR plan_code LIKE '%Merchant%'OR plan_code LIKE '%Dealer%') AND (billing_nbr LIKE '26377%'
OR billing_nbr LIKE '26378%') THEN 'Staff & Spouse' ELSE 'Others' END) AS plan_code,

--ZWL MA 
sum(nvl(case when acct_res_id_1=1 then receivable_charge_1 else '0' end,0)
+nvl(case when acct_res_id_2=1 then receivable_charge_2 else '0' end,0)
+nvl(case when attribute_1209=1 then attribute_1210 else '0' end,0)
+nvl(case when attribute_1166=1 then attribute_1168 else '0' end,0)
+nvl(case when attribute_1167=1 then attribute_1169 else '0' end,0))/1000000 as acct_main_balance_chg_zwl,

--USD MA
sum(nvl(case when acct_res_id_1=398 then receivable_charge_1 else '0' end,0)
+nvl(case when acct_res_id_2=398 then receivable_charge_2 else '0' end,0)
+nvl(case when attribute_1209=398 then attribute_1210 else '0' end,0)
+nvl(case when attribute_1166=398 then attribute_1168 else '0' end,0)
+nvl(case when attribute_1167=398 then attribute_1169 else '0' end,0))/1000000 as acct_main_balance_chg_usd,

--ZWL ACS
sum(nvl(case when (acct_res_id_1=2 or acct_res_id_1=10) then receivable_charge_1 else '0' end,0)
+nvl(case when (acct_res_id_2=2 or acct_res_id_2=10) then receivable_charge_2 else '0' end,0)
+nvl(case when (attribute_1209=2 or attribute_1209=10) then attribute_1210 else '0' end,0)
+nvl(case when (attribute_1166=2 or attribute_1166=10) then attribute_1168 else '0' end,0)
+nvl(case when (attribute_1167=2 or attribute_1167=10) then attribute_1169 else '0' end,0))/1000000 as acct_Debit_Balance_chg,

--USD ACS
sum(nvl(case when (acct_res_id_1=480) then receivable_charge_1 else '0' end,0)
+nvl(case when (acct_res_id_2=480) then receivable_charge_2 else '0' end,0)
+nvl(case when (attribute_1209=480) then attribute_1210 else '0' end,0)
+nvl(case when (attribute_1166=480) then attribute_1168 else '0' end,0)
+nvl(case when (attribute_1167=480) then attribute_1169 else '0' end,0))/1000000 as acct_Debit_Balance_chg_usd,

sum(nvl(case when substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_1=1 then receivable_charge_1 else '0' end,0)
+nvl(case when substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') and acct_res_id_2=1 then receivable_charge_2 else '0' end,0)
+nvl(case when substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') and attribute_1209=1 then attribute_1210 else '0' end,0)
+nvl(case when substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') and attribute_1166=1 then attribute_1168 else '0' end,0)
+nvl(case when substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') and attribute_1167=1 then attribute_1169 else '0' end,0)
)/1000000 as acct_main_balance_chg_onnet_zwl,

-- Off Net - Acct Main Balance - ZWL
sum(nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_1=1 then receivable_charge_1 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_2=1 then receivable_charge_2 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND attribute_1209=1 then attribute_1210 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND attribute_1166=1 then attribute_1168 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND attribute_1167=1 then attribute_1169 else '0' end,0)
)/1000000 as acct_main_balance_chg_offnet_zwl,

-- On Net - Acct Main Balance - USD
sum(nvl(case when substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') AND acct_res_id_1=398 then receivable_charge_1 else '0' end,0)
+nvl(case when substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') AND acct_res_id_2=398 then receivable_charge_2 else '0' end,0)
+nvl(case when substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') AND attribute_1209=398 then attribute_1210 else '0' end,0)
+nvl(case when substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') AND attribute_1166=398 then attribute_1168 else '0' end,0)
+nvl(case when substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') AND attribute_1167=398 then attribute_1169 else '0' end,0)
)/1000000 as acct_main_balance_chg_onnet_usd,

-- Off Net - Acct Main Balance - USD
sum(nvl(case when substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_1=398 then receivable_charge_1 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND acct_res_id_2=398 then receivable_charge_2 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND attribute_1209=398 then attribute_1210 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND attribute_1166=398 then attribute_1168 else '0' end,0)
+nvl(case when substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND attribute_1167=398 then attribute_1169 else '0' end,0)
)/1000000 as acct_main_balance_chg_offnet_usd,

-- On Net - Acct Debit Balance - ZWL
sum(nvl(case when (acct_res_id_1=2 or acct_res_id_1=10) AND substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') then receivable_charge_1 else '0' end,0)
+nvl(case when (acct_res_id_2=2 or acct_res_id_2=10) AND substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') then receivable_charge_2 else '0' end,0)
+nvl(case when (attribute_1209=2 or attribute_1209=10) AND substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') then attribute_1210 else '0' end,0)
+nvl(case when (attribute_1166=2 or attribute_1166=10) AND substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') then attribute_1168 else '0' end,0)
+nvl(case when (attribute_1167=2 or attribute_1167=10) AND substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') then attribute_1169 else '0' end,0)
)/1000000 as acct_Debit_Balance_chg_onnet_zwl,

-- Off Net - Acct Debit Balance - ZWL
sum(nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_1=2 or acct_res_id_1=10) then receivable_charge_1 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_2=2 or acct_res_id_2=10) then receivable_charge_2 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (attribute_1209=2 or attribute_1209=10) then attribute_1210 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (attribute_1166=2 or attribute_1166=10) then attribute_1168 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (attribute_1167=2 or attribute_1167=10) then attribute_1169 else '0' end,0)
)/1000000 as acct_Debit_Balance_chg_offnet_zwl, 

-- On Net - Acct Debit Balance - ZWL - ACS USD
sum(nvl(case when (acct_res_id_1=480) AND substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') then receivable_charge_1 else '0' end,0)
+nvl(case when (acct_res_id_2=480) AND substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') then receivable_charge_2 else '0' end,0)
+nvl(case when (attribute_1209=480) AND substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') then attribute_1210 else '0' end,0)
+nvl(case when (attribute_1166=480) AND substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') then attribute_1168 else '0' end,0)
+nvl(case when (attribute_1167=480) AND substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') then attribute_1169 else '0' end,0)
)/1000000 as acct_Debit_Balance_chg_onnet_usd,

-- Off Net - Acct Debit Balance - ZWL - ACS USD
sum(nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_1=480) then receivable_charge_1 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (acct_res_id_2=480) then receivable_charge_2 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (attribute_1209=480) then attribute_1210 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (attribute_1166=480) then attribute_1168 else '0' end,0)
+nvl(CASE WHEN substr(called_nbr_norm, 1, 4) IN ('2631','2632','2633','2634','2635','2636','2637','2638','2639') AND substr(called_nbr_norm, 1, 5) NOT IN ('26377', '26378') AND length(called_nbr_norm) = 12 AND (attribute_1167=480) then attribute_1169 else '0' end,0)
)/1000000 as acct_Debit_Balance_chg_offnet_usd, 


part_date
FROM rocai_analysis.u_z_cvbs_rated_sms_orc WHERE part_date = '${hiveconf:cur_date}'
GROUP BY (CASE WHEN substr(calling_nbr_norm, 1, 5) IN ('26377', '26378') THEN calling_nbr_norm ELSE billing_nbr_norm END),part_date;

ALTER TABLE rocai_analysis3.prof_sms_basic_msisdn_daily_cco DROP IF EXISTS PARTITION(part_date = '${hiveconf:cur_date}');
INSERT INTO TABLE rocai_analysis3.prof_sms_basic_msisdn_daily_cco PARTITION (part_date)
SELECT calling_nbr,sms_flag,sms_total_msgs,plan_code,
(nvl(acct_main_balance_chg_zwl, 0) +(nvl(acct_main_balance_chg_usd, 0) * rtgs_interbank_exchange)) AS acct_main_balance_chg,
(acct_Debit_Balance_chg_usd * rtgs_interbank_exchange + acct_Debit_Balance_chg) as acct_Debit_Balance_chg,
acct_main_balance_chg_zwl,
(nvl(acct_main_balance_chg_usd, 0) * rtgs_interbank_exchange) as acct_main_balance_chg_usd,
nvl(acct_main_balance_chg_onnet_zwl,0) as acct_main_balance_chg_onnet_zwl,
nvl(acct_main_balance_chg_offnet_zwl,0) as acct_main_balance_chg_offnet_zwl,
nvl(acct_main_balance_chg_onnet_usd, 0) *  rtgs_interbank_exchange as acct_main_balance_chg_onnet_usd,
nvl(acct_main_balance_chg_offnet_usd, 0) * rtgs_interbank_exchange as acct_main_balance_chg_offnet_usd,
nvl(acct_Debit_Balance_chg,0) as acct_Debit_Balance_chg_zwl,
(nvl(acct_Debit_Balance_chg_usd,0) * rtgs_interbank_exchange) as acct_Debit_Balance_chg_usd,
nvl(acct_Debit_Balance_chg_onnet_zwl,0) as acct_Debit_Balance_chg_onnet_zwl,
nvl(acct_Debit_Balance_chg_offnet_zwl,0) as acct_Debit_Balance_chg_offnet_zwl,
nvl(acct_Debit_Balance_chg_onnet_usd,0) * rtgs_interbank_exchange AS acct_Debit_Balance_chg_onnet_usd,
nvl(acct_Debit_Balance_chg_offnet_usd,0) * rtgs_interbank_exchange AS acct_Debit_Balance_chg_offnet_usd,
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
FROM rocai_analysis3.tmp_prof_sms_basic_msisdn_daily_cco a
LEFT JOIN (SELECT * FROM rocai_analysis2.rtgs_usd_conversion_rate
WHERE part_date = '${hiveconf:cur_date}') AS b ON a.part_date = b.part_date
WHERE a.part_date = '${hiveconf:cur_date}';

