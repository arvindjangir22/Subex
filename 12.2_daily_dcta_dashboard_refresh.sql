use rocai_analysis3;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
set hive.support.quoted.identifiers=none;

Select 'Current Date: ', '${hiveconf:cur_date}';
Select 'Prev. 30 Days: ', '${hiveconf:prev_30_days}';
Select 'Prev. Month: ', '${hiveconf:prev_month}';
Select 'Prev. Month Date: ', '${hiveconf:prev_month_date}';
Select 'Prev. 2 Month: ', '${hiveconf:prev_2_month}';

--new mtd definition 

DROP TABLE IF EXISTS rocai_analysis3.temp_trad_dcta_mtd_actual_counts_dashboard_backend;

CREATE TABLE rocai_analysis3.temp_trad_dcta_mtd_actual_counts_dashboard_backend stored AS orc AS
SELECT
"Actual" AS mtd_period,
calling_nbr,
part_date,
plan_code,
MAX(nvl(data_flag, 0)) AS data_flag,
MAX(nvl(voice_flag, 0)) AS voice_flag,
MAX(nvl(sms_flag, 0)) AS sms_flag,
SUM(
(nvl(bjc1_1_amt, 0)) * 0.309 + (nvl(bjc10_amt, 0)) * 0.175 + (nvl(bjc2_amt, 0)) * 0.19 + (nvl(bjc20_amt, 0)) * 0.1875 + (nvl(bjc5_amt, 0)) * 0.194 + (nvl(bjv1_amt, 0)) * 0.55 + (nvl(bjv10_amt, 0)) * 0.55 + (nvl(bjv2_amt, 0)) * 0.55 + (nvl(bjv20_amt, 0)) * 0.55 + (nvl(bjv5_amt, 0)) * 0.55 + (nvl(data_daily_bundle_1gb_2_amt, 0)) * 1 + (nvl(data_daily_bundle_250mb_1_amt, 0)) * 1 + (nvl(data_daily_bundle_2gb_3_amt, 0)) * 1 + (nvl(data_daily_bundle_50mb_05c_amt, 0)) * 1 + (nvl(dd_mth_100mb_amt, 0)) * 1 + (nvl(dd_mth_10mb_amt, 0)) * 1 + (nvl(dd_mth_1500mb_amt, 0)) * 0.96 + (nvl(dd_mth_200mb_amt, 0)) * 1 + (nvl(dd_mth_2500mb_amt, 0)) * 0.975 + (nvl(dd_mth_25mb_amt, 0)) * 1 + (nvl(dd_mth_400mb_amt, 0)) * 1 + (nvl(dd_mth_850mb_amt, 0)) * 1 + (nvl(dd_wifi_1_2gb_amt, 0)) * 0.97 + (nvl(dd_wifi_150mb_amt, 0)) * 0.96 + (nvl(dd_wifi_25gb_amt, 0)) * 1 + (nvl(dd_wifi_3_5gb_amt, 0)) * 0.96 + (nvl(dd_wifi_500mb_amt, 0)) * 0.96 + (nvl(dd_wk_120mb_amt, 0)) * 1 + (nvl(dd_wk_15mb_amt, 0)) * 1 + (nvl(dd_wk_300mb_amt, 0)) * 1 + (nvl(dd_wk_35mb_amt, 0)) * 1 + (nvl(dd_wk_650mb_amt, 0)) * 0.5 + (nvl(dmb_1_5gb_35_amt, 0)) * 1 + (nvl(dmb_10mb_1_amt, 0)) * 0.73 + (nvl(dmb_2_5gb_50_amt, 0)) * 1 + (nvl(dmb_300mb_10_amt, 0)) * 1 + (nvl(dmb_5mb_0_5_amt, 0)) * 0.77 + (nvl(dmb_750mb_20_amt, 0)) * 1 + (nvl(dmb_75mb_3_amt, 0)) * 0.8 + (nvl(facebook_daily_bundle_amt, 0)) * 1 + (nvl(facebook_monthly_bundle_amt, 0)) * 1 + (nvl(facebook_weekly_bundle_amt, 0)) * 1 + (nvl(private_wifi_50gb_75_amt, 0)) * 1 + (nvl(private_wifi_75gb_100_amt, 0)) * 1 + (nvl(social_fw_lb_30mb_amt, 0)) * 0.95 + (nvl(social_fw_lb_80mb_amt, 0)) * 0.95 + (nvl(social_fw_mb_170mb_amt, 0)) * 1 + (nvl(social_fw_mb_300mb_amt, 0)) * 1 + (nvl(video_bundle_10gb_10_amt, 0)) * 0.8 + (nvl(video_bundle_35gb_35_amt, 0)) * 1 + (nvl(whatsapp_daily_bundle_amt, 0)) * 1 + (nvl(whatsapp_monthly_bundle_amt, 0)) * 1 + (nvl(whatsapp_weekly_bundle_amt, 0)) * 1 + (nvl(facebook_1day_20mb_amt, 0)) * 1 + (nvl(whatsapp_1day_20mb_amt, 0)) * 1 + (nvl(instagram_daily_balance_20mb_amt, 0)) * 1 + (nvl(instagram_daily_balance_45mb_amt, 0)) * 1 + (nvl(instagram_weekly_balance_140mb_amt, 0)) * 1 + (nvl(instagram_monthly_balance_400mb_amt, 0)) * 1 + (nvl(twitter_daily_balance_20mb_amt, 0)) * 1 + (nvl(twitter_daily_balance_45mb_amt, 0)) * 1 + (nvl(twitter_weekly_balance_140mb_amt, 0)) * 1 + (nvl(twitter_monthly_balance_400mb_amt, 0)) * 1 + (nvl(whatsapp_weekly_65mb_amt, 0)) * 1 + (nvl(whatsapp_monthly_240mb_amt, 0)) * 1 + (nvl(data_daily_bundle_340_amt, 0)) * 1 + (nvl(sasai_daily_bundle_343_amt, 0)) * 1 + (nvl(sasai_daily_bundle_344_amt, 0)) * 1 + (nvl(sasai_weekly_bundle_345_amt, 0)) * 1 + (nvl(sasai_weekly_bundle_346_amt, 0)) * 1 + (nvl(sasai_monthly_bundle_347_amt, 0)) * 1 + (nvl(sasai_monthly_bundle_348_amt, 0)) * 1 + (nvl(dummy_1_amt, 0)) * 1 + (nvl(dummy_2_amt, 0)) * 1 + (nvl(dummy_3_amt, 0)) * 1 + (nvl(dummy_4_amt, 0)) * 1 + (nvl(dummy_5_amt, 0)) * 1 + (nvl(dummy_6_amt, 0)) * 1 + (nvl(dummy_7_amt, 0)) * 1 + (nvl(dummy_8_amt, 0)) * 1 + (nvl(dummy_9_amt, 0)) * 1 + (nvl(dummy_10_amt, 0)) * 1 + (nvl(dummy_11_amt, 0)) * 1 + (nvl(dummy_12_amt, 0)) * 1 + (nvl(dummy_13_amt, 0)) * 1 + (nvl(dummy_14_amt, 0)) * 1 + (nvl(dummy_15_amt, 0)) * 1 + (nvl(dummy_16_amt, 0)) * 0.95 + (nvl(k_iflix_3days_3_amt, 0)) * 1 + (nvl(iflix_1day_200mb_amt, 0)) * 1 + (nvl(boj_voice_amt, 0)) * 1 + (nvl(boj_data_amt, 0)) * 1 + (nvl(boj_sms_amt, 0)) * 0.95 
) AS data_bundle_revenue,
SUM(
(nvl(voice_boj_1day_amt, 0)) * 1 + (nvl(voice_boj_2day_amt, 0)) * 1 + (nvl(voice_boj_3day_amt, 0)) * 1 + (nvl(voice_boj_4day_amt, 0)) * 1 + (nvl(voice_boj_5day_amt, 0)) * 1 + (nvl(dummy_14_amt, 0)) * 1 + (nvl(k_iflix_mb_15_amt, 0)) * 1 + (nvl(k_iflix_db_1_amt, 0)) * 1 + (nvl(k_iflix_wb_5_amt, 0)) * 1 + (nvl(social_fw_hb_450mb_amt, 0)) * 1 + (nvl(social_fw_hb_600mb_amt, 0)) * 1 + (nvl(social_fw_lb_10mb_amt, 0)) * 1 + (nvl(video_bundle_10gb_10_amt, 0)) * 0.2 + (nvl(dmb_5mb_0_5_amt, 0)) * 0.23 + (nvl(dmb_75mb_3_amt, 0)) * 0.2 + (nvl(dmb_125mb_5_amt, 0)) * 0.8 + (nvl(dmb_10mb_1_amt, 0)) * 0.22 + (nvl(dd_mth_1500mb_amt,0))*0.04 + (nvl(dd_mth_2500mb_amt,0))*0.025+ (nvl(dd_wifi_150mb_amt,0))*0.04+ (nvl(dd_wifi_500mb_amt,0))*0.04+ (nvl(dd_wifi_1_2gb_amt,0))*0.03+ (nvl(dd_wifi_3_5gb_amt,0))*0.04
) AS voice_bundle_revenue,
SUM(
(nvl(bjc1_1_amt, 0)) * 0.091 + (nvl(bjc10_amt, 0)) * 0.04 + (nvl(bjc2_amt, 0)) * 0.1 + (nvl(bjc20_amt, 0)) * 0.025 + (nvl(bjc5_amt, 0)) * 0.06 + (nvl(sms_daily_bundle_15sms_amt, 0)) * 1 + (nvl(sms_daily_bundle_50sms_amt, 0)) * 1 + (nvl(sms_daily_bundle_5sms_amt, 0)) * 1 + (nvl(sms_weekly_bundle_130sms_amt, 0)) * 1 + (nvl(sms_weekly_bundle_250sms_amt, 0)) * 1 + (nvl(sms_weekly_bundle_500sms_amt, 0)) * 1 + (nvl(sms_daily_bundle_341_amt, 0)) * 1 + (nvl(sms_weekly_bundle_342_amt, 0)) * 1 + (nvl(dummy_17_amt, 0)) * 1 + (nvl(dummy_18_amt, 0)) * 1 + (nvl(video_bundle_2gb_2_amt, 0)) * 1 + (nvl(dummy_19_amt, 0)) * 1 + (nvl(dummy_20_amt, 0)) * 1 + (nvl(dmb_125mb_5_amt, 0)) * 0.2 + (nvl(dmb_10mb_1_amt, 0)) * 0.05 + (nvl(dummy_16_amt, 0)) * 0.05 + (nvl(social_fw_lb_30mb_amt, 0)) * 0.05 + (nvl(boj_sms_amt, 0)) * 0.05 + (nvl(social_fw_lb_80mb_amt, 0)) * 0.05
) AS sms_bundle_revenue
FROM
rocai_analysis3.prof_all_info_msisdn_daily_final_cco
WHERE
substr(part_date, 1, 6) = substr('${hiveconf:cur_date}', 1, 6)
AND plan_code NOT IN ('Others', 'Staff & Spouse')
GROUP BY
calling_nbr,
part_date,
plan_code;

DROP TABLE IF EXISTS rocai_analysis3.temp_trad_dcta_mtd_prior_counts_dashboard_backend;

CREATE TABLE rocai_analysis3.temp_trad_dcta_mtd_prior_counts_dashboard_backend stored AS orc AS
SELECT
"Prior Month" AS mtd_period,
calling_nbr,
part_date,
plan_code,
MAX(nvl(data_flag, 0)) AS data_flag,
MAX(nvl(voice_flag, 0)) AS voice_flag,
MAX(nvl(sms_flag, 0)) AS sms_flag,
SUM(
(nvl(bjc1_1_amt, 0)) * 0.309 + (nvl(bjc10_amt, 0)) * 0.175 + (nvl(bjc2_amt, 0)) * 0.19 + (nvl(bjc20_amt, 0)) * 0.1875 + (nvl(bjc5_amt, 0)) * 0.194 + (nvl(bjv1_amt, 0)) * 0.55 + (nvl(bjv10_amt, 0)) * 0.55 + (nvl(bjv2_amt, 0)) * 0.55 + (nvl(bjv20_amt, 0)) * 0.55 + (nvl(bjv5_amt, 0)) * 0.55 + (nvl(data_daily_bundle_1gb_2_amt, 0)) * 1 + (nvl(data_daily_bundle_250mb_1_amt, 0)) * 1 + (nvl(data_daily_bundle_2gb_3_amt, 0)) * 1 + (nvl(data_daily_bundle_50mb_05c_amt, 0)) * 1 + (nvl(dd_mth_100mb_amt, 0)) * 1 + (nvl(dd_mth_10mb_amt, 0)) * 1 + (nvl(dd_mth_1500mb_amt, 0)) * 0.96 + (nvl(dd_mth_200mb_amt, 0)) * 1 + (nvl(dd_mth_2500mb_amt, 0)) * 0.975 + (nvl(dd_mth_25mb_amt, 0)) * 1 + (nvl(dd_mth_400mb_amt, 0)) * 1 + (nvl(dd_mth_850mb_amt, 0)) * 1 + (nvl(dd_wifi_1_2gb_amt, 0)) * 0.97 + (nvl(dd_wifi_150mb_amt, 0)) * 0.96 + (nvl(dd_wifi_25gb_amt, 0)) * 1 + (nvl(dd_wifi_3_5gb_amt, 0)) * 0.96 + (nvl(dd_wifi_500mb_amt, 0)) * 0.96 + (nvl(dd_wk_120mb_amt, 0)) * 1 + (nvl(dd_wk_15mb_amt, 0)) * 1 + (nvl(dd_wk_300mb_amt, 0)) * 1 + (nvl(dd_wk_35mb_amt, 0)) * 1 + (nvl(dd_wk_650mb_amt, 0)) * 0.5 + (nvl(dmb_1_5gb_35_amt, 0)) * 1 + (nvl(dmb_10mb_1_amt, 0)) * 0.73 + (nvl(dmb_2_5gb_50_amt, 0)) * 1 + (nvl(dmb_300mb_10_amt, 0)) * 1 + (nvl(dmb_5mb_0_5_amt, 0)) * 0.77 + (nvl(dmb_750mb_20_amt, 0)) * 1 + (nvl(dmb_75mb_3_amt, 0)) * 0.8 + (nvl(facebook_daily_bundle_amt, 0)) * 1 + (nvl(facebook_monthly_bundle_amt, 0)) * 1 + (nvl(facebook_weekly_bundle_amt, 0)) * 1 + (nvl(private_wifi_50gb_75_amt, 0)) * 1 + (nvl(private_wifi_75gb_100_amt, 0)) * 1 + (nvl(social_fw_lb_30mb_amt, 0)) * 0.95 + (nvl(social_fw_lb_80mb_amt, 0)) * 0.95 + (nvl(social_fw_mb_170mb_amt, 0)) * 1 + (nvl(social_fw_mb_300mb_amt, 0)) * 1 + (nvl(video_bundle_10gb_10_amt, 0)) * 0.8 + (nvl(video_bundle_35gb_35_amt, 0)) * 1 + (nvl(whatsapp_daily_bundle_amt, 0)) * 1 + (nvl(whatsapp_monthly_bundle_amt, 0)) * 1 + (nvl(whatsapp_weekly_bundle_amt, 0)) * 1 + (nvl(facebook_1day_20mb_amt, 0)) * 1 + (nvl(whatsapp_1day_20mb_amt, 0)) * 1 + (nvl(instagram_daily_balance_20mb_amt, 0)) * 1 + (nvl(instagram_daily_balance_45mb_amt, 0)) * 1 + (nvl(instagram_weekly_balance_140mb_amt, 0)) * 1 + (nvl(instagram_monthly_balance_400mb_amt, 0)) * 1 + (nvl(twitter_daily_balance_20mb_amt, 0)) * 1 + (nvl(twitter_daily_balance_45mb_amt, 0)) * 1 + (nvl(twitter_weekly_balance_140mb_amt, 0)) * 1 + (nvl(twitter_monthly_balance_400mb_amt, 0)) * 1 + (nvl(whatsapp_weekly_65mb_amt, 0)) * 1 + (nvl(whatsapp_monthly_240mb_amt, 0)) * 1 + (nvl(data_daily_bundle_340_amt, 0)) * 1 + (nvl(sasai_daily_bundle_343_amt, 0)) * 1 + (nvl(sasai_daily_bundle_344_amt, 0)) * 1 + (nvl(sasai_weekly_bundle_345_amt, 0)) * 1 + (nvl(sasai_weekly_bundle_346_amt, 0)) * 1 + (nvl(sasai_monthly_bundle_347_amt, 0)) * 1 + (nvl(sasai_monthly_bundle_348_amt, 0)) * 1 + (nvl(dummy_1_amt, 0)) * 1 + (nvl(dummy_2_amt, 0)) * 1 + (nvl(dummy_3_amt, 0)) * 1 + (nvl(dummy_4_amt, 0)) * 1 + (nvl(dummy_5_amt, 0)) * 1 + (nvl(dummy_6_amt, 0)) * 1 + (nvl(dummy_7_amt, 0)) * 1 + (nvl(dummy_8_amt, 0)) * 1 + (nvl(dummy_9_amt, 0)) * 1 + (nvl(dummy_10_amt, 0)) * 1 + (nvl(dummy_11_amt, 0)) * 1 + (nvl(dummy_12_amt, 0)) * 1 + (nvl(dummy_13_amt, 0)) * 1 + (nvl(dummy_14_amt, 0)) * 1 + (nvl(dummy_15_amt, 0)) * 1 + (nvl(dummy_16_amt, 0)) * 0.95 + (nvl(k_iflix_3days_3_amt, 0)) * 1 + (nvl(iflix_1day_200mb_amt, 0)) * 1 + (nvl(boj_voice_amt, 0)) * 1 + (nvl(boj_data_amt, 0)) * 1 + (nvl(boj_sms_amt, 0)) * 0.95 
) AS data_bundle_revenue,
SUM(
(nvl(voice_boj_1day_amt, 0)) * 1 + (nvl(voice_boj_2day_amt, 0)) * 1 + (nvl(voice_boj_3day_amt, 0)) * 1 + (nvl(voice_boj_4day_amt, 0)) * 1 + (nvl(voice_boj_5day_amt, 0)) * 1 + (nvl(dummy_14_amt, 0)) * 1 + (nvl(k_iflix_mb_15_amt, 0)) * 1 + (nvl(k_iflix_db_1_amt, 0)) * 1 + (nvl(k_iflix_wb_5_amt, 0)) * 1 + (nvl(social_fw_hb_450mb_amt, 0)) * 1 + (nvl(social_fw_hb_600mb_amt, 0)) * 1 + (nvl(social_fw_lb_10mb_amt, 0)) * 1 + (nvl(video_bundle_10gb_10_amt, 0)) * 0.2 + (nvl(dmb_5mb_0_5_amt, 0)) * 0.23 + (nvl(dmb_75mb_3_amt, 0)) * 0.2 + (nvl(dmb_125mb_5_amt, 0)) * 0.8 + (nvl(dmb_10mb_1_amt, 0)) * 0.22 + (nvl(dd_mth_1500mb_amt,0))*0.04 + (nvl(dd_mth_2500mb_amt,0))*0.025+ (nvl(dd_wifi_150mb_amt,0))*0.04+ (nvl(dd_wifi_500mb_amt,0))*0.04+ (nvl(dd_wifi_1_2gb_amt,0))*0.03+ (nvl(dd_wifi_3_5gb_amt,0))*0.04
) AS voice_bundle_revenue,
SUM(
(nvl(bjc1_1_amt, 0)) * 0.091 + (nvl(bjc10_amt, 0)) * 0.04 + (nvl(bjc2_amt, 0)) * 0.1 + (nvl(bjc20_amt, 0)) * 0.025 + (nvl(bjc5_amt, 0)) * 0.06 + (nvl(sms_daily_bundle_15sms_amt, 0)) * 1 + (nvl(sms_daily_bundle_50sms_amt, 0)) * 1 + (nvl(sms_daily_bundle_5sms_amt, 0)) * 1 + (nvl(sms_weekly_bundle_130sms_amt, 0)) * 1 + (nvl(sms_weekly_bundle_250sms_amt, 0)) * 1 + (nvl(sms_weekly_bundle_500sms_amt, 0)) * 1 + (nvl(sms_daily_bundle_341_amt, 0)) * 1 + (nvl(sms_weekly_bundle_342_amt, 0)) * 1 + (nvl(dummy_17_amt, 0)) * 1 + (nvl(dummy_18_amt, 0)) * 1 + (nvl(video_bundle_2gb_2_amt, 0)) * 1 + (nvl(dummy_19_amt, 0)) * 1 + (nvl(dummy_20_amt, 0)) * 1 + (nvl(dmb_125mb_5_amt, 0)) * 0.2 + (nvl(dmb_10mb_1_amt, 0)) * 0.05 + (nvl(dummy_16_amt, 0)) * 0.05 + (nvl(social_fw_lb_30mb_amt, 0)) * 0.05 + (nvl(boj_sms_amt, 0)) * 0.05 + (nvl(social_fw_lb_80mb_amt, 0)) * 0.05
) AS sms_bundle_revenue
FROM
rocai_analysis3.prof_all_info_msisdn_daily_final_cco
WHERE
substr(part_date, 1, 6) = substr('${hiveconf:prev_month_date}', 1, 6)
AND substr(part_date, 7, 8) <= substr('${hiveconf:cur_date}', 7, 8)
AND plan_code NOT IN ('Others', 'Staff & Spouse')
GROUP BY
calling_nbr,
part_date,
plan_code;

-- GPRS MTD Counts
DROP TABLE IF EXISTS rocai_analysis3.gprs_mtd_counts_dashboard_backend_snapshot_cco;

CREATE TABLE rocai_analysis3.gprs_mtd_counts_dashboard_backend_snapshot_cco stored AS orc AS
SELECT
mtd_period,
count(DISTINCT calling_nbr) AS mtd_msisdn_cnt
FROM
rocai_analysis3.temp_trad_dcta_mtd_actual_counts_dashboard_backend
WHERE
nvl(data_flag, 0) = 1
OR nvl(data_bundle_revenue, 0) > 0
GROUP BY
mtd_period
UNION
SELECT
mtd_period,
count(DISTINCT calling_nbr) AS mtd_msisdn_cnt
FROM
rocai_analysis3.temp_trad_dcta_mtd_prior_counts_dashboard_backend
WHERE
nvl(data_flag, 0) = 1
OR nvl(data_bundle_revenue, 0) > 0
GROUP BY
mtd_period;

-- Voice MTD Counts
DROP TABLE IF EXISTS rocai_analysis3.voice_mtd_counts_dashboard_backend_snapshot_cco;

CREATE TABLE rocai_analysis3.voice_mtd_counts_dashboard_backend_snapshot_cco stored AS orc AS
SELECT
mtd_period,
count(DISTINCT calling_nbr) AS mtd_msisdn_cnt
FROM
rocai_analysis3.temp_trad_dcta_mtd_actual_counts_dashboard_backend
WHERE
nvl(voice_flag, 0) = 1
OR nvl(voice_bundle_revenue, 0) > 0
GROUP BY
mtd_period
UNION
SELECT
mtd_period,
count(DISTINCT calling_nbr) AS mtd_msisdn_cnt
FROM
rocai_analysis3.temp_trad_dcta_mtd_prior_counts_dashboard_backend
WHERE
nvl(voice_flag, 0) = 1
OR nvl(voice_bundle_revenue, 0) > 0
GROUP BY
mtd_period;

-- SMS MTD Counts
DROP TABLE IF EXISTS rocai_analysis3.sms_mtd_counts_dashboard_backend_snapshot_cco;

CREATE TABLE rocai_analysis3.sms_mtd_counts_dashboard_backend_snapshot_cco stored AS orc AS
SELECT
mtd_period,
count(DISTINCT calling_nbr) AS mtd_msisdn_cnt
FROM
rocai_analysis3.temp_trad_dcta_mtd_actual_counts_dashboard_backend
WHERE
nvl(sms_flag, 0) = 1
OR nvl(sms_bundle_revenue, 0) > 0
GROUP BY
mtd_period
UNION
SELECT
mtd_period,
count(DISTINCT calling_nbr) AS mtd_msisdn_cnt
FROM
rocai_analysis3.temp_trad_dcta_mtd_prior_counts_dashboard_backend
WHERE
nvl(sms_flag, 0) = 1
OR nvl(sms_bundle_revenue, 0) > 0
GROUP BY
mtd_period;



Drop Table if exists rocai_analysis3.daily_dashboard_backend_snapshot_cco;
Create Table rocai_analysis3.daily_dashboard_backend_snapshot_cco stored as orc as
Select
x.part_date,
(case when plan_code like 'Prepaid%' then 'Prepaid'
when plan_code like 'Postpaid%' then 'Postpaid'
when plan_code like 'Hybrid%' then 'Hybrid'
when plan_code like 'Staff & Spouse%' then 'Staff & Spouse'
else plan_code end) as plan_code,
data_flag,
voice_flag,
sms_flag,
homing_bsc,
homing_province,
homing_district,
homing_clusters,
max_technology,
subs_max_tech,
overall_cust_segment,
calling_nbr_cnt,
voice_user_cnt,
voice_rgs_user_cnt,
voice_non_rgs_user_cnt,
data_user_cnt,
data_rgs_user_cnt,
data_non_rgs_user_cnt,
sms_user_cnt,
sms_rgs_user_cnt,
sms_non_rgs_user_cnt,
data_total_sessions,
data_volume_gb,
voice_total_calls,
voice_usage_mins,
unbilled_voice_usage_mins,
sms_total_msgs,
y.old_mutual_implied_rate,
y.rtgs_translation_premium,
y.bond_translation_premium,
y.rtgs_interbank_exchange,
y.tax_rate,
(case when plan_code='Staff & Spouse' then 0 else voice_acct_main_balance_chg end) as voice_acct_main_balance_chg,
(case when plan_code='Staff & Spouse' then 0 else voice_acct_credit_balance_chg end) as voice_acct_credit_balance_chg,
(case when plan_code='Staff & Spouse' then 0 else voice_bundle_revenue end) as voice_bundle_revenue,
(case when plan_code='Staff & Spouse' then 0 else (voice_acct_main_balance_chg+voice_acct_credit_balance_chg+voice_bundle_revenue) end) as voice_total_revenue,
(case when plan_code='Staff & Spouse' then 0 else data_acct_main_balance_chg end) as data_acct_main_balance_chg,
(case when plan_code='Staff & Spouse' then 0 else data_acct_credit_balance_chg end) as data_acct_credit_balance_chg,
(case when plan_code='Staff & Spouse' then 0 else data_bundle_revenue end) as data_bundle_revenue,
(case when plan_code='Staff & Spouse' then 0 else (data_acct_main_balance_chg+data_acct_credit_balance_chg+data_bundle_revenue) end) as data_total_revenue,
(case when plan_code='Staff & Spouse' then 0 else sms_acct_main_balance_chg end) as sms_acct_main_balance_chg,
(case when plan_code='Staff & Spouse' then 0 else sms_acct_credit_balance_chg end) as sms_acct_credit_balance_chg,
(case when plan_code='Staff & Spouse' then 0 else sms_bundle_revenue end) as sms_bundle_revenue,
(case when plan_code='Staff & Spouse' then 0 else (sms_acct_main_balance_chg+sms_acct_credit_balance_chg+sms_bundle_revenue) end) as sms_total_revenue,
(case when plan_code='Staff & Spouse' then 0 else acs_commission_chg end) as acs_commission_chg,
(case when plan_code='Staff & Spouse' then 0 else total_revenue end) as total_revenue
From
(Select
part_date,
(case when plan_code like 'Prepaid%' then 'Prepaid'
when plan_code like 'Postpaid%' then 'Postpaid'
when plan_code like 'Hybrid%' then 'Hybrid'
when plan_code like 'Staff & Spouse%' then 'Staff & Spouse'
else plan_code end) as plan_code,
data_flag,
voice_flag,
sms_flag,
homing_bsc,
homing_province,
homing_district,
homing_clusters,
max_technology,
subs_max_tech,
overall_cust_segment,
calling_nbr_cnt,
voice_user_cnt,
voice_rgs_user_cnt,
voice_non_rgs_user_cnt,
data_user_cnt,
data_rgs_user_cnt,
data_non_rgs_user_cnt,
sms_user_cnt,
sms_rgs_user_cnt,
sms_non_rgs_user_cnt,
data_total_sessions,
data_volume_gb,
data_acct_main_balance_chg,
data_acct_credit_balance_chg,
voice_total_calls,
voice_usage_mins,
unbilled_voice_usage_mins,
voice_acct_main_balance_chg,
voice_acct_credit_balance_chg,
sms_total_msgs,
sms_acct_main_balance_chg,
sms_acct_credit_balance_chg,

((nvl(bjc1_1_amt,0))*0.6+
(nvl(bjc10_amt,0))*0.785+
(nvl(bjc2_amt,0))*0.71+
(nvl(bjc20_amt,0))*0.7875+
(nvl(bjc5_amt,0))*0.746+
(nvl(bjv1_amt,0))*0.45+
(nvl(bjv10_amt,0))*0.45+
(nvl(bjv2_amt,0))*0.45+
(nvl(bjv20_amt,0))*0.45+
(nvl(bjv5_amt,0))*0.45+
(nvl(voice_boj_1day_amt,0))*1+
(nvl(voice_boj_2day_amt,0))*1+
(nvl(voice_boj_3day_amt,0))*1+
(nvl(voice_boj_4day_amt,0))*1+
(nvl(voice_boj_5day_amt,0))*1+
(nvl(dummy_14_amt,0))*1 + 
(nvl(k_iflix_mb_15_amt,0))*1 + 
(nvl(k_iflix_db_1_amt,0))*1 + 
(nvl(k_iflix_wb_5_amt,0))*1+
(nvl(social_fw_hb_450mb_amt,0))*1+
(nvl(social_fw_hb_600mb_amt,0))*1+
(nvl(social_fw_lb_10mb_amt,0))*1+
(nvl(video_bundle_10gb_10_amt,0))*0.2+
(nvl(dmb_5mb_0_5_amt,0))*0.23+
(nvl(dmb_75mb_3_amt,0))*0.2+
(nvl(dmb_125mb_5_amt,0))*0.8+
(nvl(dmb_10mb_1_amt,0))*0.22+
(nvl(dd_mth_1500mb_amt,0))*0.04+
(nvl(dd_mth_2500mb_amt,0))*0.025+
(nvl(dd_wifi_150mb_amt,0))*0.04+
(nvl(dd_wifi_500mb_amt,0))*0.04+
(nvl(dd_wifi_1_2gb_amt,0))*0.03+
(nvl(dd_wifi_3_5gb_amt,0))*0.04) as voice_bundle_revenue,

((nvl(bjc1_1_amt,0))*0.309+
(nvl(bjc10_amt,0))*0.175+
(nvl(bjc2_amt,0))*0.19+
(nvl(bjc20_amt,0))*0.1875+
(nvl(bjc5_amt,0))*0.194+
(nvl(bjv1_amt,0))*0.55+
(nvl(bjv10_amt,0))*0.55+
(nvl(bjv2_amt,0))*0.55+
(nvl(bjv20_amt,0))*0.55+
(nvl(bjv5_amt,0))*0.55+
(nvl(data_daily_bundle_1gb_2_amt,0))*1+
(nvl(data_daily_bundle_250mb_1_amt,0))*1+
(nvl(data_daily_bundle_2gb_3_amt,0))*1+
(nvl(data_daily_bundle_50mb_05c_amt,0))*1+
(nvl(dd_mth_100mb_amt,0))*1+
(nvl(dd_mth_10mb_amt,0))*1+
(nvl(dd_mth_1500mb_amt,0))*0.96+
(nvl(dd_mth_200mb_amt,0))*1+
(nvl(dd_mth_2500mb_amt,0))*0.975+
(nvl(dd_mth_25mb_amt,0))*1+
(nvl(dd_mth_400mb_amt,0))*1+
(nvl(dd_mth_850mb_amt,0))*1+
(nvl(dd_wifi_1_2gb_amt,0))*0.97+
(nvl(dd_wifi_150mb_amt,0))*0.96+
(nvl(dd_wifi_25gb_amt,0))*1+
(nvl(dd_wifi_3_5gb_amt,0))*0.96+
(nvl(dd_wifi_500mb_amt,0))*0.96+
(nvl(dd_wk_120mb_amt,0))*1+
(nvl(dd_wk_15mb_amt,0))*1+
(nvl(dd_wk_300mb_amt,0))*1+
(nvl(dd_wk_35mb_amt,0))*1+
(nvl(dd_wk_650mb_amt,0))*0.5+
(nvl(dmb_1_5gb_35_amt,0))*1+
(nvl(dmb_10mb_1_amt,0))*0.73+
(nvl(dmb_2_5gb_50_amt,0))*1+
(nvl(dmb_300mb_10_amt,0))*1+
(nvl(dmb_5mb_0_5_amt,0))*0.77+
(nvl(dmb_750mb_20_amt,0))*1+
(nvl(dmb_75mb_3_amt,0))*0.8+
(nvl(facebook_daily_bundle_amt,0))*1+
(nvl(facebook_monthly_bundle_amt,0))*1+
(nvl(facebook_weekly_bundle_amt,0))*1+
(nvl(k_iflix_3days_3_amt,0))*1+
(nvl(private_wifi_50gb_75_amt,0))*1+
(nvl(private_wifi_75gb_100_amt,0))*1+
(nvl(social_fw_lb_30mb_amt,0))*0.95+
(nvl(social_fw_mb_170mb_amt,0))*1+
(nvl(social_fw_mb_300mb_amt,0))*1+
(nvl(social_fw_lb_80mb_amt,0))*0.95+
(nvl(video_bundle_10gb_10_amt,0))*0.8+
(nvl(video_bundle_35gb_35_amt,0))*1+
(nvl(whatsapp_daily_bundle_amt,0))*1+
(nvl(whatsapp_monthly_bundle_amt,0))*1+
(nvl(whatsapp_weekly_bundle_amt,0))*1+
(nvl(iflix_1day_200mb_amt,0))*1+
(nvl(facebook_1day_20mb_amt,0))*1+
(nvl(whatsapp_1day_20mb_amt,0))*1+
(nvl(instagram_daily_balance_20mb_amt,0))*1+
(nvl(instagram_daily_balance_45mb_amt,0))*1+
(nvl(instagram_weekly_balance_140mb_amt,0))*1+
(nvl(instagram_monthly_balance_400mb_amt,0))*1+
(nvl(twitter_daily_balance_20mb_amt,0))*1+
(nvl(twitter_daily_balance_45mb_amt,0))*1+
(nvl(twitter_weekly_balance_140mb_amt,0))*1+
(nvl(twitter_monthly_balance_400mb_amt,0))*1+
(nvl(whatsapp_weekly_65mb_amt,0))*1+
(nvl(whatsapp_monthly_240mb_amt,0))*1+
(nvl(data_daily_bundle_340_amt,0))*1+
(nvl(sasai_daily_bundle_343_amt,0))*1+
(nvl(sasai_daily_bundle_344_amt,0))*1+
(nvl(sasai_weekly_bundle_345_amt,0))*1+
(nvl(sasai_weekly_bundle_346_amt,0))*1+
(nvl(sasai_monthly_bundle_347_amt,0))*1+
(nvl(sasai_monthly_bundle_348_amt,0))*1+
(nvl(boj_voice_amt,0))*1+
(nvl(boj_data_amt,0))*1+
(nvl(boj_sms_amt,0))*0.95+
(nvl(dummy_1_amt,0))*1+
(nvl(dummy_2_amt,0))*1+
(nvl(dummy_3_amt,0))*1+
(nvl(dummy_4_amt,0))*1+
(nvl(dummy_5_amt,0))*1+
(nvl(dummy_6_amt,0))*1+
(nvl(dummy_7_amt,0))*1+
(nvl(dummy_8_amt,0))*1+
(nvl(dummy_9_amt,0))*1+
(nvl(dummy_10_amt,0))*1+
(nvl(dummy_11_amt,0))*1+
(nvl(dummy_12_amt,0))*1+
(nvl(dummy_13_amt,0))*1+
(nvl(dummy_14_amt,0))*1+
(nvl(dummy_15_amt,0))*1+
(nvl(dummy_16_amt,0))*0.95) as data_bundle_revenue,

((nvl(bjc1_1_amt,0))*0.091+
(nvl(bjc10_amt,0))*0.04+
(nvl(bjc2_amt,0))*0.1+
(nvl(bjc20_amt,0))*0.025+
(nvl(bjc5_amt,0))*0.06+
(nvl(sms_daily_bundle_15sms_amt,0))*1+
(nvl(sms_daily_bundle_50sms_amt,0))*1+
(nvl(sms_daily_bundle_5sms_amt,0))*1+
(nvl(sms_weekly_bundle_130sms_amt,0))*1+
(nvl(sms_weekly_bundle_250sms_amt,0))*1+
(nvl(sms_weekly_bundle_500sms_amt,0))*1+
(nvl(sms_daily_bundle_341_amt,0))*1+
(nvl(sms_weekly_bundle_342_amt,0))*1+
(nvl(dummy_17_amt,0))*1+
(nvl(dummy_18_amt,0))*1+
(nvl(video_bundle_2gb_2_amt,0))*1+
(nvl(dummy_19_amt,0))*1+
(nvl(dummy_20_amt, 0))*1+
(nvl(dmb_125mb_5_amt,0))*0.2+
(nvl(dmb_10mb_1_amt,0))*0.05+
(nvl(dummy_16_amt,0))*0.05+
(nvl(social_fw_lb_30mb_amt,0))*0.05+
(nvl(boj_sms_amt,0))*0.05+
(nvl(social_fw_lb_80mb_amt,0))*0.05) as sms_bundle_revenue,

acs_commission_chg,
total_revenue
from rocai_analysis3.daily_dashboard_backend_final_cco
Where substr(part_date, 1, 6) >= '${hiveconf:prev_2_month}') x
Left Join rocai_analysis2.rtgs_usd_conversion_rate y
On x.part_date = y.part_date;


-----------------------------------------------------------
-- YTD Stats
-----------------------------------------------------------
Drop table if exists rocai_analysis3.ytd_budget_metrics_dashboard_backend_snapshot_cco;
Create table rocai_analysis3.ytd_budget_metrics_dashboard_backend_snapshot_cco stored as orc as
Select 
"Budget" AS ytd_period,
a.part_Date,
b.tax_rate,
b.rtgs_interbank_exchange,
sum(case when lower(a.service)='data' then a.budget_stretched_usd * b.rtgs_interbank_exchange *b.tax_rate else '0' end)  as data_total_revenue,
sum(case when lower(a.service)='sms' then a.budget_stretched_usd * rtgs_interbank_exchange *b.tax_rate else '0' end) as sms_total_revenue,
sum(case when lower(a.service) IN ('prepaid voice','postpaid voice') then a.budget_stretched_usd * rtgs_interbank_exchange *b.tax_rate else '0' end) as voice_total_revenue
from rocai_analysis2.prof_temp_dcta_trad_budget_stats_wk a
left join rocai_analysis2.rtgs_usd_conversion_rate b on a.part_Date = b.part_date
WHERE
a.part_date >= 20230301
AND a.part_date <= '${hiveconf:cur_date}'
group by 
a.part_Date,
b.tax_rate,
b.rtgs_interbank_exchange
UNION ALL
Select 
"Actual" as ytd_period,
a.part_Date,
b.tax_rate,
b.rtgs_interbank_exchange,
sum(data_total_revenue) as data_total_revenue,
sum(sms_total_revenue) as sms_total_revenue,
sum(voice_total_revenue) as voice_total_revenue
from rocai_analysis3.dcta_final_profiler_cco a
LEFT JOIN rocai_analysis2.rtgs_usd_conversion_rate b ON a.part_Date = b.part_date
where
a.part_date >= 20230301
And a.part_date<='${hiveconf:cur_date}'
AND a.plan_code <>'Others' 
and a.plan_code NOT LIKE 'Staff & Spouse%'
group by
a.part_Date,
b.tax_rate,
b.rtgs_interbank_exchange;

-----------------------------------------------------------
-- MTD Stats
-----------------------------------------------------------
Drop table if exists rocai_analysis3.mtd_budget_metrics_dashboard_backend_snapshot_cco;
Create table rocai_analysis3.mtd_budget_metrics_dashboard_backend_snapshot_cco stored as orc as
Select 
"Budget" AS mtd_period,
a.part_Date,
b.tax_rate,
b.rtgs_interbank_exchange,
sum(case when lower(a.service)='data' then a.budget_stretched_usd * b.rtgs_interbank_exchange *b.tax_rate else '0' end)  as data_total_revenue,
sum(case when lower(a.service)='sms' then a.budget_stretched_usd * rtgs_interbank_exchange *b.tax_rate else '0' end) as sms_total_revenue,
sum(case when lower(a.service) IN ('prepaid voice','postpaid voice') then a.budget_stretched_usd * rtgs_interbank_exchange *b.tax_rate else '0' end) as voice_total_revenue
from rocai_analysis2.prof_temp_dcta_trad_budget_stats_wk a
left join rocai_analysis2.rtgs_usd_conversion_rate b on a.part_Date = b.part_date
WHERE
substr(a.part_date, 1, 6) = substr('${hiveconf:cur_date}', 1, 6) AND a.part_date <= '${hiveconf:cur_date}'
-- substr(a.part_date, 1, 6) = substr(20230309, 1, 6) AND a.part_date <= 20230309
group by a.part_Date, b.tax_rate, b.rtgs_interbank_exchange
UNION ALL
Select 
"Actual" as mtd_period,
a.part_Date,
b.tax_rate,
b.rtgs_interbank_exchange,
sum(data_total_revenue) as data_total_revenue,
sum(sms_total_revenue) as sms_total_revenue,
sum(voice_total_revenue) as voice_total_revenue
from rocai_analysis3.dcta_final_profiler_cco a
LEFT JOIN rocai_analysis2.rtgs_usd_conversion_rate b ON a.part_Date = b.part_date
where
substr(a.part_date, 1, 6) = substr('${hiveconf:cur_date}', 1, 6) AND a.part_date <= '${hiveconf:cur_date}'
-- substr(a.part_date, 1, 6) = substr(20230309, 1, 6) AND a.part_date <= 20230309
AND a.plan_code <>'Others' 
and a.plan_code NOT LIKE 'Staff & Spouse%'
group by a.part_Date, b.tax_rate, b.rtgs_interbank_exchange;

-----------------------------------------------------------
-- A1D Stats
-----------------------------------------------------------
Drop table if exists rocai_analysis3.a1d_budget_metrics_dashboard_backend_snapshot_cco;

Create table rocai_analysis3.a1d_budget_metrics_dashboard_backend_snapshot_cco stored as orc as
Select 
"Budget" AS a1d_period,
a.part_Date,
b.tax_rate,
b.rtgs_interbank_exchange,
sum(case when lower(a.service)='data' then a.budget_stretched_usd * b.rtgs_interbank_exchange *b.tax_rate else '0' end)  as data_total_revenue,
sum(case when lower(a.service)='sms' then a.budget_stretched_usd * rtgs_interbank_exchange *b.tax_rate else '0' end) as sms_total_revenue,
sum(case when lower(a.service) IN ('prepaid voice','postpaid voice') then a.budget_stretched_usd * rtgs_interbank_exchange *b.tax_rate else '0' end) as voice_total_revenue
from rocai_analysis2.prof_temp_dcta_trad_budget_stats_wk a
left join rocai_analysis2.rtgs_usd_conversion_rate b on a.part_Date = b.part_date
WHERE
a.part_date = '${hiveconf:cur_date}'
-- a.part_date = 20230309
group by
a.part_Date,
b.tax_rate,
b.rtgs_interbank_exchange
UNION ALL
Select 
"Actual" as a1d_period,
a.part_Date,
b.tax_rate,
b.rtgs_interbank_exchange,
sum(data_total_revenue) as data_total_revenue,
sum(sms_total_revenue) as sms_total_revenue,
sum(voice_total_revenue) as voice_total_revenue
from rocai_analysis3.dcta_final_profiler_cco a
LEFT JOIN rocai_analysis2.rtgs_usd_conversion_rate b ON a.part_Date = b.part_date
where
a.part_date = '${hiveconf:cur_date}'
-- a.part_date = 20230309
AND a.plan_code <>'Others'and a.plan_code NOT LIKE 'Staff & Spouse%'
group by
a.part_Date,
b.tax_rate,
b.rtgs_interbank_exchange;