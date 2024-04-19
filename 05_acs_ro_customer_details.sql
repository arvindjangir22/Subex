drop table if exists rocai_analysis2.tmp_acs_msisdn_overall_stats_new;
create table rocai_analysis2.tmp_acs_msisdn_overall_stats_new stored as orc as 
select calling_nbr_new,
max(acct_id) acct_id,
max(borrow_date) as last_borrow_date,
avg(return_days) as avg_repayment_age,
max(outstanding_days) as outstanding_repayment_age,
sum(due_payments) due_payments,
sum(borrow_count) as total_borrow_count,
sum(case when return_days>7 then borrow_count else 0 end) as late_payments,
sum(borrow_amount) as total_borrow_amount,
sum(outstanding_amount) outstanding_amount,
sum(outstanding_amount_usd) outstanding_amount_usd,
sum(return_amount) as total_return_amount,
sum(commission_amount) as total_commission_amount,
sum(gross_return_amount) as total_gross_return_amount,
sum(borrow_amount_usd) as total_borrow_amount_usd,
sum(return_amount_usd) as total_return_amount_usd,
sum(commission_amount_usd) as total_commission_amount_usd,
sum(gross_return_amount_usd) as total_gross_return_amount_usd,
sum(gross_return_amount_usd_deadline) as total_gross_return_amount_usd_deadline,
sum(borrow_other_amount) as borrow_other_amount,
sum(borrow_other_amount_usd) as borrow_other_amount_usd,
sum(borrow_2_amount) as borrow_2_amount,
sum(borrow_2_amount_usd) as borrow_2_amount_usd,
sum(borrow_5_amount) as borrow_5_amount,
sum(borrow_5_amount_usd) as borrow_5_amount_usd,
sum(borrow_10_amount) as borrow_10_amount,
sum(borrow_10_amount_usd) as borrow_10_amount_usd,
sum(borrow_15_amount) as borrow_15_amount,
sum(borrow_15_amount_usd) as borrow_15_amount_usd,
sum(borrow_20_amount) as borrow_20_amount,
sum(borrow_20_amount_usd) as borrow_20_amount_usd,
sum(borrow_30_amount) as borrow_30_amount,
sum(borrow_30_amount_usd) as borrow_30_amount_usd,
sum(borrow_35_amount) as borrow_35_amount,
sum(borrow_35_amount_usd) as borrow_35_amount_usd,
sum(borrow_50_amount) as borrow_50_amount,
sum(borrow_50_amount_usd) as borrow_50_amount_usd,
sum(borrow_55_amount) as borrow_55_amount,
sum(borrow_55_amount_usd) as borrow_55_amount_usd,
sum(borrow_100_amount) as borrow_100_amount,
sum(borrow_100_amount_usd) as borrow_100_amount_usd,
sum(borrow_105_amount) as borrow_105_amount,
sum(borrow_105_amount_usd) as borrow_105_amount_usd,
sum(borrow_200_amount) as borrow_200_amount,
sum(borrow_200_amount_usd) as borrow_200_amount_usd,
sum(borrow_205_amount) as borrow_205_amount,
sum(borrow_205_amount_usd) as borrow_205_amount_usd,
sum(borrow_250_amount) as borrow_250_amount,
sum(borrow_250_amount_usd) as borrow_250_amount_usd,
sum(borrow_255_amount) as borrow_255_amount,
sum(borrow_255_amount_usd) as borrow_255_amount_usd,
sum(borrow_300_amount) as borrow_300_amount,
sum(borrow_300_amount_usd) as borrow_300_amount_usd,
sum(borrow_305_amount) as borrow_305_amount,
sum(borrow_305_amount_usd) as borrow_305_amount_usd,
sum(borrow_400_amount) as borrow_400_amount,
sum(borrow_400_amount_usd) as borrow_400_amount_usd,
sum(borrow_405_amount) as borrow_405_amount,
sum(borrow_405_amount_usd) as borrow_405_amount_usd,
sum(borrow_500_amount) as borrow_500_amount,
sum(borrow_500_amount_usd) as borrow_500_amount_usd,
sum(borrow_505_amount) as borrow_505_amount,
sum(borrow_505_amount_usd) as borrow_505_amount_usd,
sum(borrow_600_amount) as borrow_600_amount,
sum(borrow_600_amount_usd) as borrow_600_amount_usd,
sum(borrow_605_amount) as borrow_605_amount,
sum(borrow_605_amount_usd) as borrow_605_amount_usd,
sum(borrow_700_amount) as borrow_700_amount,
sum(borrow_700_amount_usd) as borrow_700_amount_usd,
sum(borrow_705_amount) as borrow_705_amount,
sum(borrow_705_amount_usd) as borrow_705_amount_usd,
sum(borrow_1005_amount) as borrow_1005_amount,
sum(borrow_1005_amount_usd) as borrow_1005_amount_usd,
sum(borrow_1505_amount) as borrow_1505_amount,
sum(borrow_1505_amount_usd) as borrow_1505_amount_usd,
sum(borrow_2005_amount) as borrow_2005_amount,
sum(borrow_2005_amount_usd) as borrow_2005_amount_usd,

sum(borrow_other_count) as borrow_other_count,
sum(borrow_2_count) as borrow_2_count,
sum(borrow_5_count) as borrow_5_count,
sum(borrow_10_count) as borrow_10_count,
sum(borrow_15_count) as borrow_15_count,
sum(borrow_20_count) as borrow_20_count,
sum(borrow_30_count) as borrow_30_count,
sum(borrow_35_count) as borrow_35_count,
sum(borrow_50_count) as borrow_50_count,
sum(borrow_55_count) as borrow_55_count,
sum(borrow_100_count) as borrow_100_count,
sum(borrow_105_count) as borrow_105_count,
sum(borrow_200_count) as borrow_200_count,
sum(borrow_205_count) as borrow_205_count,
sum(borrow_250_count) as borrow_250_count,
sum(borrow_255_count) as borrow_255_count,
sum(borrow_300_count) as borrow_300_count,
sum(borrow_305_count) as borrow_305_count,
sum(borrow_400_count) as borrow_400_count,
sum(borrow_405_count) as borrow_405_count,
sum(borrow_500_count) as borrow_500_count,
sum(borrow_505_count) as borrow_505_count,
sum(borrow_600_count) as borrow_600_count,
sum(borrow_605_count) as borrow_605_count,
sum(borrow_700_count) as borrow_700_count,
sum(borrow_705_count) as borrow_705_count,
sum(borrow_1005_count) as borrow_1005_count,
sum(borrow_1505_count) as borrow_1505_count,
sum(borrow_2005_count) as borrow_2005_count,

sum(return_count) as return_count
from rocai_analysis2.prof_acs_credit_borrow_return_daily 
group by calling_nbr_new;


drop table if exists rocai_analysis2.tmp_msisdn_acs_pref_denom_new;
create table rocai_analysis2.tmp_msisdn_acs_pref_denom_new stored as orc as
select *
from (
select *,row_number() over (partition by calling_nbr_new order by borrow_count desc,preferred_denomination desc) as rnk 
from(
select nvl(b.msisdn_norm,concat('000000',a.acct_id)) calling_nbr_new,borrow_amount as preferred_denomination,count(1) as borrow_count
from rocai_analysis2.tmp_acs_credit_borrow_txn a 
left join 
(Select acct_id, msisdn_norm From rocai_analysis2.tmp_z_ref_msisdn_acctid_latest_date
Where plan_name not like '%Staff%'
and plan_name not like '%Spouse%'
and plan_name not like '%Hybrid%'
and plan_name not like '%Postpaid%'
and plan_name like '%Prepaid%'
And acct_id in (Select x.acct_id From rocai_analysis2.tmp_msisdn_acct_id_mapping x)
Group By acct_id, msisdn_norm)b 
on a.acct_id=b.acct_id
group by nvl(b.msisdn_norm,concat('000000',a.acct_id)),borrow_amount)t) x 
where rnk=1;


drop table if exists rocai_analysis2.tmp_msisdn_acs_highest_denom_new;
create table rocai_analysis2.tmp_msisdn_acs_highest_denom_new stored as orc as
select nvl(b.msisdn_norm,concat('000000',a.acct_id)) calling_nbr_new,max(borrow_amount) as highest_denomination
from rocai_analysis2.tmp_acs_credit_borrow_txn a
left join
(Select acct_id, msisdn_norm From rocai_analysis2.tmp_z_ref_msisdn_acctid_latest_date
Where plan_name not like '%Staff%'
and plan_name not like '%Spouse%'
and plan_name not like '%Hybrid%'
and plan_name not like '%Postpaid%'
and plan_name like '%Prepaid%'
And acct_id in (Select x.acct_id From rocai_analysis2.tmp_msisdn_acct_id_mapping x)
Group By acct_id, msisdn_norm)b  
on a.acct_id=b.acct_id
group by nvl(b.msisdn_norm,concat('000000',a.acct_id));

drop table if exists rocai_analysis2.tmp_msisdn_acs_hom_seg_latest_new;
create table rocai_analysis2.tmp_msisdn_acs_hom_seg_latest_new stored as orc as
select nvl(a.calling_nbr,b.calling_nbr) as calling_nbr_new,overall_cust_segment
,homing_site_code    
,homing_site_name    
,homing_latitude     
,homing_longitude    
,homing_max_technology
,homing_bsc          
,homing_province     
,homing_district     
,homing_clusters
from (select * from rocai_analysis2.prof_all_mno_cust_segments where part_month = substr('20231111',1,6)) a 
full join (select * from rocai_analysis2.prof_all_mno_homing where part_month = substr('20231111',1,6)) b
on a.calling_nbr=b.calling_nbr
and a.part_month=b.part_month
group by nvl(a.calling_nbr,b.calling_nbr),overall_cust_segment
,homing_site_code    
,homing_site_name    
,homing_latitude     
,homing_longitude    
,homing_max_technology    
,homing_bsc          
,homing_province     
,homing_district     
,homing_clusters;

drop table if exists rocai_analysis2.tmp_msisdn_acs_hom_seg_latest_new_final;
create table rocai_analysis2.tmp_msisdn_acs_hom_seg_latest_new_final stored as orc as
select a.calling_nbr_new,highest_denomination,preferred_denomination
,overall_cust_segment
,homing_site_code    
,homing_site_name    
,homing_latitude     
,homing_longitude    
,homing_max_technology
,homing_bsc          
,homing_province     
,homing_district     
,homing_clusters,
case when overall_cust_segment is null then 'Inactive' else 'Active' end as Activity_Status
from rocai_analysis2.tmp_msisdn_acs_highest_denom_new a 
left join rocai_analysis2.tmp_msisdn_acs_pref_denom_new b 
on a.calling_nbr_new = b.calling_nbr_new 
left join rocai_analysis2.tmp_msisdn_acs_hom_seg_latest_new c 
on a.calling_nbr_new = c.calling_nbr_new;


drop table if exists rocai_analysis2.prof_msisdn_acs_ro_stats_agg_seg_homing_new;
create table rocai_analysis2.prof_msisdn_acs_ro_stats_agg_seg_homing_new stored as orc as
select a.*,overall_cust_segment
,homing_site_code    
,homing_site_name    
,homing_latitude     
,homing_longitude    
,homing_max_technology
,homing_bsc          
,homing_province     
,homing_district     
,homing_clusters,
Activity_Status,
highest_denomination,preferred_denomination
from rocai_analysis2.tmp_acs_msisdn_overall_stats_new a left join rocai_analysis2.tmp_msisdn_acs_hom_seg_latest_new_final b 
on a.calling_nbr_new=b.calling_nbr_new;

drop table if exists rocai_analysis2.prof_acs_ro_msisdn_details;
create table rocai_analysis2.prof_acs_ro_msisdn_details stored as orc as
select calling_nbr_new,
a.acct_id,
avg_repayment_age,
outstanding_repayment_age,
outstanding_amount,
outstanding_amount_usd,
last_borrow_date,
total_borrow_count,
late_payments,
total_borrow_amount,
total_return_amount,
total_commission_amount,
total_gross_return_amount,
total_borrow_amount_usd,
total_return_amount_usd,
total_commission_amount_usd,
total_gross_return_amount_usd,
total_gross_return_amount_usd_deadline,
borrow_other_amount,
borrow_other_amount_usd,
borrow_2_amount,
borrow_2_amount_usd,
borrow_5_amount,
borrow_5_amount_usd,
borrow_10_amount,
borrow_10_amount_usd,
borrow_15_amount,
borrow_15_amount_usd,
borrow_20_amount,
borrow_20_amount_usd,
borrow_30_amount,
borrow_30_amount_usd,
borrow_35_amount,
borrow_35_amount_usd,
borrow_50_amount,
borrow_50_amount_usd,
borrow_55_amount,
borrow_55_amount_usd,
borrow_100_amount,
borrow_100_amount_usd,
borrow_105_amount,
borrow_105_amount_usd,
borrow_200_amount,
borrow_200_amount_usd,
borrow_205_amount,
borrow_205_amount_usd,
borrow_250_amount,
borrow_250_amount_usd,
borrow_255_amount,
borrow_255_amount_usd,
borrow_300_amount,
borrow_300_amount_usd,
borrow_305_amount,
borrow_305_amount_usd,
borrow_400_amount,
borrow_400_amount_usd,
borrow_405_amount,
borrow_405_amount_usd,
borrow_500_amount,
borrow_500_amount_usd,
borrow_505_amount,
borrow_505_amount_usd,
borrow_600_amount,
borrow_600_amount_usd,
borrow_605_amount,
borrow_605_amount_usd,
borrow_700_amount,
borrow_700_amount_usd,
borrow_705_amount,
borrow_705_amount_usd,
borrow_1005_amount,
borrow_1005_amount_usd,
borrow_1505_amount,
borrow_1505_amount_usd,
borrow_2005_amount,
borrow_2005_amount_usd,

nvl(overall_cust_segment,'Inactive') overall_cust_segment,
nvl(homing_site_code,'Unknown') homing_site_code,
nvl(homing_site_name,'Unknown') homing_site_name,
homing_latitude,
homing_longitude,
nvl(homing_max_technology,'Unknown') homing_max_technology,
nvl(homing_bsc,'Unknown') homing_bsc,
nvl(homing_province,'Unknown') homing_province,
nvl(homing_district,'Unknown') homing_district,
nvl(homing_clusters,'Unknown') homing_clusters,
nvl(gender,'Unknown') gender,
case when (2021-substr(dob,1,4))>=18 and (2021-substr(dob,1,4))<=29 then "18-29"
when (2021-substr(dob,1,4))>=30 and (2021-substr(dob,1,4))<=39 then "30-39"
when (2021-substr(dob,1,4))>=40 and (2021-substr(dob,1,4))<=49 then "40-49"
when (2021-substr(dob,1,4))>=50 and (2021-substr(dob,1,4))<=59 then "50-59"
when (2021-substr(dob,1,4))>=60 and (2021-substr(dob,1,4))<=69 then "60-69"
when (2021-substr(dob,1,4))>=70 then "70+"
else "Unknown" end as age_group,
case when ((2021-substr(dob,1,4))<18 or (2021-substr(dob,1,4))>100) then "Unknown" else (2021-substr(dob,1,4)) end as age,
CAST(MONTHS_BETWEEN(from_unixtime(unix_timestamp('20231111','yyyyMMdd')),from_unixtime(unix_timestamp(cust_create_date ,'yyyyMMdd'), 'yyyy-MM-dd')) AS INT) as age_on_nw,
nvl(cust_language,'Unknown') cust_language,
nvl(HANDSET_support_type,'Unknown') HANDSET_support_type,
nvl(device_type,'Unknown') device_type,
Activity_Status,
highest_denomination,preferred_denomination,
datediff(from_unixtime(unix_timestamp('20231111' ,'yyyyMMdd')),from_unixtime(unix_timestamp(last_borrow_date ,'yyyyMMdd'))) as days_since_last_borrow,
borrow_other_count, 
borrow_2_count, 
borrow_5_count,
borrow_10_count,
borrow_15_count,
borrow_20_count,
borrow_30_count,
borrow_35_count,
borrow_50_count,
borrow_55_count,
borrow_100_count,
borrow_105_count,
borrow_200_count,
borrow_205_count,
borrow_250_count,
borrow_255_count,
borrow_300_count,
borrow_305_count,
borrow_400_count,
borrow_405_count,
borrow_500_count,
borrow_505_count,
borrow_600_count,
borrow_605_count,
borrow_700_count,
borrow_705_count,
borrow_1005_count,
borrow_1505_count,
borrow_2005_count,


return_count
from rocai_analysis2.prof_msisdn_acs_ro_stats_agg_seg_homing_new a left join rocai_analysis2.prof_msisdn_device_demographics_rolling5months b 
on a.calling_nbr_new=b.calling_nbr;