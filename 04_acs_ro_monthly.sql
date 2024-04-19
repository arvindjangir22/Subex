drop table if exists rocai_analysis2.tmp_acs_msisdn_monthly_stats_new;
create table rocai_analysis2.tmp_acs_msisdn_monthly_stats_new stored as orc as 
select calling_nbr_new,
homing_site_code    
,homing_site_name    
,homing_bsc          
,homing_province     
,homing_district     
,homing_clusters,
overall_cust_segment
,max(homing_latitude) homing_latitude     
,max(homing_longitude) homing_longitude    
,max(homing_max_technology) homing_max_technology
,substr(borrow_date,1,6) as borrow_month,
sum(due_payments) due_payments,
bigint(sum(borrow_count)) as total_borrow_count,
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

bigint(sum(borrow_other_count)) as borrow_other_count,
bigint(sum(borrow_2_count)) as borrow_2_count,
bigint(sum(borrow_5_count)) as borrow_5_count,
bigint(sum(borrow_10_count)) as borrow_10_count,
bigint(sum(borrow_15_count)) as borrow_15_count,
bigint(sum(borrow_20_count)) as borrow_20_count,
bigint(sum(borrow_30_count)) as borrow_30_count,
bigint(sum(borrow_35_count)) as borrow_35_count,
bigint(sum(borrow_50_count)) as borrow_50_count,
bigint(sum(borrow_55_count)) as borrow_55_count,
bigint(sum(borrow_100_count)) as borrow_100_count,
bigint(sum(borrow_105_count)) as borrow_105_count,
bigint(sum(borrow_200_count)) as borrow_200_count,
bigint(sum(borrow_205_count)) as borrow_205_count,
bigint(sum(borrow_250_count)) as borrow_250_count,
bigint(sum(borrow_255_count)) as borrow_255_count,
bigint(sum(borrow_300_count)) as borrow_300_count,
bigint(sum(borrow_305_count)) as borrow_305_count,
bigint(sum(borrow_400_count)) as borrow_400_count,
bigint(sum(borrow_405_count)) as borrow_405_count,
bigint(sum(borrow_500_count)) as borrow_500_count,
bigint(sum(borrow_505_count)) as borrow_505_count,
bigint(sum(borrow_600_count)) as borrow_600_count,
bigint(sum(borrow_605_count)) as borrow_605_count,
bigint(sum(borrow_700_count)) as borrow_700_count,
bigint(sum(borrow_705_count)) as borrow_705_count,
bigint(sum(borrow_1005_count)) as borrow_1005_count,
bigint(sum(borrow_1505_count)) as borrow_1505_count,
bigint(sum(borrow_2005_count)) as borrow_2005_count,
bigint(sum(return_count)) as return_count
from 
(select a.*,overall_cust_segment
,homing_site_code    
,homing_site_name    
,homing_latitude     
,homing_longitude    
,homing_max_technology
,homing_bsc          
,homing_province     
,homing_district     
,homing_clusters
from rocai_analysis2.prof_acs_credit_borrow_return_daily a
left join rocai_analysis2.prof_all_mno_cust_segments b 
on a.calling_nbr_new=b.calling_nbr
and substr(a.borrow_date,1,6)=b.part_month
left join rocai_analysis2.prof_all_mno_homing c 
on  a.calling_nbr_new=c.calling_nbr
and substr(a.borrow_date,1,6)=c.part_month) t
group by calling_nbr_new,overall_cust_segment,
homing_site_code    
,homing_site_name    
,homing_bsc          
,homing_province     
,homing_district     
,homing_clusters
,substr(borrow_date,1,6);


drop table if exists rocai_analysis2.prof_acs_monthly_stats_new;
create table rocai_analysis2.prof_acs_monthly_stats_new stored as orc as
with overall as (select borrow_month,percentile(total_borrow_count*100,0.5)/100 as median_overall_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
group by borrow_month),
t2 as (
select borrow_month,
percentile(borrow_2_count*100,0.5)/100 as median_2_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_2_count>0
group by borrow_month
),
t5 as (
select borrow_month,
percentile(borrow_5_count*100,0.5)/100 as median_5_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_5_count>0
group by borrow_month
),
t10 as (
select borrow_month,
percentile(borrow_10_count*100,0.5)/100 as median_10_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_10_count>0
group by borrow_month
)
,t15 as (
select borrow_month,
percentile(borrow_15_count*100,0.5)/100 as median_15_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_15_count>0
group by borrow_month
)
,t20 as (
select borrow_month,
percentile(borrow_20_count*100,0.5)/100 as median_20_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_20_count>0
group by borrow_month
)
,t30 as (
select borrow_month,
percentile(borrow_30_count*100,0.5)/100 as median_30_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_30_count>0
group by borrow_month
)
,t35 as (
select borrow_month,
percentile(borrow_35_count*100,0.5)/100 as median_35_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_35_count>0
group by borrow_month
)
,t50 as (
select borrow_month,
percentile(borrow_50_count*100,0.5)/100 as median_50_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_50_count>0
group by borrow_month
)
,t55 as (
select borrow_month,
percentile(borrow_55_count*100,0.5)/100 as median_55_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_55_count>0
group by borrow_month
)
,t100 as (
select borrow_month,
percentile(borrow_100_count*100,0.5)/100 as median_100_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_100_count>0
group by borrow_month
)
,t105 as (
select borrow_month,
percentile(borrow_105_count*100,0.5)/100 as median_105_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_105_count>0
group by borrow_month
)
,t200 as (
select borrow_month,
percentile(borrow_200_count*100,0.5)/100 as median_200_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_200_count>0
group by borrow_month
)
,t205 as (
select borrow_month,
percentile(borrow_205_count*100,0.5)/100 as median_205_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_205_count>0
group by borrow_month
)
,t250 as (
select borrow_month,
percentile(borrow_250_count*100,0.5)/100 as median_250_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_250_count>0
group by borrow_month
)
,t255 as (
select borrow_month,
percentile(borrow_255_count*100,0.5)/100 as median_255_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_255_count>0
group by borrow_month
)
,t300 as (
select borrow_month,
percentile(borrow_300_count*100,0.5)/100 as median_300_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_300_count>0
group by borrow_month
)
,t305 as (
select borrow_month,
percentile(borrow_305_count*100,0.5)/100 as median_305_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_305_count>0
group by borrow_month
)
,t400 as (
select borrow_month,
percentile(borrow_400_count*100,0.5)/100 as median_400_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_400_count>0
group by borrow_month
)
,t405 as (
select borrow_month,
percentile(borrow_405_count*100,0.5)/100 as median_405_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_405_count>0
group by borrow_month
)
,t500 as (
select borrow_month,
percentile(borrow_500_count*100,0.5)/100 as median_500_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_500_count>0
group by borrow_month
)
,t505 as (
select borrow_month,
percentile(borrow_505_count*100,0.5)/100 as median_505_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_505_count>0
group by borrow_month
)
,t600 as (
select borrow_month,
percentile(borrow_600_count*100,0.5)/100 as median_600_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_600_count>0
group by borrow_month
)
,t605 as (
select borrow_month,
percentile(borrow_605_count*100,0.5)/100 as median_605_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_605_count>0
group by borrow_month
)
,t700 as (
select borrow_month,
percentile(borrow_700_count*100,0.5)/100 as median_700_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_700_count>0
group by borrow_month
)
,t705 as (
select borrow_month,
percentile(borrow_705_count*100,0.5)/100 as median_705_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_705_count>0
group by borrow_month
)
,t1005 as (
select borrow_month,
percentile(borrow_1005_count*100,0.5)/100 as median_1005_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_1005_count>0
group by borrow_month
)
,t1505 as (
select borrow_month,
percentile(borrow_1505_count*100,0.5)/100 as median_1505_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_1505_count>0
group by borrow_month
)
,t2005 as (
select borrow_month,
percentile(borrow_2005_count*100,0.5)/100 as median_2005_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new
where borrow_2005_count>0
group by borrow_month
)
select a.borrow_month,
overall_cust_segment,
homing_site_code,
homing_site_name,
homing_latitude,
homing_longitude,
homing_bsc,
homing_province,
homing_district,
homing_clusters,
count(distinct calling_nbr_new) as borrrowers,
count(distinct(case when due_payments>0 then calling_nbr_new else null end)) as due_customers,
sum(due_payments) as due_payments,
sum(total_borrow_count) total_borrow_count,
sum(late_payments) late_payments,
sum(total_borrow_amount) total_borrow_amount,
sum(outstanding_amount) outstanding_amount,
sum(outstanding_amount_USD) outstanding_amount_USD,
sum(total_return_amount) total_return_amount,
sum(total_commission_amount) total_commission_amount,
sum(total_gross_return_amount) total_gross_return_amount,
sum(total_borrow_amount_usd) total_borrow_amount_usd,
sum(total_return_amount_usd) total_return_amount_usd,
sum(total_commission_amount_usd) total_commission_amount_usd,
sum(total_gross_return_amount_usd) total_gross_return_amount_usd,
sum(total_gross_return_amount_usd_deadline) total_gross_return_amount_usd_deadline,
sum(borrow_other_amount) borrow_other_amount, 
sum(borrow_other_amount_usd) borrow_other_amount_usd, 
sum(borrow_2_amount) borrow_2_amount,
sum(borrow_2_amount_usd) borrow_2_amount_usd,
sum(borrow_5_amount) borrow_5_amount,
sum(borrow_5_amount_usd) borrow_5_amount_usd,
sum(borrow_10_amount) borrow_10_amount,
sum(borrow_10_amount_usd) borrow_10_amount_usd,
sum(borrow_15_amount) borrow_15_amount,
sum(borrow_15_amount_usd) borrow_15_amount_usd,
sum(borrow_20_amount) borrow_20_amount,
sum(borrow_20_amount_usd) borrow_20_amount_usd,
sum(borrow_30_amount) borrow_30_amount,
sum(borrow_30_amount_usd) borrow_30_amount_usd,
sum(borrow_35_amount) borrow_35_amount,
sum(borrow_35_amount_usd) borrow_35_amount_usd,
sum(borrow_50_amount) borrow_50_amount,
sum(borrow_50_amount_usd) borrow_50_amount_usd,
sum(borrow_55_amount) borrow_55_amount,
sum(borrow_55_amount_usd) borrow_55_amount_usd,
sum(borrow_100_amount) borrow_100_amount,
sum(borrow_100_amount_usd) borrow_100_amount_usd,
sum(borrow_105_amount) borrow_105_amount,
sum(borrow_105_amount_usd) borrow_105_amount_usd,
sum(borrow_200_amount) borrow_200_amount,
sum(borrow_200_amount_usd) borrow_200_amount_usd,
sum(borrow_205_amount) borrow_205_amount,
sum(borrow_205_amount_usd) borrow_205_amount_usd,
sum(borrow_250_amount) borrow_250_amount,
sum(borrow_250_amount_usd) borrow_250_amount_usd,
sum(borrow_255_amount) borrow_255_amount,
sum(borrow_255_amount_usd) borrow_255_amount_usd,
sum(borrow_300_amount) borrow_300_amount,
sum(borrow_300_amount_usd) borrow_300_amount_usd,
sum(borrow_305_amount) borrow_305_amount,
sum(borrow_305_amount_usd) borrow_305_amount_usd,
sum(borrow_400_amount) borrow_400_amount,
sum(borrow_400_amount_usd) borrow_400_amount_usd,
sum(borrow_405_amount) borrow_405_amount,
sum(borrow_405_amount_usd) borrow_405_amount_usd,
sum(borrow_500_amount) borrow_500_amount,
sum(borrow_500_amount_usd) borrow_500_amount_usd,
sum(borrow_505_amount) borrow_505_amount,
sum(borrow_505_amount_usd) borrow_505_amount_usd,
sum(borrow_600_amount) borrow_600_amount,
sum(borrow_600_amount_usd) borrow_600_amount_usd,
sum(borrow_605_amount) borrow_605_amount,
sum(borrow_605_amount_usd) borrow_605_amount_usd,
sum(borrow_700_amount) borrow_700_amount,
sum(borrow_700_amount_usd) borrow_700_amount_usd,
sum(borrow_705_amount) borrow_705_amount,
sum(borrow_705_amount_usd) borrow_705_amount_usd,
sum(borrow_1005_amount) borrow_1005_amount,
sum(borrow_1005_amount_usd) borrow_1005_amount_usd,
sum(borrow_1505_amount) borrow_1505_amount,
sum(borrow_1505_amount_usd) borrow_1505_amount_usd,
sum(borrow_2005_amount) borrow_2005_amount,
sum(borrow_2005_amount_usd) borrow_2005_amount_usd,
sum(borrow_other_count) borrow_other_count,
sum(borrow_2_count) borrow_2_count,
sum(borrow_5_count) borrow_5_count,
sum(borrow_10_count) borrow_10_count,
sum(borrow_15_count) borrow_15_count,
sum(borrow_20_count) borrow_20_count,
sum(borrow_30_count) borrow_30_count,
sum(borrow_35_count) borrow_35_count,
sum(borrow_50_count) borrow_50_count,
sum(borrow_55_count) borrow_55_count,
sum(borrow_100_count) borrow_100_count,
sum(borrow_105_count) borrow_105_count,
sum(borrow_200_count) borrow_200_count,
sum(borrow_205_count) borrow_205_count,
sum(borrow_250_count) borrow_250_count,
sum(borrow_255_count) borrow_255_count,
sum(borrow_300_count) borrow_300_count,
sum(borrow_305_count) borrow_305_count,
sum(borrow_400_count) borrow_400_count,
sum(borrow_405_count) borrow_405_count,
sum(borrow_500_count) borrow_500_count,
sum(borrow_505_count) borrow_505_count,
sum(borrow_600_count) borrow_600_count,
sum(borrow_605_count) borrow_605_count,
sum(borrow_700_count) borrow_700_count,
sum(borrow_705_count) borrow_705_count,
sum(borrow_1005_count) borrow_1005_count,
sum(borrow_1505_count) borrow_1505_count,
sum(borrow_2005_count) borrow_2005_count,
sum(return_count) return_count,
median_overall_borrow_frequency,
median_2_borrow_frequency,
median_5_borrow_frequency,
nvl(median_10_borrow_frequency,0) median_10_borrow_frequency,
nvl(median_15_borrow_frequency,0) median_15_borrow_frequency,
nvl(median_20_borrow_frequency,0) median_20_borrow_frequency,
nvl(median_30_borrow_frequency,0) median_30_borrow_frequency,
nvl(median_35_borrow_frequency,0) median_35_borrow_frequency,
nvl(median_50_borrow_frequency,0) median_50_borrow_frequency,
nvl(median_55_borrow_frequency,0) median_55_borrow_frequency,
nvl(median_100_borrow_frequency,0) median_100_borrow_frequency,
nvl(median_105_borrow_frequency,0) median_105_borrow_frequency,
nvl(median_200_borrow_frequency,0) median_200_borrow_frequency,
nvl(median_205_borrow_frequency,0) median_205_borrow_frequency,
nvl(median_250_borrow_frequency,0) median_250_borrow_frequency,
nvl(median_255_borrow_frequency,0) median_255_borrow_frequency,
nvl(median_300_borrow_frequency,0) median_300_borrow_frequency,
nvl(median_305_borrow_frequency,0) median_305_borrow_frequency,
nvl(median_400_borrow_frequency,0) median_400_borrow_frequency,
nvl(median_405_borrow_frequency,0) median_405_borrow_frequency,
nvl(median_500_borrow_frequency,0) median_500_borrow_frequency,
nvl(median_505_borrow_frequency,0) median_505_borrow_frequency,
nvl(median_600_borrow_frequency,0) median_600_borrow_frequency,
nvl(median_605_borrow_frequency,0) median_605_borrow_frequency,
nvl(median_700_borrow_frequency,0) median_700_borrow_frequency,
nvl(median_705_borrow_frequency,0) median_705_borrow_frequency,
nvl(median_1005_borrow_frequency,0) median_1005_borrow_frequency,
nvl(median_1505_borrow_frequency,0) median_1505_borrow_frequency,
nvl(median_2005_borrow_frequency,0) median_2005_borrow_frequency
from rocai_analysis2.tmp_acs_msisdn_monthly_stats_new a 
left join t2 e 
on a.borrow_month = e.borrow_month
left join t5 f 
on a.borrow_month = f.borrow_month
left join t10 g 
on a.borrow_month = g.borrow_month
left join t15 h
on a.borrow_month = h.borrow_month
left join t20 i
on a.borrow_month = i.borrow_month
left join t30 j
on a.borrow_month = j.borrow_month
left join t35 u
on a.borrow_month = u.borrow_month
left join t50 l
on a.borrow_month = l.borrow_month
left join t55 a1
on a.borrow_month = a1.borrow_month

left join t100 m
on a.borrow_month = m.borrow_month
left join t105 a2
on a.borrow_month = a2.borrow_month

left join t200 n
on a.borrow_month = n.borrow_month
left join t205 a3
on a.borrow_month = a3.borrow_month

left join t250 o
on a.borrow_month = o.borrow_month

left join t255 a4
on a.borrow_month = a4.borrow_month

left join t300 p
on a.borrow_month = p.borrow_month

left join t305 a5
on a.borrow_month = a5.borrow_month

left join t400 q
on a.borrow_month = q.borrow_month

left join t405 a6
on a.borrow_month = a6.borrow_month

left join t500 r
on a.borrow_month = r.borrow_month

left join t505 a7
on a.borrow_month = a7.borrow_month


left join overall k
on a.borrow_month = k.borrow_month
left join t600 v
on a.borrow_month = v.borrow_month

left join t605 a8
on a.borrow_month = a8.borrow_month

left join t700 w
on a.borrow_month = w.borrow_month

left join t705 a9
on a.borrow_month = a9.borrow_month

left join t1005 a10
on a.borrow_month = a10.borrow_month

left join t1505 a11
on a.borrow_month = a11.borrow_month

left join t2005 a12
on a.borrow_month = a12.borrow_month

group by a.borrow_month,
overall_cust_segment,
homing_site_code,
homing_site_name,
homing_latitude,
homing_longitude,
homing_bsc,
homing_province,
homing_district,
homing_clusters,
median_overall_borrow_frequency,
median_2_borrow_frequency,
median_5_borrow_frequency,
median_10_borrow_frequency,
median_15_borrow_frequency,
median_20_borrow_frequency,
median_30_borrow_frequency,
median_35_borrow_frequency,
median_50_borrow_frequency,
median_55_borrow_frequency,
median_100_borrow_frequency,
median_105_borrow_frequency,
median_200_borrow_frequency,
median_205_borrow_frequency,
median_250_borrow_frequency,
median_255_borrow_frequency,
median_300_borrow_frequency,
median_305_borrow_frequency,
median_400_borrow_frequency,
median_405_borrow_frequency,
median_500_borrow_frequency,
median_505_borrow_frequency,
median_600_borrow_frequency,
median_605_borrow_frequency,
median_700_borrow_frequency,
median_705_borrow_frequency,
median_1005_borrow_frequency,
median_1505_borrow_frequency,
median_2005_borrow_frequency;
