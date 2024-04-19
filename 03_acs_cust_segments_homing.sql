set hive.exec.dynamic.partition.mode=nonstrict; 
use rocai_analysis2;

alter table rocai_analysis2.prof_all_mno_cust_segments drop if exists partition(part_month='202311');
insert into rocai_analysis2.prof_all_mno_cust_segments partition (part_month)
select calling_nbr,overall_cust_segment,'202311' as part_month
from rocai_analysis3.tmp_msisdn_rolling_30days_segments_final_cco;


alter table rocai_analysis2.prof_all_mno_homing drop if exists partition(part_month='202311');
insert into rocai_analysis2.prof_all_mno_homing partition (part_month)
select calling_nbr         
,homing_site_code    
,homing_site_name    
,homing_latitude     
,homing_longitude    
,max_technology as homing_max_technology
,'' as homing_rat_type
,homing_bsc          
,homing_province     
,homing_district     
,homing_clusters
,'202311' as part_month
from rocai_analysis3.prof_rolling_30days_homing_msisdn_corrected_final_cco;