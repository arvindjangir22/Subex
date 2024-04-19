--Last 7 months of borrow txns
drop table if exists rocai_analysis2.tmp_acs_credit_borrow_txn;
create table rocai_analysis2.tmp_acs_credit_borrow_txn stored as orc as
select a.*,nvl(b.expected_commission,0) expected_commission
from
(select *,'Borrow' as acct_book_type_new
from rocai_analysis2.prof_acs_acct_credit_borrowers
where substr(borrow_date,1,6)>='202305' 
and borrow_date<='20231111') a 
left join rocai_analysis2.prof_acs_commission_rates b 
on a.borrow_amount=b.borrow_amt;

--First borrow msisdn level in last 7 months
drop table if exists rocai_analysis2.tmp_acs_credit_first_borrow;
create table rocai_analysis2.tmp_acs_credit_first_borrow stored as orc as
select acct_id,min(borrow_ts) first_borrow_ts
from rocai_analysis2.tmp_acs_credit_borrow_txn
group by acct_id;

--Last 7 months of return & excluding repayments for borrows before 7 months
drop table if exists rocai_analysis2.tmp_acs_credit_return_txn;
create table rocai_analysis2.tmp_acs_credit_return_txn stored as orc as
select a.*
from(
select acct_id,
return_ts,
return_date,
return_id,
'Return' as acct_book_type_new,
case when acct_book_type = "Return Loan" then return_amount else 0 end as return_amount,
case when acct_book_type = "ACS Commission" then return_amount else 0 end as commission_amount,
case when acct_book_type = "Return Loan" then 1 else 0 end as return_count
from rocai_analysis2.prof_acs_acct_credit_returners 
where substr(return_date,1,6)>= '202305'
and return_date<='20231111') a 
left join rocai_analysis2.tmp_acs_credit_first_borrow b 
on a.acct_id = b.acct_id
where return_ts>first_borrow_ts and first_borrow_ts is not null;

--Union Borrow & Return Sets

drop table if exists rocai_analysis2.tmp_acs_credit_borrow_return_txn;
create table rocai_analysis2.tmp_acs_credit_borrow_return_txn stored as orc as
select acct_id,
borrow_ts as transaction_time,
borrow_date as transaction_date,
borrow_id,
null as return_id,
acct_book_type_new,
borrow_amount,
expected_commission,
0 as return_amount,
0 as commission_amount,
0 as return_count
from rocai_analysis2.tmp_acs_credit_borrow_txn
union all
select acct_id,
return_ts as transaction_time,
return_date as transaction_date,
null as borrow_id,
return_id,
acct_book_type_new,
0 as borrow_amount,
0 as expected_commission,
return_amount,
commission_amount,
return_count
from rocai_analysis2.tmp_acs_credit_return_txn;

--Flagging between Borrows & Returns to create transaction groups

drop table if exists rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg ;
create table rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg stored as orc as 
SELECT *, SUM(gap) over (PARTITION BY acct_id ORDER BY transaction_time) txn_group
from(
select *,
CASE WHEN acct_book_type_new = lag(acct_book_type_new) over (PARTITION BY acct_id ORDER BY transaction_time)
then 0 else 1 end gap
from rocai_analysis2.tmp_acs_credit_borrow_return_txn) t;

--Mapping borrow with corresponding return transaction groups

drop table if exists rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg_map ;
create table rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg_map stored as orc as 
select acct_id,
txn_group,
acct_book_type_new,
borrow_amount,
borrow_count,
borrow_ts,
borrow_date,
borrow_id,
expected_commission,
borrow_other_amount,
borrow_2_amount,
borrow_5_amount,
borrow_10_amount,
borrow_15_amount,
borrow_20_amount,
borrow_30_amount,
borrow_35_amount,
borrow_50_amount,
borrow_55_amount,
borrow_100_amount,
borrow_105_amount,
borrow_200_amount,
borrow_205_amount,
borrow_250_amount,
borrow_255_amount,
borrow_300_amount,
borrow_305_amount,
borrow_400_amount,
borrow_405_amount,
borrow_500_amount,
borrow_505_amount,
borrow_600_amount,
borrow_605_amount,
borrow_700_amount,
borrow_705_amount,
borrow_1005_amount,
borrow_1505_amount,
borrow_2005_amount,

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


return_ts,
return_amount,
return_count,
return_date,
balance,
return_id,
commission_amount,
closing_balance,
last_transaction_time,
final_settlement,
final_expected_commission
from(
Select *,case when borrow_ts=last_transaction_time then closing_balance else 0 end as final_settlement,
case when borrow_ts=last_transaction_time and closing_balance<0 then expected_commission else 0 end as final_expected_commission
from(
select *,sum(balance) over (partition by acct_id order by borrow_ts rows between unbounded preceding and current row) closing_balance,
max(borrow_ts) over (partition by acct_id) last_transaction_time
from(
SELECT a.*,nvl(return_ts,cast(concat(substr('20231111',1,4),'-',substr('20231111',5,2),'-',substr('20231111',7,2),' 00:00:01') as string)) return_ts
,nvl(return_amount,0) return_amount,nvl(return_count,0) return_count,nvl(return_date,'20231111') return_date
,nvl(return_amount,0)-borrow_amount as balance,return_id,
nvl(commission_amount,0) commission_amount
from 
(select acct_id,txn_group,acct_book_type_new,sum(borrow_amount) borrow_amount,count(1) borrow_count,
min(transaction_time) as borrow_ts,min(transaction_date) as borrow_date,min(borrow_id) borrow_id,sum(expected_commission) expected_commission,
sum(case when borrow_amount not in ('2.0','5.0','10.0','15.0','20.0','30.0','35.0','50.0','55.0','100.0','105.0','200.0','205.0','250.0','255.0',
'300.0','305.0','400.0','405.0','500.0','505.0','600.0','605.0','700.0','705.0','1005.0','1505.0','2005.0') then borrow_amount else 0 end) as borrow_other_amount, 
sum(case when borrow_amount = 2 then borrow_amount else 0 end) as borrow_2_amount,
sum(case when borrow_amount = 5 then borrow_amount else 0 end) as borrow_5_amount,
sum(case when borrow_amount = 10 then borrow_amount else 0 end) as borrow_10_amount,
sum(case when borrow_amount = 15 then borrow_amount else 0 end) as borrow_15_amount,
sum(case when borrow_amount = 20 then borrow_amount else 0 end) as borrow_20_amount,
sum(case when borrow_amount = 30 then borrow_amount else 0 end) as borrow_30_amount,
sum(case when borrow_amount = 35 then borrow_amount else 0 end) as borrow_35_amount,
sum(case when borrow_amount = 50 then borrow_amount else 0 end) as borrow_50_amount,
sum(case when borrow_amount = 55 then borrow_amount else 0 end) as borrow_55_amount,
sum(case when borrow_amount = 100 then borrow_amount else 0 end) as borrow_100_amount,
sum(case when borrow_amount = 105 then borrow_amount else 0 end) as borrow_105_amount,
sum(case when borrow_amount = 200 then borrow_amount else 0 end) as borrow_200_amount,
sum(case when borrow_amount = 205 then borrow_amount else 0 end) as borrow_205_amount,
sum(case when borrow_amount = 250 then borrow_amount else 0 end) as borrow_250_amount,
sum(case when borrow_amount = 255 then borrow_amount else 0 end) as borrow_255_amount,
sum(case when borrow_amount = 300 then borrow_amount else 0 end) as borrow_300_amount,
sum(case when borrow_amount = 305 then borrow_amount else 0 end) as borrow_305_amount,

sum(case when borrow_amount = 400 then borrow_amount else 0 end) as borrow_400_amount,
sum(case when borrow_amount = 405 then borrow_amount else 0 end) as borrow_405_amount,
sum(case when borrow_amount = 500 then borrow_amount else 0 end) as borrow_500_amount,
sum(case when borrow_amount = 505 then borrow_amount else 0 end) as borrow_505_amount,
sum(case when borrow_amount = 600 then borrow_amount else 0 end) as borrow_600_amount,
sum(case when borrow_amount = 605 then borrow_amount else 0 end) as borrow_605_amount,

sum(case when borrow_amount = 700 then borrow_amount else 0 end) as borrow_700_amount,
sum(case when borrow_amount = 705 then borrow_amount else 0 end) as borrow_705_amount,

sum(case when borrow_amount = 1005 then borrow_amount else 0 end) as borrow_1005_amount,
sum(case when borrow_amount = 1505 then borrow_amount else 0 end) as borrow_1505_amount,
sum(case when borrow_amount = 2005 then borrow_amount else 0 end) as borrow_2005_amount,



sum(case when borrow_amount not in ('2.0','5.0','10.0','15.0','20.0','30.0','35.0','50.0','55.0','100.0','105.0','200.0','205.0','250.0','255.0',
'300.0','305.0','400.0','405.0','500.0','505.0','600.0','605.0','700.0','705.0','1005.0','1505.0','2005.0') then 1 else 0 end) as borrow_other_count, 
sum(case when borrow_amount = 2 then 1 else 0 end) as borrow_2_count, 
sum(case when borrow_amount = 5 then 1 else 0 end) as borrow_5_count,
sum(case when borrow_amount = 10 then 1 else 0 end) as borrow_10_count,
sum(case when borrow_amount = 15 then 1 else 0 end) as borrow_15_count,
sum(case when borrow_amount = 20 then 1 else 0 end) as borrow_20_count,
sum(case when borrow_amount = 30 then 1 else 0 end) as borrow_30_count,
sum(case when borrow_amount = 35 then 1 else 0 end) as borrow_35_count,
sum(case when borrow_amount = 50 then 1 else 0 end) as borrow_50_count,
sum(case when borrow_amount = 55 then 1 else 0 end) as borrow_55_count,
sum(case when borrow_amount = 100 then 1 else 0 end) as borrow_100_count,
sum(case when borrow_amount = 105 then 1 else 0 end) as borrow_105_count,
sum(case when borrow_amount = 200 then 1 else 0 end) as borrow_200_count,
sum(case when borrow_amount = 205 then 1 else 0 end) as borrow_205_count,
sum(case when borrow_amount = 250 then 1 else 0 end) as borrow_250_count,
sum(case when borrow_amount = 255 then 1 else 0 end) as borrow_255_count,
sum(case when borrow_amount = 300 then 1 else 0 end) as borrow_300_count,
sum(case when borrow_amount = 305 then 1 else 0 end) as borrow_305_count,
sum(case when borrow_amount = 400 then 1 else 0 end) as borrow_400_count,
sum(case when borrow_amount = 405 then 1 else 0 end) as borrow_405_count,
sum(case when borrow_amount = 500 then 1 else 0 end) as borrow_500_count,
sum(case when borrow_amount = 505 then 1 else 0 end) as borrow_505_count,
sum(case when borrow_amount = 600 then 1 else 0 end) as borrow_600_count,
sum(case when borrow_amount = 605 then 1 else 0 end) as borrow_605_count,
sum(case when borrow_amount = 700 then 1 else 0 end) as borrow_700_count,
sum(case when borrow_amount = 705 then 1 else 0 end) as borrow_705_count,
sum(case when borrow_amount = 1005 then 1 else 0 end) as borrow_1005_count,
sum(case when borrow_amount = 1505 then 1 else 0 end) as borrow_1505_count,
sum(case when borrow_amount = 2005 then 1 else 0 end) as borrow_2005_count
from rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg where acct_book_type_new = 'Borrow'
group by acct_id,txn_group,acct_book_type_new
) a 
left join 
(select acct_id,txn_group,acct_book_type_new,
sum(return_amount) return_amount,
sum(return_count) return_count,
sum(commission_amount) commission_amount,
max(transaction_time) as return_ts,
max(transaction_date) as return_date,
max(return_id) return_id
from rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg 
where acct_book_type_new = 'Return'
group by acct_id,txn_group,acct_book_type_new) b
on a.acct_id=b.acct_id and a.txn_group+1 = b.txn_group) t) x) x2;

--daily aggregation
drop table if exists rocai_analysis2.prof_acs_credit_borrow_return_acct_daily ;
create table rocai_analysis2.prof_acs_credit_borrow_return_acct_daily stored as orc as 
select *,
borrow_amount/borrow_exchange as borrow_amount_usd,
borrow_other_amount/borrow_exchange as borrow_other_amount_usd,
borrow_2_amount/borrow_exchange as borrow_2_amount_usd,
borrow_5_amount/borrow_exchange as borrow_5_amount_usd,
borrow_10_amount/borrow_exchange as borrow_10_amount_usd,
borrow_15_amount/borrow_exchange as borrow_15_amount_usd,
borrow_20_amount/borrow_exchange as borrow_20_amount_usd,
borrow_30_amount/borrow_exchange as borrow_30_amount_usd,

borrow_35_amount/borrow_exchange as borrow_35_amount_usd,

borrow_50_amount/borrow_exchange as borrow_50_amount_usd,
borrow_55_amount/borrow_exchange as borrow_55_amount_usd,
borrow_100_amount/borrow_exchange as borrow_100_amount_usd,
borrow_105_amount/borrow_exchange as borrow_105_amount_usd,
borrow_200_amount/borrow_exchange as borrow_200_amount_usd,
borrow_205_amount/borrow_exchange as borrow_205_amount_usd,
borrow_250_amount/borrow_exchange as borrow_250_amount_usd,
borrow_255_amount/borrow_exchange as borrow_255_amount_usd,
borrow_300_amount/borrow_exchange as borrow_300_amount_usd,
borrow_305_amount/borrow_exchange as borrow_305_amount_usd,
borrow_400_amount/borrow_exchange as borrow_400_amount_usd,
borrow_405_amount/borrow_exchange as borrow_405_amount_usd,
borrow_500_amount/borrow_exchange as borrow_500_amount_usd,
borrow_505_amount/borrow_exchange as borrow_505_amount_usd,
borrow_600_amount/borrow_exchange as borrow_600_amount_usd,
borrow_605_amount/borrow_exchange as borrow_605_amount_usd,
borrow_700_amount/borrow_exchange as borrow_700_amount_usd,
borrow_705_amount/borrow_exchange as borrow_705_amount_usd,

borrow_1005_amount/borrow_exchange as borrow_1005_amount_usd,
borrow_1505_amount/borrow_exchange as borrow_1505_amount_usd,
borrow_2005_amount/borrow_exchange as borrow_2005_amount_usd,



return_amount/return_exchange as return_amount_usd,
commission_amount/return_exchange as commission_amount_usd,
gross_return_amount/return_exchange as gross_return_amount_usd,
case when return_days>7 then gross_return_amount/deadline_exchange else gross_return_amount/return_exchange end as gross_return_amount_usd_deadline,
outstanding_amount/return_exchange as outstanding_amount_usd,
case when final_settlement<0 then return_days else 0 end as outstanding_days,
case when final_settlement<0 then borrow_count else 0 end as due_payments  
from(
select a.*,
datediff(from_unixtime(unix_timestamp(return_DATE ,'yyyyMMdd')),from_unixtime(unix_timestamp(BORROW_DATE ,'yyyyMMdd'))) AS return_Days,
final_expected_commission/1.145+abs(final_settlement) as outstanding_amount,
abs(final_settlement) as outstanding_amount_wo_commission,
return_amount+commission_amount/1.145 as gross_return_amount,
b.rtgs_interbank_exchange as borrow_exchange,
c.rtgs_interbank_exchange as return_exchange,
d.rtgs_interbank_exchange as deadline_exchange
from
(select acct_id,
borrow_date,
return_date,
sum(borrow_amount) borrow_amount,
sum(borrow_count) borrow_count,
sum(borrow_other_amount) borrow_other_amount,
sum(borrow_2_amount) borrow_2_amount,
sum(borrow_5_amount) borrow_5_amount,
sum(borrow_10_amount) borrow_10_amount,
sum(borrow_15_amount) borrow_15_amount,
sum(borrow_20_amount) borrow_20_amount,
sum(borrow_30_amount) borrow_30_amount,
sum(borrow_35_amount) borrow_35_amount,
sum(borrow_50_amount) borrow_50_amount,
sum(borrow_55_amount) borrow_55_amount,
sum(borrow_100_amount) borrow_100_amount,
sum(borrow_105_amount) borrow_105_amount,
sum(borrow_200_amount) borrow_200_amount,
sum(borrow_205_amount) borrow_205_amount,
sum(borrow_250_amount) borrow_250_amount,
sum(borrow_255_amount) borrow_255_amount,
sum(borrow_300_amount) borrow_300_amount,
sum(borrow_305_amount) borrow_305_amount,
sum(borrow_400_amount) borrow_400_amount,
sum(borrow_405_amount) borrow_405_amount,
sum(borrow_500_amount) borrow_500_amount,
sum(borrow_505_amount) borrow_505_amount,
sum(borrow_600_amount) borrow_600_amount,
sum(borrow_605_amount) borrow_605_amount,
sum(borrow_700_amount) borrow_700_amount,
sum(borrow_705_amount) borrow_705_amount,
sum(borrow_1005_amount) borrow_1005_amount,
sum(borrow_1505_amount) borrow_1505_amount,
sum(borrow_2005_amount) borrow_2005_amount,
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
sum(return_amount) return_amount,
sum(return_count) return_count,
sum(commission_amount/1.145) commission_amount,
sum(final_settlement) final_settlement,
sum(final_expected_commission) final_expected_commission
from rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg_map
group by acct_id,
borrow_date,
return_date) a 
Left Join rocai_analysis2.rtgs_usd_conversion_rate b
On a.borrow_date = b.part_date
Left Join rocai_analysis2.rtgs_usd_conversion_rate c
On a.return_date = c.part_date
Left Join rocai_analysis2.rtgs_usd_conversion_rate d
On (date_Format(date_add(from_unixtime(unix_timestamp(a.BORROW_DATE ,'yyyyMMdd')),7),'yyyyMMdd')) = d.part_date) t;

--Adding calling nbr 
drop table if exists rocai_analysis2.prof_acs_credit_borrow_return_daily ;
create table rocai_analysis2.prof_acs_credit_borrow_return_daily stored as orc as 
select a.*, nvl(b.msisdn_norm,concat('000000',a.acct_id)) as calling_nbr_new
from rocai_analysis2.prof_acs_credit_borrow_return_acct_daily a 
left join (Select acct_id, msisdn_norm From rocai_analysis2.tmp_z_ref_msisdn_acctid_latest_date
Where plan_name not like '%Staff%'
and plan_name not like '%Spouse%'
and plan_name not like '%Hybrid%'
and plan_name not like '%Postpaid%'
and plan_name like '%Prepaid%'
And acct_id in (Select x.acct_id From rocai_analysis2.tmp_msisdn_acct_id_mapping x)
Group By acct_id, msisdn_norm)b 
on a.acct_id=b.acct_id;