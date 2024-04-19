--Last 7 months of borrow txns
drop table if exists rocai_analysis2.tmp_acs_credit_borrow_txn_usd;
create table rocai_analysis2.tmp_acs_credit_borrow_txn_usd stored as orc as
select a.*,nvl(b.expected_commission,0) expected_commission
from
(select *,'Borrow' as acct_book_type_new
from rocai_analysis2.prof_acs_acct_credit_borrowers
where substr(borrow_date,1,6)>='202305' 
and borrow_date<='20231111' and acct_res_id = '480') a 
left join (select * from rocai_analysis2.prof_acs_commission_rates  where acct_res_id = '480')  b 
on a.borrow_amount=b.borrow_amt;


--First borrow msisdn level in last 7 months
drop table if exists rocai_analysis2.tmp_acs_credit_first_borrow_usd;
create table rocai_analysis2.tmp_acs_credit_first_borrow_usd stored as orc as
select acct_id,min(borrow_ts) first_borrow_ts
from rocai_analysis2.tmp_acs_credit_borrow_txn_usd
group by acct_id;

--Last 7 months of return & excluding repayments for borrows before 7 months

drop table if exists rocai_analysis2.tmp_acs_credit_return_txn_usd;
create table rocai_analysis2.tmp_acs_credit_return_txn_usd stored as orc as
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
left join rocai_analysis2.tmp_acs_credit_first_borrow_usd b 
on a.acct_id = b.acct_id
where return_ts>first_borrow_ts and first_borrow_ts is not null;

--Union Borrow & Return Sets

drop table if exists rocai_analysis2.tmp_acs_credit_borrow_return_txn_usd;
create table rocai_analysis2.tmp_acs_credit_borrow_return_txn_usd stored as orc as
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
from rocai_analysis2.tmp_acs_credit_borrow_txn_usd
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
from rocai_analysis2.tmp_acs_credit_return_txn_usd;

--Flagging between Borrows & Returns to create transaction groups

drop table if exists rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg_usd ;
create table rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg_usd stored as orc as 
SELECT *, SUM(gap) over (PARTITION BY acct_id ORDER BY transaction_time) txn_group
from(
select *,
CASE WHEN acct_book_type_new = lag(acct_book_type_new) over (PARTITION BY acct_id ORDER BY transaction_time)
then 0 else 1 end gap
from rocai_analysis2.tmp_acs_credit_borrow_return_txn_usd) t;

--Mapping borrow with corresponding return transaction groups

drop table if exists rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg_map_usd ;
create table rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg_map_usd stored as orc as 
select acct_id,
txn_group,
acct_book_type_new,
borrow_amount,
borrow_count,
borrow_ts,
borrow_date,
borrow_id,
expected_commission,
borrow_05_amount,
borrow_075_amount,
borrow_1_amount,
borrow_2_amount,
borrow_3_amount,
borrow_5_amount,
borrow_7_amount,
borrow_10_amount,
borrow_05_count, 
borrow_075_count,
borrow_1_count,
borrow_2_count,
borrow_3_count,
borrow_5_count,
borrow_7_count,
borrow_10_count,
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
(select acct_id,
txn_group,
acct_book_type_new,
sum(borrow_amount) borrow_amount,
count(1) borrow_count,
min(transaction_time) as borrow_ts,
min(transaction_date) as borrow_date,
min(borrow_id) borrow_id,
sum(expected_commission) expected_commission,
sum(case when borrow_amount = 0.5 then borrow_amount else 0 end) as borrow_05_amount,
sum(case when borrow_amount = 0.75 then borrow_amount else 0 end) as borrow_075_amount,
sum(case when borrow_amount = 1 then borrow_amount else 0 end) as borrow_1_amount,
sum(case when borrow_amount = 2 then borrow_amount else 0 end) as borrow_2_amount,
sum(case when borrow_amount = 3 then borrow_amount else 0 end) as borrow_3_amount,
sum(case when borrow_amount = 5 then borrow_amount else 0 end) as borrow_5_amount,
sum(case when borrow_amount = 7 then borrow_amount else 0 end) as borrow_7_amount,
sum(case when borrow_amount = 10 then borrow_amount else 0 end) as borrow_10_amount,
sum(case when borrow_amount = 0.5 then 1 else 0 end) as borrow_05_count, 
sum(case when borrow_amount = 0.75 then 1 else 0 end) as borrow_075_count,
sum(case when borrow_amount = 1 then 1 else 0 end) as borrow_1_count,
sum(case when borrow_amount = 2 then 1 else 0 end) as borrow_2_count,
sum(case when borrow_amount = 3 then 1 else 0 end) as borrow_3_count,
sum(case when borrow_amount = 5 then 1 else 0 end) as borrow_5_count,
sum(case when borrow_amount = 7 then 1 else 0 end) as borrow_7_count,
sum(case when borrow_amount = 10 then 1 else 0 end) as borrow_10_count
from rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg_usd where acct_book_type_new = 'Borrow'
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
from rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg_usd 
where acct_book_type_new = 'Return'
group by acct_id,txn_group,acct_book_type_new) b
on a.acct_id=b.acct_id and a.txn_group+1 = b.txn_group) t) x) x2;



--daily aggregation
drop table if exists rocai_analysis2.prof_acs_credit_borrow_return_acct_daily_usd ;
create table rocai_analysis2.prof_acs_credit_borrow_return_acct_daily_usd stored as orc as 
select *,
borrow_amount/borrow_exchange as borrow_amount_usd,
borrow_05_amount/borrow_exchange as borrow_05_amount_usd,
borrow_075_amount/borrow_exchange as borrow_075_amount_usd,
borrow_1_amount/borrow_exchange as borrow_1_amount_usd,
borrow_2_amount/borrow_exchange as borrow_2_amount_usd,
borrow_3_amount/borrow_exchange as borrow_3_amount_usd,
borrow_5_amount/borrow_exchange as borrow_5_amount_usd,
borrow_7_amount/borrow_exchange as borrow_7_amount_usd,
borrow_10_amount/borrow_exchange as borrow_10_amount_usd,
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
final_expected_commission/1.265+abs(final_settlement) as outstanding_amount,
abs(final_settlement) as outstanding_amount_wo_commission,
return_amount+commission_amount/1.265 as gross_return_amount,
b.rtgs_interbank_exchange as borrow_exchange,
c.rtgs_interbank_exchange as return_exchange,
d.rtgs_interbank_exchange as deadline_exchange
from
(select acct_id,
borrow_date,
return_date,
sum(borrow_amount) borrow_amount,
sum(borrow_count) borrow_count,
sum(borrow_05_amount) borrow_05_amount,
sum(borrow_075_amount) borrow_075_amount,
sum(borrow_1_amount) borrow_1_amount,
sum(borrow_2_amount) borrow_2_amount,
sum(borrow_3_amount) borrow_3_amount,
sum(borrow_5_amount) borrow_5_amount,
sum(borrow_7_amount) borrow_7_amount,
sum(borrow_10_amount) borrow_10_amount,
sum(borrow_05_count) borrow_05_count, 
sum(borrow_075_count) borrow_075_count,
sum(borrow_1_count) borrow_1_count,
sum(borrow_2_count) borrow_2_count,
sum(borrow_3_count) borrow_3_count,
sum(borrow_5_count) borrow_5_count,
sum(borrow_7_count) borrow_7_count,
sum(borrow_10_count) borrow_10_count,
sum(return_amount) return_amount,
sum(return_count) return_count,
sum(commission_amount/1.265) commission_amount,
sum(final_settlement) final_settlement,
sum(final_expected_commission) final_expected_commission
from rocai_analysis2.tmp_acs_credit_borrow_return_txn_flg_map_usd
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
drop table if exists rocai_analysis2.prof_acs_credit_borrow_return_daily_usd ;
create table rocai_analysis2.prof_acs_credit_borrow_return_daily_usd stored as orc as 
select a.*, nvl(b.msisdn_norm,concat('000000',a.acct_id)) as calling_nbr_new
from rocai_analysis2.prof_acs_credit_borrow_return_acct_daily_usd a 
left join (Select acct_id, msisdn_norm From rocai_analysis2.tmp_z_ref_msisdn_acctid_latest_date
Where plan_name not like '%Staff%'
and plan_name not like '%Spouse%'
and plan_name not like '%Hybrid%'
and plan_name not like '%Postpaid%'
and plan_name like '%Prepaid%'
And acct_id in (Select x.acct_id From rocai_analysis2.tmp_msisdn_acct_id_mapping x)
Group By acct_id, msisdn_norm)b 
on a.acct_id=b.acct_id;