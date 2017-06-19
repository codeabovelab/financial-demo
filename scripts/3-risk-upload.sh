#!/usr/bin/env bash

scp -P 2100 dist/BlockTradeFuture.csv dist/AllocationsFuture.csv codeabovelab@demo.arcadiadata.com:/home/codeabovelab/trade-import/

ssh codeabovelab@demo.arcadiadata.com -p 2100 <<'ENDSSH'
#commands to run on arcadia host
cd trade-import
hdfs dfs -mkdir /tmp/import/databases/trade-block-future
hdfs dfs -copyFromLocal -f BlockTradeFuture.csv /tmp/import/databases/trade-block-future/

hdfs dfs -mkdir /tmp/import/databases/trade-allocations-future
hdfs dfs -copyFromLocal -f AllocationsFuture.csv /tmp/import/databases/trade-allocations-future/

beeline  -u "jdbc:hive2://10.12.0.21:31050/default;auth=noSasl" -n arcadia -e "
drop table if exists trade_block_future;
create external table trade_block_future (
  tradedatetime timestamp,
  block_trade_id string,
  block_lei string,
  legal_name string,
  trade_type string,
  tier string,
  underlying_asset string,
  asset_class string,
  base_product string,
  sub_product string,
  upi string,
  buy_or_sell string,
  amount int,
  price double,
  value double,
  execution_type string,
  trader_name string,
  salesperson_name string,
  settlement_depository string,
  currency string,
  trading_desk string,
  transaction_location string,
  gmei_entered timestamp
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/tmp/import/databases/trade-block-future/';

drop table if exists trade_allocations_future;
create external table trade_allocations_future (
  allocation_tradedatetime timestamp,
  block_trade_id string,
  amount double,
  price double,
  allocation_id string,
  trade_type string,
  settlement_depository string,
  block_lei string,
  allocation_lei string,
  settlement_date timestamp,
  settlement_confirm_number int,
  settlement_confirm_date timestamp,
  gmei_entered timestamp
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/tmp/import/databases/trade-allocations-future/';

DROP VIEW IF EXISTS trade_allocations_risk_view;
create view trade_allocations_risk_view as
  select
    a.allocation_id,
    CASE
    WHEN (a.price*a.amount)<=100 THEN 1
    WHEN (a.price*a.amount)<=1000 and (a.price*a.amount)>100 THEN 1.2
    WHEN (a.price*a.amount)<=10000 and (a.price*a.amount)>1000 THEN 1.5
    WHEN (a.price*a.amount)<=100000 and (a.price*a.amount)>10000 THEN 2
    WHEN (a.price*a.amount)<=1000000 and (a.price*a.amount)>100000 THEN 3
    WHEN (a.price*a.amount)<=10000000 and (a.price*a.amount)>1000000 THEN 4
    WHEN (a.price*a.amount)<=100000000 and (a.price*a.amount)>10000000 THEN 5
    WHEN (a.price*a.amount)<=1000000000 and (a.price*a.amount)>100000000 THEN 6
    WHEN (a.price*a.amount)>1000000000 THEN 7
    ELSE 0
    END as value_risk,
    case when a.gmei_entered is null then 2 else 1 end as ctpty_risk,
    CASE b.base_product
    WHEN 'Agricultural' THEN 2.6
    WHEN 'CapFloor' THEN 2
    WHEN 'Complex Exotic' THEN 3.5
    WHEN 'Contract For Difference' THEN 3.8
    WHEN 'Cross Currency' THEN 2.1
    WHEN 'Energy' THEN 2.5
    WHEN 'Environmental' THEN 2.7
    WHEN 'Exotic' THEN 1.7
    WHEN 'FRA' THEN 1.9
    WHEN 'Forward' THEN 2.3
    WHEN 'Freight' THEN 2.8
    WHEN 'IR Swap' THEN 1.8
    WHEN 'Index' THEN 1.3
    WHEN 'Index Tranche' THEN 1.2
    WHEN 'Metals' THEN 2.4
    WHEN 'Multi Commodity Exotic' THEN 2.9
    WHEN 'NDF' THEN 3.1
    WHEN 'NDO' THEN 3.2
    WHEN 'Option' THEN 2.2
    WHEN 'Other' THEN 3.9
    WHEN 'Portfolio Swap' THEN 3.7
    WHEN 'Simple Exotic' THEN 3.4
    WHEN 'Single Name' THEN 1.1
    WHEN 'Spot' THEN 1
    WHEN 'Swap' THEN 3.6
    WHEN 'Swaptions' THEN 1.6
    WHEN 'Total Return Swap' THEN 1.4
    WHEN 'Total Return Swap Index' THEN 1.5
    WHEN 'Vanilla Option' THEN 3.3
    ELSE 0
    END as product_risk,
    case when datediff(a.settlement_date,now())<0 then 6 else case datediff(a.settlement_date, now()) when 3 then 3 when 2 then 3 when 1 then 4 when 0 then 5 else 1 end end as due_date_risk
  from trade_allocations_future a inner join trade_block_future b on (b.block_trade_id=a.block_trade_id);

DROP VIEW IF EXISTS trade_allocations_dk_risk_view ;
create view trade_allocations_dk_risk_view as
  select *, value_risk*product_risk*due_date_risk*ctpty_risk as dk_risk from trade_allocations_risk_view;

DROP VIEW IF EXISTS trade_allocations_dk_risk_stat ;
create view trade_allocations_dk_risk_stat as
  select min(dk_risk) as min_dk_risk, max(dk_risk) as max_dk_risk, avg(dk_risk) as avg_dk_risk, stddev(dk_risk) as stddev_risk  from trade_allocations_dk_risk_view;

DROP VIEW IF EXISTS trade_allocations_future_risk;
create view trade_allocations_future_risk as
select r.*, s.avg_dk_risk, s.stddev_risk, ((dk_risk-s.min_dk_risk)/(s.max_dk_risk-s.min_dk_risk))*20 as min_max_dk_risk
from trade_allocations_dk_risk_view r
  inner join trade_allocations_dk_risk_stat s on (1=1);

"
ENDSSH