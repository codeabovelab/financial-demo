DROP TABLE IF EXISTS tmp_block_future CASCADE;
CREATE TABLE tmp_block_future AS
SELECT * FROM tmp_block_source;

UPDATE tmp_block_future SET tradedatetime=tradedatetime + interval '11 months';

SELECT count(1) FROM tmp_block_future;

DROP TABLE IF EXISTS tmp_allocation_future CASCADE;
CREATE TABLE tmp_allocation_future AS
  SELECT gen_business_time(gen_window_day(b.tradedatetime, 1, 2)) as allocation_datetime,
         a.block_trade_id,
         a.amount,
         a.price,
         a.allocation_id,
         a.trade_type,
         a.settlement_depository,
         a.block_lei,
         a.allocation_lei,
         b.tradedatetime + interval '3 days' as settlement_date,
         a.settlement_confirm_number,
         gen_business_time(gen_window_day(b.tradedatetime, 1, 5)) as settlement_confirm_date,
         a.gmei_entered
  FROM tmp_allocation_source a inner join tmp_block_future b on b.block_trade_id=a.block_trade_id;

UPDATE tmp_allocation_future set price = price * -1 WHERE price<0;

UPDATE tmp_allocation_future u SET settlement_confirm_number=null, settlement_confirm_date=null FROM
       (SELECT allocation_id FROM tmp_allocation_future ORDER BY random() LIMIT ((SELECT count(1) FROM tmp_allocation_future)*0.50)) s
WHERE s.allocation_id=u.allocation_id;

SELECT count(1) FROM tmp_allocation_future;


