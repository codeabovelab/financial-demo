CREATE OR REPLACE FUNCTION gen_window_day(in original_day TIMESTAMP WITHOUT TIME ZONE, in min_days integer, in max_days integer) returns timestamp WITHOUT TIME ZONE as $$
DECLARE early_date TIMESTAMP WITHOUT TIME ZONE;
  DECLARE late_date TIMESTAMP WITHOUT TIME ZONE;
BEGIN
  early_date := original_day + cast (min_days||' days' as interval) ;
  late_date := original_day + cast (max_days||' days' as interval) ;
  return early_date + random()*(late_date-early_date);
END;
$$ LANGUAGE plpgsql;

DROP TABLE IF EXISTS tmp_allocation_source CASCADE;
CREATE TABLE tmp_allocation_source
(
  allocation_tradedatetime TIMESTAMP WITHOUT TIME ZONE,
  block_trade_id VARCHAR(50),
  amount  FLOAT,
  price  DOUBLE PRECISION,
  allocation_id VARCHAR(50),
  trade_type VARCHAR(50),
  settlement_depository VARCHAR(50),
  block_lei VARCHAR(50),
  allocation_lei VARCHAR(50),
  settlement_date TIMESTAMP WITHOUT TIME ZONE,
  settlement_confirm_number INTEGER,
  settlement_confirm_date TIMESTAMP WITHOUT TIME ZONE,
  gmei_entered TIMESTAMP WITHOUT TIME ZONE
);

ALTER TABLE tmp_allocation_source OWNER TO trades;

INSERT INTO tmp_allocation_source (block_trade_id, amount, price, allocation_id, trade_type, settlement_depository,
                                   block_lei, allocation_lei, allocation_tradedatetime,
                                   settlement_date, settlement_confirm_number, settlement_confirm_date, gmei_entered)
  SELECT
    a.block_trade_id, a.amount::FLOAT, a.price::DOUBLE PRECISION, a.allocation_id, a.trade_type, a.settlement_depository,
    a.block_lei, a.allocation_lei, gen_business_time(gen_window_day(b.tradedatetime, 1, 30)),
    b.tradedatetime + interval '3 days', random()*10000000, gen_business_time(gen_window_day(b.tradedatetime, 1, 5)),
    g.bank_entered_datetime
  FROM allocationstradedata_csv a INNER JOIN tmp_block_source b on b.block_trade_id=a.block_trade_id
    left join tmp_gmei_source g on g.lei = a.allocation_lei;

select count(1) from tmp_allocation_source;

-- pick random LEIs from the golden source and assign to allocation records missing allocation_lei value
ALTER TABLE tmp_allocation_source ADD COLUMN manual_lei BOOLEAN NOT NULL DEFAULT FALSE;

-- slow! takes a hour
UPDATE tmp_allocation_source u
SET manual_lei=True, allocation_lei=(select g.lei from tmp_gmei_source g WHERE random() <= 0.1 AND g.lei is not null and g.lei<>u.block_lei ORDER BY random() LIMIT 1)
WHERE
  (
    (extract(day from u.allocation_tradedatetime) < 20 AND round(random() * 100)::integer < (90+extract(day from u.allocation_tradedatetime)-20) )
    OR
    (extract(day from u.allocation_tradedatetime) >= 20 AND round(random() * 100)::integer < (90+extract(day from u.allocation_tradedatetime)-20) )
  )
  AND
  u.allocation_lei='';

UPDATE tmp_allocation_source a SET gmei_entered=g.bank_entered_datetime
FROM (SELECT lei, bank_entered_datetime FROM tmp_gmei_source) g
WHERE g.lei = a.allocation_lei;


--amount of block trade LEIs missing from the golden source
select count(1) from (
                       SELECT
                         b.block_trade_id,
                         g.bank_entered_datetime
                       FROM tmp_allocation_source b LEFT JOIN tmp_gmei_source g ON g.lei = b.allocation_lei
                     ) s
where s.bank_entered_datetime is null;