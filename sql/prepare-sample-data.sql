CREATE OR REPLACE FUNCTION sample_data(in days_ahead integer, starting_from DATE) RETURNS VOID as $$
  DECLARE business_days RECORD;
  DECLARE products RECORD;
  DECLARE trade_desks RECORD;
  DECLARE bt_id TEXT;
  DECLARE bt_lei TEXT;
  DECLARE bt_legal_name TEXT;
  DECLARE bt_legal_name_entered TIMESTAMP;
  DECLARE bt_asset text;
  DECLARE bt_subprod text;
  DECLARE amount integer;
  DECLARE price DOUBLE PRECISION;
  DECLARE desk_trader_name text;
  DECLARE block_timestamp TIMESTAMP WITHOUT TIME ZONE;
BEGIN

  IF starting_from IS NULL THEN
    starting_from:=now();
  END IF;

  FOR business_days IN
  SELECT generate_series(starting_from, starting_from + cast (days_ahead||' days' as interval), '1 day') as bd
  LOOP

    DROP TABLE IF EXISTS temp_random_leis;
    create temporary table temp_random_leis as
      select g.lei, g.legalname, g.bank_entered_datetime
      FROM tmp_gmei_source g WHERE random() <= 0.1 AND g.lei is not null ORDER BY random() LIMIT 500;

    FOR trade_desks IN
    SELECT trading_desk from tmp_block_source group by 1
    LOOP

      select into desk_trader_name
        s.trader_name
      from (select trader_name from tmp_block_source group by 1) s ORDER BY random() LIMIT 1;


      FOR products IN
      SELECT trading_desk, base_product from tmp_block_source where trading_desk=trade_desks.trading_desk group by 1,2
      LOOP

        SELECT INTO  bt_lei, bt_legal_name, bt_legal_name_entered
          g.lei, g.legalname, g.bank_entered_datetime
        FROM temp_random_leis g ORDER BY random() LIMIT 1;

        SELECT INTO bt_asset, bt_subprod
          s.asset_class,  s.sub_product
        from (select asset_class, base_product, sub_product from tmp_block_source group by 1,2,3) s
        where base_product=products.base_product ORDER BY random() LIMIT 1;

        bt_id:=replace(upper(uuid_generate_v4()::text), '-', '');
        block_timestamp:=gen_business_time(business_days.bd::TIMESTAMP WITHOUT TIME ZONE);

        --RAISE DEBUG 'Sampling for day (%) with product (%) at desk(%)', business_days.bd, products.base_product, trade_desks.trading_desk;

        amount:=round(random() * 1000)::integer;
        price:=round((random() * 1000)::numeric, 4);

        INSERT INTO tmp_block_future (tradedatetime, block_trade_id, block_lei, legal_name, trade_type, tier, underlying_asset,
                                      asset_class, base_product, sub_product, upi, buy_or_sell,
                                      amount, price, value,
                                      execution_type, trader_name, salesperson_name, settlement_depository, currency, trading_desk,
                                      transaction_location, gmei_entered)
        VALUES (
          block_timestamp,
          bt_id, bt_lei, bt_legal_name, 'Block',
          (select s.tier from (select tier from tmp_block_source group by 1) s ORDER BY random() LIMIT 1),
          'Asset '||round(random()*100)::integer,
          bt_asset,
          products.base_product,
          bt_subprod,
          bt_asset||products.base_product||bt_subprod,
          case when round(random()*100)::integer<50 then 'B' else 'S' end,
          amount, price, amount*price,
          (select s.execution_type from (select execution_type from tmp_block_source group by 1) s ORDER BY random() LIMIT 1),
          desk_trader_name,
          (select s.salesperson_name from (select salesperson_name from tmp_block_source group by 1) s ORDER BY random() LIMIT 1),
          (select s.settlement_depository from (select settlement_depository from tmp_block_source group by 1) s ORDER BY random() LIMIT 1),
          (select s.currency from (select currency from tmp_block_source group by 1) s ORDER BY random() LIMIT 1),
          trade_desks.trading_desk,
          'custom',
          bt_legal_name_entered);



      END LOOP;

    END LOOP;



    INSERT INTO tmp_allocation_future (block_trade_id, amount, price, allocation_id, trade_type, settlement_depository,
                                       block_lei, allocation_lei, allocation_datetime,
                                       settlement_date, settlement_confirm_number, settlement_confirm_date, gmei_entered)
      SELECT
        blocks.block_trade_id,
        round((random() * 1000)::NUMERIC,2), round((random() * 100000)::numeric, 4),
        replace(upper(uuid_generate_v4()::text), '-', ''), blocks.trade_type, blocks.settlement_depository,
        blocks.block_lei, leis.lei, gen_business_time(gen_window_day(blocks.tradedatetime, 1, 1)),
        blocks.tradedatetime + interval '3 days', random()*10000000, gen_business_time(gen_window_day(blocks.tradedatetime, 1, 5)),
        case when round(random() * 100)::integer < (90+extract(day from blocks.tradedatetime)-20) then leis.bank_entered_datetime else null end
      FROM
        (
          WITH t AS (SELECT *, row_number() OVER () AS rn from tmp_block_future where transaction_location='custom')
          SELECT row_number() OVER () as jid, * FROM (
                                                       SELECT trunc(random() * (SELECT max(rn) FROM t))::int + 1 AS rn
                                                       FROM   generate_series(1, 32400) g
                                                     ) r
            JOIN   t USING (rn)
        ) blocks --- N random block trade records
        INNER JOIN (

                     WITH gmei_src AS (SELECT *, row_number() OVER () AS rn from tmp_gmei_source where lei is not null and lei<>'')
                     SELECT row_number() OVER () as jid, * FROM (
                                                                  SELECT trunc(random() * (SELECT max(rn) FROM gmei_src))::int + 1 AS rn
                                                                  FROM   generate_series(1, 32400) g
                                                                ) gmei_randoms
                       JOIN   gmei_src USING (rn)
                   ) leis --- N random LEI records
          ON blocks.jid=leis.jid;

  END LOOP;
  RETURN;

END;
$$ LANGUAGE plpgsql;

-----------------------------------------------------------
----------------MAIN GENERATION----------------------------
-----------------------------------------------------------
--select sample_data(30, '2017-04-10'::DATE);
--generate allocation records for 60 days in future starting 2 days before now
select sample_data(60, (now() - '2 days'::interval)::date);
-----------------------------------------------------------
-----------------------------------------------------------
-----------------------------------------------------------
--added block records
select count(*) from tmp_block_future where transaction_location='custom';
--added allocation records
select count(a.*) from tmp_allocation_future a inner join tmp_block_future b using (block_trade_id)
where b.transaction_location='custom';