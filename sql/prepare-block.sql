
DROP TABLE IF EXISTS tmp_block_source CASCADE;
CREATE TABLE tmp_block_source
(
  tradedatetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  block_trade_id VARCHAR(50),
  block_lei VARCHAR(50),
  legal_name VARCHAR(500),
  trade_type VARCHAR(50),
  tier  VARCHAR(100),
  underlying_asset  VARCHAR(100),
  asset_class  VARCHAR(100),
  base_product  VARCHAR(100),
  sub_product  VARCHAR(100),
  upi  VARCHAR(100),
  buy_or_sell  VARCHAR(10),
  amount  INTEGER,
  price  DOUBLE PRECISION,
  value  DOUBLE PRECISION,
  execution_type VARCHAR(50),
  trader_name VARCHAR(100),
  salesperson_name VARCHAR(100),
  settlement_depository VARCHAR(50),
  currency VARCHAR(10),
  trading_desk VARCHAR(50),
  transaction_location VARCHAR(100),
  gmei_entered TIMESTAMP WITHOUT TIME ZONE
);

ALTER TABLE tmp_block_source OWNER TO trades;

--fasten lookups
DROP INDEX IF EXISTS idx_block_trader;
CREATE INDEX idx_block_trader ON tmp_block_source (trader_name);

DROP INDEX IF EXISTS idx_block_salesperson;
CREATE INDEX idx_block_salesperson ON tmp_block_source (salesperson_name);

DROP INDEX IF EXISTS idx_block_currency;
CREATE INDEX idx_block_currency ON tmp_block_source (currency);

DROP INDEX IF EXISTS idx_block_depo;
CREATE INDEX idx_block_depo ON tmp_block_source (settlement_depository);

DROP INDEX IF EXISTS idx_block_et;
CREATE INDEX idx_block_et ON tmp_block_source (execution_type);

DROP INDEX IF EXISTS idx_block_tier;
CREATE INDEX idx_block_tier ON tmp_block_source (tier);

DROP INDEX IF EXISTS idx_block_asset_product_subprod;
CREATE INDEX idx_block_asset_product_subprod ON tmp_block_source (asset_class, base_product, sub_product);

DROP INDEX IF EXISTS idx_block_product;
CREATE INDEX idx_block_product ON tmp_block_source (base_product);

DROP INDEX IF EXISTS idx_block_desk;
CREATE INDEX idx_block_desk ON tmp_block_source (trading_desk);

INSERT INTO tmp_block_source (tradedatetime, block_trade_id, block_lei, legal_name, trade_type, tier, underlying_asset,
                              asset_class, base_product, sub_product, upi, buy_or_sell,
                              amount, price, value,
                              execution_type, trader_name, salesperson_name, settlement_depository, currency, trading_desk,
                              transaction_location, gmei_entered)
  SELECT
    gen_business_time(to_timestamp(s.tradedatetime, 'MM-DD-YY')::timestamp without time zone),
    block_trade_id, block_lei, legal_name, trade_type, tier, underlying_asset, asset_class, base_product, sub_product, upi, buy_or_sell,
    amount::integer, price::DOUBLE PRECISION, s.value::DOUBLE PRECISION,
    execution_type, trader_name, salesperson_name, settlement_depository, currency, trading_desk, '',
    g.bank_entered_datetime
  FROM blocktradedata_csv s left join tmp_gmei_source g on g.lei = s.block_lei;

select count(1) from tmp_block_source;

--reconstruct GMEI records missing from the block trades
INSERT INTO tmp_gmei_source (GMEI_PublicationDateTime, GMEI_LastUpdateDate,
                             Bank_Entered_Datetime,
                             LEI, LegalName, OtherName,
                             AddressLineOne, AddressLineTwo, AddressLineThree, AddressLineFour,
                             City, State, Country, PostCode, EntityStatus, Parent_LEI,
                             BackOfficeSystemID, TradingystemOneID, TradingystemTwoID, TradingystemThreeID, TradingystemFourID)
  SELECT
    gen_business_time(gen_business_day()) pt,
    null,
    null,
    b.block_lei,
    min(b.legal_name),
    '!min-out-of-'||count(b.legal_name)||'-lei-names!',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    null,
    'BO-'||upper(short_uid()),
    'T1-'||upper(short_uid()),
    'T2-'||upper(short_uid()),
    'T3-'||upper(short_uid()),
    'T4-'||upper(short_uid())
  FROM tmp_block_source b LEFT OUTER JOIN tmp_gmei_source g ON g.lei = b.block_lei
  WHERE g.bank_entered_datetime is null
  GROUP BY b.block_lei;

UPDATE tmp_gmei_source SET
  GMEI_LastUpdateDate=gen_business_time(gen_update_day(GMEI_PublicationDateTime)) ,
  Bank_Entered_Datetime=gen_business_time(gen_update_day(GMEI_PublicationDateTime))
WHERE gmei_lastupdatedate is null and tmp_gmei_source.bank_entered_datetime is NULL;

UPDATE tmp_gmei_source u SET LEI=NULL FROM
  (SELECT uid FROM tmp_gmei_source ORDER BY random() LIMIT ((SELECT count(1) FROM tmp_gmei_source)*0.05)) s
WHERE s.uid=u.uid;

-------------

UPDATE tmp_block_source b SET gmei_entered=g.bank_entered_datetime
FROM (SELECT lei, bank_entered_datetime FROM tmp_gmei_source) g
WHERE g.lei = b.block_lei;


--amount of block trade LEIs missing from the golden source
select count(1) from (
                       SELECT
                         b.block_trade_id,
                         g.bank_entered_datetime
                       FROM tmp_block_source b LEFT JOIN tmp_gmei_source g ON g.lei = b.block_lei
                     ) s
where s.bank_entered_datetime is null;