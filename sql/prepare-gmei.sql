--remove duplicates from the imported table - optional
--DELETE FROM gmeiissuedfullfile_csv USING gmeiissuedfullfile_csv a
--WHERE gmeiissuedfullfile_csv.ns1_lei = a.ns1_lei AND gmeiissuedfullfile_csv.ns1_legalname = a.ns1_legalname
--      and gmeiissuedfullfile_csv.ctid < a.ctid;


DROP TABLE IF EXISTS tmp_business_days CASCADE;
CREATE TABLE tmp_business_days
(
  business_day TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

INSERT INTO tmp_business_days (business_day)
  SELECT
    t.some_day
  from
    (select generate_series(now() - cast (60||' months' as interval), now(), '1 day') as some_day) t
  where extract(isodow from some_day) < 6;

CREATE OR REPLACE FUNCTION short_uid() returns text as $$
DECLARE key text;
BEGIN
  key := encode(gen_random_bytes(6), 'hex');
  RETURN key;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gen_business_day() returns timestamp WITHOUT TIME ZONE as $$
DECLARE business_day TIMESTAMP WITHOUT TIME ZONE;
BEGIN
  SELECT INTO business_day
    p.business_day
  FROM tmp_business_days p
  ORDER BY random()
  LIMIT 1;

  RETURN business_day;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION gen_next_year_business_day() returns timestamp WITHOUT TIME ZONE as $$
DECLARE business_day TIMESTAMP WITHOUT TIME ZONE;
BEGIN
  SELECT INTO business_day
    s.some_day
  from
    (select generate_series(now(), now() + cast (12||' months' as interval), '1 day') as some_day) s
  where extract(isodow from s.some_day) < 6
  ORDER BY random()
  LIMIT 1;

  RETURN business_day;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION gen_update_day(in original_day TIMESTAMP WITHOUT TIME ZONE) returns timestamp WITHOUT TIME ZONE as $$
DECLARE safe_range integer;
  DECLARE end_day TIMESTAMP WITHOUT TIME ZONE;
BEGIN
  safe_range := 365;
  end_day := original_day + random()*(now()-original_day);
  --if estimated update date is more than a year ago (not within a "safe_range" days) - prefer a closer date in 50% cases
  if extract(day from now()-end_day) > safe_range AND random()>0.5 THEN
    end_day := now() - cast (safe_range||' days' as interval);
    end_day := end_day + random()*(now()-end_day);
  END IF;
  return end_day;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION gen_business_time(in business_day TIMESTAMP WITHOUT TIME ZONE) returns timestamp WITHOUT TIME ZONE as $$
DECLARE start_time TIMESTAMP WITHOUT TIME ZONE;
  DECLARE end_time TIMESTAMP WITHOUT TIME ZONE;
BEGIN
  start_time := date_trunc('day', business_day) + time '09:00';
  end_time := date_trunc('day', business_day) + time '18:00';
  return start_time + random()*(end_time-start_time);
END;
$$ LANGUAGE plpgsql;

DROP TABLE IF EXISTS tmp_gmei_source CASCADE;
CREATE TABLE tmp_gmei_source
(
  GMEI_PublicationDateTime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  GMEI_LastUpdateDate TIMESTAMP WITHOUT TIME ZONE,
  Bank_Entered_Datetime TIMESTAMP WITHOUT TIME ZONE,
  Bank_Last_Update_Datetime TIMESTAMP WITHOUT TIME ZONE,
  LEI VARCHAR(50),
  LegalName VARCHAR(500),
  OtherName VARCHAR(500),
  AddressLineOne  VARCHAR(100),
  AddressLineTwo  VARCHAR(100),
  AddressLineThree  VARCHAR(100),
  AddressLineFour  VARCHAR(100),
  City  VARCHAR(100),
  State  VARCHAR(100),
  Country  VARCHAR(100),
  PostCode  VARCHAR(20),
  EntityStatus  VARCHAR(100),
  Parent_LEI VARCHAR(50),
  BackOfficeSystemID VARCHAR(50),
  TradingystemOneID VARCHAR(50),
  TradingystemTwoID VARCHAR(50),
  TradingystemThreeID VARCHAR(50),
  TradingystemFourID VARCHAR(50),
  CONSTRAINT uk_bo UNIQUE(BackOfficeSystemID),
  CONSTRAINT uk_t1 UNIQUE(TradingystemOneID),
  CONSTRAINT uk_t2 UNIQUE(TradingystemTwoID),
  CONSTRAINT uk_t3 UNIQUE(TradingystemThreeID),
  CONSTRAINT uk_t4 UNIQUE(TradingystemFourID)
);

ALTER TABLE tmp_gmei_source OWNER TO trades;

INSERT INTO tmp_gmei_source (GMEI_PublicationDateTime, GMEI_LastUpdateDate,
                             Bank_Entered_Datetime,
                             LEI, LegalName, OtherName,
                             AddressLineOne, AddressLineTwo, AddressLineThree, AddressLineFour,
                             City, State, Country, PostCode, EntityStatus, Parent_LEI,
                             BackOfficeSystemID, TradingystemOneID, TradingystemTwoID, TradingystemThreeID, TradingystemFourID)
  SELECT
    to_timestamp(s.publicationdatetime, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    to_timestamp(s.lastupdatedate, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    gen_business_time(gen_business_day()),
    s.ns1_lei,
    s.ns1_legalname,
    s.ns1_othername,
    s.ns1_addresslineone,
    s.ns1_addresslinetwo,
    s.ns1_addresslinethree,
    s.ns1_addresslinefour,
    s.ns1_city,
    s.ns1_state,
    s.ns1_country,
    s.ns1_postcode,
    s.ns1_entitystatus,
    s.ns1_relatedlei,
    'BO-'||upper(short_uid()),
    'T1-'||upper(short_uid()),
    'T2-'||upper(short_uid()),
    'T3-'||upper(short_uid()),
    'T4-'||upper(short_uid())
  FROM gmeiissuedfullfile_csv s
  ORDER BY s.lastupdatedate desc;
  --LIMIT ((SELECT count(1) FROM gmeiissuedfullfile_csv)*0.90);

select count(1) from tmp_gmei_source;

ALTER TABLE tmp_gmei_source ADD COLUMN uid text NOT NULL DEFAULT uuid_generate_v4();

UPDATE tmp_gmei_source SET Bank_Last_Update_Datetime=gen_business_time(gen_update_day(Bank_Entered_Datetime));

--fasten GMEI lookups
DROP INDEX IF EXISTS idx_gmei_source;
CREATE UNIQUE INDEX idx_gmei_source ON tmp_gmei_source (lei);
