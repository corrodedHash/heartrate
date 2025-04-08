 CREATE TABLE activity_log
  (
     activitytimestamp TIMESTAMP WITH time zone NOT NULL,
     activityseconds   INTEGER NOT NULL,
     activitytype      VARCHAR(12) NOT NULL,
     heartrateaverage  SMALLINT NOT NULL,
     heartratemax      SMALLINT NOT NULL
  );