CREATE TABLE IF NOT EXISTS punches (
 chipNumber integer NOT NULL,
 stationCode integer NOT NULL,
 stage text NOT NULL,
 time integer NOT NULL,
 primary key (chipNumber, stationCode, stage)
);

CREATE INDEX chipIdx ON punches (chipNumber);
CREATE INDEX stationIdx ON punches (stationCode);