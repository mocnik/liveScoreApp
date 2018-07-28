CREATE TABLE IF NOT EXISTS punches (
 chipNumber integer NOT NULL,
 stationCode integer NOT NULL,
 time integer NOT NULL
);

CREATE INDEX chipIdx ON punches (chipNumber);
CREATE INDEX stationIdx ON punches (stationCode);