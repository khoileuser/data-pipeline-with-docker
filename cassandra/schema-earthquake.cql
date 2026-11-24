USE kafkapipeline;
CREATE TABLE IF NOT EXISTS kafkapipeline.earthquakedata (
  id TEXT,
  place TEXT,
  magnitude DOUBLE,
  mag_type TEXT,
  event_time TIMESTAMP,
  updated_time TIMESTAMP,
  latitude DOUBLE,
  longitude DOUBLE,
  depth_km DOUBLE,
  significance INT,
  felt_reports INT,
  alert_level TEXT,
  tsunami INT,
  status TEXT,
  event_type TEXT,
  network TEXT,
  url TEXT,
  PRIMARY KEY (id)
);
