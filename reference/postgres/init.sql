-- Storage for the reference pipeline: the last stop before the API.

CREATE TABLE readings (
  device          text        NOT NULL,
  site            text        NOT NULL,
  seq             bigint      NOT NULL,
  observed_at     timestamptz NOT NULL,
  temperature_k   double precision NOT NULL,
  pressure_pa     integer     NOT NULL,
  vibration_mm_s  double precision NOT NULL,
  stored_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (device, seq)
);
CREATE INDEX readings_device_time ON readings (device, observed_at DESC);
CREATE INDEX readings_site_time ON readings (site, observed_at DESC);

CREATE TABLE site_minute (
  site                  text        NOT NULL,
  window_start          timestamptz NOT NULL,
  window_end            timestamptz NOT NULL,
  readings              integer     NOT NULL,
  devices               integer     NOT NULL,
  median_temperature_k  double precision NOT NULL,
  median_pressure_pa    integer     NOT NULL,
  stored_at             timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (site, window_start)
);
