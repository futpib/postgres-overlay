
CREATE TABLE various_types
  ( id SERIAL PRIMARY KEY
  , text text NOT NULL
  , timestamp timestamp
  );

INSERT INTO various_types
SELECT generate_series, md5(random()::text), now()
FROM generate_series(0, 7);
