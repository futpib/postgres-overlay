
CREATE TABLE various_types
  ( id SERIAL PRIMARY KEY
  , text text NOT NULL
  , timestamp timestamp
  );

INSERT INTO various_types
SELECT generate_series, md5(random()::text), now()
FROM generate_series(0, 7);

CREATE TABLE compound_primary_key
  ( foo_id integer
  , bar_id integer
  , text text
  , PRIMARY KEY (foo_id, bar_id)
  );

INSERT INTO compound_primary_key
SELECT generate_series, generate_series % 4, md5(random()::text)
FROM generate_series(0, 7);
