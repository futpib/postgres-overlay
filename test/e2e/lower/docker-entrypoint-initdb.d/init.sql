
CREATE TABLE various_types
  ( id SERIAL PRIMARY KEY
  , text text NOT NULL DEFAULT 'default_text'
  , timestamp timestamp
  , array_array_integer integer[][]
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

CREATE TABLE no_primary_key
  ( text text
  , integer integer
  );

INSERT INTO no_primary_key
SELECT md5(random()::text)
FROM generate_series(0, 7);

CREATE TABLE U&"weird_name_\0441\043B\043E\043D_$1"
  ( U&"id1_\0441\043B\043E\043D_$1" SERIAL
  , "id2_\n$\r\'\t`\b_$1" SERIAL
  , text text
  , PRIMARY KEY (U&"id1_\0441\043B\043E\043D_$1", "id2_\n$\r\'\t`\b_$1")
  );

INSERT INTO U&"weird_name_\0441\043B\043E\043D_$1"
VALUES (3, 3, 'initial_text_value')
