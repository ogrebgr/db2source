
CREATE TABLE test.test
(
id serial NOT NULL,
intcol integer NOT NULL,
varcharcol character varying(45) COLLATE pg_catalog."default" NOT NULL,
textcol text COLLATE pg_catalog."default" NOT NULL,
timecol time without time zone NOT NULL,
datetimecol timestamp without time zone NOT NULL,
floatcol real NOT NULL,
doublecol double precision NOT NULL,
datecol date NOT NULL,
tscol timestamp with time zone NOT NULL,
longtextcol text COLLATE pg_catalog."default" NOT NULL,
booleancol boolean NOT NULL,
CONSTRAINT test_pkey PRIMARY KEY (id)
)
WITH (
OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE test.test
OWNER to postgres;

GRANT ALL ON TABLE test.test TO postgres;

GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE test.test TO test;