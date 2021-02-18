CREATE TABLE IF NOT EXISTS municipio (
	city_ibge_code bigint NOT NULL,
	city varchar(128) NOT NULL,
	state varchar(2) NOT NULL,
	estimated_population_2019 bigint NOT NULL,
	CONSTRAINT municipio_pk PRIMARY KEY (city_ibge_code)
) WITH (
  OIDS=FALSE
);