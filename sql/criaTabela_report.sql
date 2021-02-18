CREATE TABLE IF NOT EXISTS report (
	city_ibge_code bigint NOT NULL,
	report_date date NOT NULL,
	epidemiological_week bigint NOT NULL,
	new_confirmed int NOT NULL,
	new_deaths int NOT NULL,
	CONSTRAINT report_pk PRIMARY KEY (city_ibge_code, report_date),
    CONSTRAINT report_city_data UNIQUE (city_ibge_code, report_date),
    CONSTRAINT report_fk0 FOREIGN KEY (city_ibge_code)
        REFERENCES sc_covid.municipio (city_ibge_code) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
) WITH (
  OIDS=FALSE
);