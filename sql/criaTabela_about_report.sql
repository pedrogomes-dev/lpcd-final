
CREATE TABLE IF NOT EXISTS about_report (
	city_ibge_code bigint NOT NULL,
	report_date date NOT NULL,
	is_last bool NOT NULL,
	is_repeated bool NOT NULL,
	last_available_confirmed bigint NOT NULL,
	last_available_confirmed_per_100k_inhabitants float4 NOT NULL,
	last_available_date date NOT NULL,
	last_available_death_rate float4 NOT NULL,
	last_available_deaths bigint NOT NULL,
	order_for_place bigint NOT NULL,
	CONSTRAINT about_report_pk PRIMARY KEY (city_ibge_code, report_date),
    CONSTRAINT about_report_fk0 FOREIGN KEY (city_ibge_code, report_date)
        REFERENCES sc_covid.report (city_ibge_code, report_date) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
) WITH (
  OIDS=FALSE
);

