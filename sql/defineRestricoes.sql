ALTER TABLE report ADD CONSTRAINT report_fk0 FOREIGN KEY (city_ibge_code) REFERENCES municipio(city_ibge_code);

ALTER TABLE about_report ADD CONSTRAINT about_report_fk0 FOREIGN KEY (city_ibge_code) REFERENCES report(city_ibge_code);
ALTER TABLE about_report ADD CONSTRAINT about_report_fk1 FOREIGN KEY (report_date) REFERENCES report(report_date);