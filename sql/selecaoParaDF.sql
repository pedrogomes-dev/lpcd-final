select m.* , r.report_date as date, r.epidemiological_week, r.new_confirmed, r.new_deaths,
a.is_last, a.is_repeated, a.last_available_confirmed, a.last_available_confirmed_per_100k_inhabitants, 
a.last_available_date, a.last_available_death_rate, a.last_available_deaths, a.order_for_place
	from sc_covid.municipio m 
		JOIN sc_covid.report r ON m.city_ibge_code = r.city_ibge_code
		JOIN sc_covid.about_report a ON a.city_ibge_code = r.city_ibge_code AND a.report_date = r.report_date;