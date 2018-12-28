INSERT INTO basis.maengder(
	element_kode,
	beskrivelse,
	alt_enhed,
	objekt_type,
	enhedspris,
	aktiv,
	maengde_sql,
	source_schema,
	source_table,
	source_column,
	source_label,
	source_column_pris,
	source_where_clause,
	target_column
) VALUES
	('{"{HA,01}"}', 'Klippeflade', NULL, '{2}', 0.00, TRUE, 'ST_Area(geometri) + ST_Perimeter(geometri) * hoejde * klip_sider_kode / 2', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	('{"{HA,01,01}"}', 'Klippeflade', NULL, '{2}', 0.00, TRUE, 'ST_Area(geometri) + ST_Perimeter(geometri) * hoejde * klip_sider_kode / 2', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	('{"{HA,02}"}', 'Klippeflade', NULL, '{2}', 0.00, TRUE, 'ST_Area(geometri) + ST_Perimeter(geometri) * hoejde * klip_sider_kode / 2', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	('{"{BL,05}"}', 'Klippeflade', '2', '{1}', 0.00, TRUE, 'ST_Length(geometri) * hoejde', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	('{}', 'Renhold', NULL, '{2}', 0.00, TRUE, NULL, 'basis', 'driftniv', 'driftniv_kode', 'driftniv', 'enhedspris_f', NULL, 'driftniv_kode');