ALTER TABLE greg.flader		DISABLE TRIGGER ALL;
ALTER TABLE greg.linier		DISABLE TRIGGER ALL;
ALTER TABLE greg.punkter	DISABLE TRIGGER ALL;
ALTER TABLE greg.flader		ENABLE TRIGGER z_flader_history_iud;
ALTER TABLE greg.linier		ENABLE TRIGGER z_linier_history_iud;
ALTER TABLE greg.punkter	ENABLE TRIGGER z_punkter_history_iud;