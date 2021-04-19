CREATE TABLE test_ingestion_metadata (
	ingestion_date DATE NOT NULL,
	historic BOOLEAN NOT NULL,
	num_obs INT NOT NULL,
	data_size FLOAT NOT NULL
);

CREATE TABLE test_upload_metadata (
	ingestion_date DATE NOT NULL,
	historic BOOLEAN NOT NULL,
	file_name VARCHAR(250) NOT NULL
);

CREATE TABLE test_clean_metadata (
    original_rows INT NOT NULL,
    original_cols INT NOT NULL,
    final_rows INT NOT NULL,
    final_cols INT NOT NULL,
	historic BOOLEAN NOT NULL,
	ingestion_date DATE NOT NULL
);

CREATE TABLE test_fe_metadata (
    original_rows INT NOT NULL,
    original_cols INT NOT NULL,
    final_rows INT NOT NULL,
    final_cols INT NOT NULL,
	historic BOOLEAN NOT NULL,
	ingestion_date DATE NOT NULL
);
