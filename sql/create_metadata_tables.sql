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
    ingestion_date DATE NOT NULL,
    training BOOLEAN NOT NULL
);

CREATE TABLE test_training_metadata (
    historic BOOLEAN NOT NULL,
    query_date DATE NOT NULL,
    no_of_models INT NOT NULL,
    algorithms VARCHAR(100) NOT NULL,
    training_time FLOAT NOT NULL,
    split_criteria VARCHAR(100) NOT NULL
);

CREATE TABLE test_selection_metadata (
    historic BOOLEAN NOT NULL,
    query_date DATE NOT NULL,
    best_model VARCHAR(250) NOT NULL,
    possible_models INT NOT NULL,
    cutting_threshold FLOAT NOT NULL,
    fpr_restriction FLOAT NOT NULL,
    best_auc FLOAT NOT NULL
);

CREATE TABLE unittests(
    test_date DATE NOT NULL,
    test_name VARCHAR(250) NOT NULL
);
