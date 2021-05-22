CREATE TABLE ingestion_metadata (
    ingestion_date DATE NOT NULL,
    historic BOOLEAN NOT NULL,
    num_obs INT NOT NULL,
    data_size FLOAT NOT NULL
);

CREATE TABLE upload_metadata (
    ingestion_date DATE NOT NULL,
    historic BOOLEAN NOT NULL,
    file_name VARCHAR(250) NOT NULL
);

CREATE TABLE clean_metadata (
    original_rows INT NOT NULL,
    original_cols INT NOT NULL,
    final_rows INT NOT NULL,
    final_cols INT NOT NULL,
    historic BOOLEAN NOT NULL,
    ingestion_date DATE NOT NULL
);

CREATE TABLE fe_metadata (
    original_rows INT NOT NULL,
    original_cols INT NOT NULL,
    final_rows INT NOT NULL,
    final_cols INT NOT NULL,
    historic BOOLEAN NOT NULL,
    ingestion_date DATE NOT NULL,
    training BOOLEAN NOT NULL
);

CREATE TABLE training_metadata (
    historic BOOLEAN NOT NULL,
    query_date DATE NOT NULL,
    no_of_models INT NOT NULL,
    algorithms VARCHAR(100) NOT NULL,
    training_time FLOAT NOT NULL,
    split_criteria VARCHAR(100) NOT NULL
);

CREATE TABLE selection_metadata (
    historic BOOLEAN NOT NULL,
    query_date DATE NOT NULL,
    best_model VARCHAR(250) NOT NULL,
    possible_models INT NOT NULL,
    cutting_threshold FLOAT NOT NULL,
    fpr_restriction FLOAT NOT NULL,
    best_auc FLOAT NOT NULL
);

CREATE TABLE aequitas_metadata (
    group_entities INT NOT NULL,
    mean_positive_rate FLOAT NOT NULL,
    mean_ppr_disparity FLOAT NOT NULL,
    unsupervised_fairness BOOLEAN NOT NULL,
    supervised_fairness BOOLEAN NOT NULL,
    overall_fairness BOOLEAN NOT NULL
);

CREATE TABLE prediction_metadata (
    query_date DATE NOT NULL,
    prediction_date DATE NOT NULL,
    model_date DATE NOT NULL,
    model_s3_path VARCHAR(250) NOT NULL,
    threshold FLOAT NOT NULL,
    no_predictions INT NOT NULL,
    no_positive_labels INT NOT NULL,
    no_negative_labels INT NOT NULL
);

CREATE TABLE unittests(
    test_date DATE NOT NULL,
    test_name VARCHAR(250) NOT NULL
);
