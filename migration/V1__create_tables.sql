CREATE DATABASE IF NOT EXISTS ${database};

CREATE TABLE IF NOT EXISTS ${database}.metrics (
    timestamp DateTime,
    metric_name LowCardinality(String),
    metric_unit LowCardinality(String),
    metric_type LowCardinality(String),
    qty Float64 DEFAULT 0,
    max Float64 DEFAULT 0,
    min Float64 DEFAULT 0,
    avg Float64 DEFAULT 0,
    asleep Float64 DEFAULT 0,
    in_bed Float64 DEFAULT 0,
    sleep_source LowCardinality(String) DEFAULT '',
    in_bed_source LowCardinality(String) DEFAULT '',
    PRIMARY KEY (timestamp, metric_name)
) ENGINE = ReplacingMergeTree();

CREATE TABLE IF NOT EXISTS ${database}.workouts (
    id UUID,
    name LowCardinality(String),
    start DateTime,
    end DateTime,
    active_energy_qty Float64 DEFAULT 0,
    active_energy_units LowCardinality(String) DEFAULT '',
    distance_qty Float64 DEFAULT 0,
    distance_units LowCardinality(String) DEFAULT '',
    intensity_qty Float64 DEFAULT 0,
    intensity_units LowCardinality(String) DEFAULT '',
    humidity_qty Float64 DEFAULT 0,
    humidity_units LowCardinality(String) DEFAULT '',
    temperature_qty Float64 DEFAULT 0,
    temperature_units LowCardinality(String) DEFAULT '',
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree();

CREATE TABLE IF NOT EXISTS ${database}.workout_routes (
    workout_id UUID,
    timestamp DateTime,
    lat Float64,
    lon Float64,
    altitude Float64,
    course Float64 DEFAULT 0,
    vertical_accuracy Float64 DEFAULT 0,
    horizontal_accuracy Float64 DEFAULT 0,
    course_accuracy Float64 DEFAULT 0,
    speed Float64 DEFAULT 0,
    speed_accuracy Float64 DEFAULT 0,
    PRIMARY KEY (workout_id, timestamp)
) ENGINE = ReplacingMergeTree();

CREATE TABLE IF NOT EXISTS ${database}.workout_heart_rate_data (
    workout_id UUID,
    timestamp DateTime,
    qty Float64,
    min Float64,
    max Float64,
    avg Float64,
    units LowCardinality(String),
    source LowCardinality(String),
    PRIMARY KEY (workout_id, timestamp)
) ENGINE = ReplacingMergeTree();

CREATE TABLE IF NOT EXISTS ${database}.workout_heart_rate_recovery (
    workout_id UUID,
    timestamp DateTime,
    qty Float64,
    min Float64,
    max Float64,
    avg Float64,
    units LowCardinality(String),
    source LowCardinality(String),
    PRIMARY KEY (workout_id, timestamp)
) ENGINE = ReplacingMergeTree();

CREATE TABLE IF NOT EXISTS ${database}.workout_step_count_log (
    workout_id UUID,
    timestamp DateTime,
    qty Float64,
    units LowCardinality(String),
    source LowCardinality(String),
    PRIMARY KEY (workout_id, timestamp)
) ENGINE = ReplacingMergeTree();

CREATE TABLE IF NOT EXISTS ${database}.workout_walking_running_distance (
    workout_id UUID,
    timestamp DateTime,
    qty Float64,
    units LowCardinality(String),
    source LowCardinality(String),
    PRIMARY KEY (workout_id, timestamp)
) ENGINE = ReplacingMergeTree();

CREATE TABLE IF NOT EXISTS ${database}.workout_active_energy (
    workout_id UUID,
    timestamp DateTime,
    qty Float64,
    units LowCardinality(String),
    source LowCardinality(String),
    PRIMARY KEY (workout_id, timestamp)
) ENGINE = ReplacingMergeTree();

CREATE TABLE IF NOT EXISTS ${database}.ecg (
    id UUID,
    classification LowCardinality(String),
    source LowCardinality(String),
    average_heart_rate Float64,
    start DateTime,
    end DateTime,
    number_of_voltage_measurements UInt32,
    sampling_frequency UInt32,
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree();

CREATE TABLE IF NOT EXISTS ${database}.ecg_voltage (
    ecg_id UUID,
    sample_index UInt32,
    timestamp DateTime64(9),
    voltage Float64,
    units LowCardinality(String),
    PRIMARY KEY (ecg_id, sample_index)
) ENGINE = ReplacingMergeTree();

CREATE TABLE IF NOT EXISTS ${database}.state_of_mind (
    id UUID,
    start DateTime,
    end DateTime,
    valence Float64,
    valence_classification LowCardinality(String),
    kind LowCardinality(String),
    labels Array(String),
    associations Array(String),
    PRIMARY KEY (id)
) ENGINE = ReplacingMergeTree();
