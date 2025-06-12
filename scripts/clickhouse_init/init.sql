CREATE DATABASE IF NOT EXISTS staging;

CREATE TABLE staging.students (
    student_id                      UInt64,
    age                             Int32,
    gender                          String,
    academic_level                  String,
    country                         String,
    avg_daily_usage_hours           Float32,
    most_used_platform              String,
    affects_academic_performance    UInt8,
    sleep_hours_per_night           Float32,
    mental_health_score             Int32,
    relationship_status             String,
    conflicts_over_social_media     Int32,
    addicted_score                  Int32
)
ENGINE = MergeTree()
PARTITION BY country
ORDER BY (country, student_id)
SETTINGS index_granularity = 8192;
