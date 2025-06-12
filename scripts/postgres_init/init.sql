CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE staging.students (
    student_id                BIGINT       PRIMARY KEY,
    age                       INTEGER      NOT NULL,
    gender                    TEXT         NOT NULL,
    academic_level            TEXT         NOT NULL,
    country                   TEXT         NOT NULL,
    avg_daily_usage_hours     REAL         NOT NULL,
    most_used_platform        TEXT         NOT NULL,
    affects_academic_performance BOOLEAN    NOT NULL,
    sleep_hours_per_night     REAL         NOT NULL,
    mental_health_score       INTEGER      NOT NULL,
    relationship_status       TEXT         NOT NULL,
    conflicts_over_social_media INTEGER    NOT NULL,
    addicted_score            INTEGER      NOT NULL
);
