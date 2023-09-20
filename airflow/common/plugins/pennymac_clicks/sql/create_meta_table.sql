CREATE TABLE IF NOT EXISTS {{ params.table_empty_keys }} (
    id SERIAL PRIMARY KEY,
    index INT8,
    empty_keys text
);

