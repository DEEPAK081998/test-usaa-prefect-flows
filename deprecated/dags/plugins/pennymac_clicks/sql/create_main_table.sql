CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    id SERIAL PRIMARY KEY,
    INDEX INT8,
    _filename text,
    _loaded_by_pennymac timestamp,
    _loaded_to_sendgrid timestamp,
    SUBSCRIBERKEY text,
    EMAILADDRESS text,
    MDMID text,
    CLICKDATE text,
    LINKNAME text,
    FIRSTNAME text,
    LASTNAME text,
    PHONENUMBER text
);