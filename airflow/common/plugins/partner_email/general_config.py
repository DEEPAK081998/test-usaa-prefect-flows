"""Store general configuration for all partners and reports."""

# QA
QA_SENDING_RESPONSE_YAML = 'sent_emails/{today}_{report_type}_qa_sending_response.yaml'

# Email
PAYLOAD_JSON = 'debug/payload.json'
BATCHES_JSON = 'debug/batches.json'
TEST_RECIPIENTS = [
    'gloureiro@homestoryrewards.com',
    'tijanag@homestoryrewards.com',
    'vgarate@homestoryrewards.com'
]
JTG_DEV_EMAIL = [
    'dbansal@homestoryrewards.com',
    'vrathore@homestoryrewards.com',
    'everma@homestoryrewards.com'
]
DEV_EMAIL = 'gloureiro@homestoryrewards.com'
KRIS_EMAIL = 'kristina@homestoryrewards.com'
VIC_EMAIL = 'vgarate@homestoryrewards.com'
MARKO_EMAIL = 'mpjanovic@homestoryrewards.com'
TIJANA_EMAIL = 'tijanag@homestoryrewards.com'
CARY_EMAIL = 'cary@homestoryrewards.com'
BI_EMAIL = 'bi@homestoryrewards.com'

# Batch
MAX_BATCH_LENGTH = 450  # emails
MAX_BATCH_SIZE = 45  # Mb
