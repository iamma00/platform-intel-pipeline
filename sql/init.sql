CREATE TABLE IF NOT EXISTS addresses (
    id SERIAL PRIMARY KEY,
    name TEXT,
    address TEXT,
    pincode TEXT,
    city TEXT,
    address_type TEXT,         -- predicted: Corporate / Residential / Unknown
    matched_keywords TEXT,     -- which keywords triggered the decision (useful for debugging)
    confidence TEXT,           -- High / Low based on keyword strength
    created_at TIMESTAMP DEFAULT NOW()
);