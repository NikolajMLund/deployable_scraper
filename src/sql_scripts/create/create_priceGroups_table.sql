CREATE TABLE IF NOT EXISTS priceGroups (
    priceGroupId INTEGER PRIMARY KEY AUTOINCREMENT,
    locationId TEXT NOT NULL,
    revision INTEGER,
    evseIdsHash TEXT NOT NULL,  -- Hash of sorted evseIds
    connectorGroup INTEGER,
    plugType TEXT,
    speed, TEXT,
    mixedPlugTypes Boolean, -- These is expected to be False (but depends on the business logic used), if not then evseIds are not uniquely of the same type
    mixedSpeeds Boolean, -- These is expected to be False (but depends on the business logic used), if not then evseIds are not uniquely of the same speed
    createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
    evseIdsRawData TEXT,
        
    -- Include hash in uniqueness constraint
    UNIQUE(locationId, evseIdsHash)
);

--CREATE INDEX IF NOT EXISTS idx_pricegroups_location 
--ON priceGroups(locationId, revision, ConnectorGroup);

CREATE INDEX IF NOT EXISTS idx_pricegroups_hash ON priceGroups(evseIdsHash);