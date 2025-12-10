CREATE TABLE IF NOT EXISTS evseIds (
    -- Primary keys
    locationId TEXT, 
    revision INTEGER, 
    evseId TEXT,  -- info within 'evses'
    createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
    -- 
    isRoamingPartner BOOLEAN, 
    isRoamingAllowed BOOLEAN, 
    visibility TEXT, 

    -- info within 'evses'
    chargePointId TEXT, 
    vendorName TEXT,
    -- info within 'evses' within 'connectors'
    evseConnectorId TEXT,
    plugType Text,
    powerType Text,
    maxPowerKw FLOAT, 
    connectorId INT,
    speed TEXT, 
    -- 

    PRIMARY KEY (locationId, revision, evseId)
);

CREATE INDEX IF NOT EXISTS idx_locationId_revision_evseId ON evseIds(locationId, revision, evseId)