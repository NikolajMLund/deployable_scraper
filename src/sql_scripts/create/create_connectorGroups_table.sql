CREATE TABLE IF NOT EXISTS connectorGroups (
    locationId TEXT, 
    revision INTEGER,
    connectorGroup INTEGER,  
    createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
    plugType TEXT, 
    speed TEXT, 
    count INT,
    PRIMARY KEY (locationId, revision, connectorGroup),
    UNIQUE(locationId, revision, speed, plugType),
    FOREIGN KEY (locationId, revision) 
        REFERENCES locations(locationId, revision)
);

CREATE INDEX IF NOT EXISTS idx_location_revision_speed_plugtype ON connectorGroups(locationId, revision, speed, plugType);
CREATE INDEX IF NOT EXISTS idx_location_speed_plugtype ON connectorGroups(locationId, speed, plugType);