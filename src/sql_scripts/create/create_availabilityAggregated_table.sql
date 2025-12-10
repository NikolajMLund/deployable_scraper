-- Python should handle that the foreign Key used actually exists in ConnectorCounts.

CREATE TABLE IF NOT EXISTS availabilityAggregated (
    locationId TEXT, 
    revision INTEGER,
    connectorGroup INTEGER, 
    createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
    availableCount INTEGER,
    totalCount INTEGER,
    FOREIGN KEY (locationId, revision, connectorGroup) 
        REFERENCES connectorGroups(locationId, revision, connectorGroup)
);


