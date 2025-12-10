-- TODO: I need to come up with a smart way of making sure prices can be connected to evseIds. 
-- I do not trust the connectorGroups because it is based on the ordering of the list when the row was inserted.
--
CREATE TABLE IF NOT EXISTS pricesLog (
    locationId TEXT NOT NULL, 
    revision INTEGER NOT NULL, -- Has to be inserted from connectorGroups  
    connectorGroup INT NOT NULL, -- has to be inserted from connectorGroups
    createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
    -- Variables of interest: 
    plugType TEXT,
    speed, TEXT,
    product TEXT,
    isFlat BOOLEAN,

    -- timetable within plugs
    from_datetime DATETIME,
    to_datetime DATETIME, 
    price TEXT,
    is_next_day BOOLEAN,
    timeTableRawData TEXT,  -- For backup purposes
    evseIdsRawData TEXT, -- For backup purposes
    mixedPlugTypes Boolean, -- These should be True, if not then evseIds are not uniquely of the same type
    mixedSpeeds Boolean, -- These should be True, if not then evseIds are not uniquely of the same speed
    FOREIGN KEY (locationId, revision, connectorGroup) 
        REFERENCES connectorGroups(locationId, revision, connectorGroup)
);