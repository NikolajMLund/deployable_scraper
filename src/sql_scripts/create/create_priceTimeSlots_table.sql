CREATE TABLE IF NOT EXISTS priceTimeSlots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    locationId TEXT,
    priceGroupId INTEGER NOT NULL,
    product TEXT,
    isFlat BOOLEAN,
    from_datetime DATETIME, -- price is valid from 
    to_datetime DATETIME, -- price is valid to
    isCurrent BOOLEAN, -- Indicates whether the price is the current price or a forecasted/expected price
    price TEXT,
    is_next_day BOOLEAN,
    createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
    timeTableRawData TEXT,
    
    FOREIGN KEY (priceGroupId) REFERENCES priceGroups(priceGroupId) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_price_timeslots_group ON priceTimeSlots(priceGroupId);
CREATE INDEX IF NOT EXISTS idx_price_timeslots_datetime ON priceTimeSlots(from_datetime, to_datetime);



-- CREATE TABLE IF NOT EXISTS pricesLog (
--     locationId TEXT NOT NULL, 
--     revision INTEGER NOT NULL, -- Has to be inserted from connectorGroups  
--     connectorGroup INT NOT NULL, -- has to be inserted from connectorGroups
--     createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
--     -- Variables of interest: 
--     plugType TEXT,
--     speed, TEXT,
--     product TEXT,
--     isFlat BOOLEAN,

--     -- timetable within plugs
--     from_datetime DATETIME,
--     to_datetime DATETIME, 
--     price TEXT,
--     is_next_day BOOLEAN,
--     timeTableRawData TEXT,  -- For backup purposes
--     evseIdsRawData TEXT, -- For backup purposes
--     mixedPlugTypes Boolean, -- These should be True, if not then evseIds are not uniquely of the same type
--     mixedSpeeds Boolean, -- These should be True, if not then evseIds are not uniquely of the same speed
--     FOREIGN KEY (locationId, revision, connectorGroup) 
--         REFERENCES connectorGroups(locationId, revision, connectorGroup)
-- );