SELECT revision, connectorGroup 
FROM connectorGroups 
WHERE locationId = ? 
AND plugType = ? 
AND speed = ?
ORDER BY createdAt DESC
LIMIT 1;
