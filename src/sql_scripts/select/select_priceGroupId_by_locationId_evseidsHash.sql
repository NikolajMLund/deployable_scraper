SELECT priceGroupId
FROM priceGroups
WHERE locationId = ? AND evseIdsHash = ?;