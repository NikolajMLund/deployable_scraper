import os
import sqlite3
import logging
from importlib import resources
from datetime import datetime
import json
import hashlib

# Create module-level logger
logger = logging.getLogger(__name__)

def compute_evseids_hash(evseIds_list):
    """Create a consistent hash from evseIds list"""
    # Sort to ensure same list gives same hash
    sorted_ids = sorted(evseIds_list)
    # Convert to JSON string for consistent representation
    ids_string = json.dumps(sorted_ids, sort_keys=True)
    # Create hash
    return hashlib.sha256(ids_string.encode()).hexdigest()[:16]  # Use first 16 chars



class db:
    def __init__(self, name:str):
        self.name = name

    def check_if_db_exists(self):  
        _exists=os.path.exists(f'{self.name}.db')
        return _exists
    
    def enable_wal_mode(self):
        """Enable WAL mode on existing database"""
        if not self.check_if_db_exists():
            logger.warning(f"Database {self.name}.db does not exist yet")
            return False
        
        try:
            conn = sqlite3.connect(f'{self.name}.db', timeout=30)
            cursor = conn.cursor()
            
            # Check current mode
            cursor.execute('PRAGMA journal_mode')
            current_mode = cursor.fetchone()[0]
            logger.info(f"Current journal mode: {current_mode}")
            
            # Enable WAL mode
            cursor.execute('PRAGMA journal_mode=WAL')
            new_mode = cursor.fetchone()[0]
            
            # Set busy timeout
            cursor.execute('PRAGMA busy_timeout=120000') # wait 2 minutes. 
                        
            logger.info(f"Journal mode changed: {current_mode} → {new_mode}")
            return new_mode.lower() == 'wal'
        
        except Exception as e:
            logger.error(f"Failed to enable WAL mode: {e}")
            return False
        finally: 
            if conn: 
                conn.close()


    def create_db(self): 
        if self.check_if_db_exists():
            logger.debug(f"Database {self.name} already exist - adding tables if they currently do not exist:")
        else: 
            logger.debug(f"Database {self.name} Initialized - adding tables:")
        
        # connect to db 
        conn = sqlite3.connect(f'{self.name}.db', timeout = 30)
        cursor = conn.cursor()
        
        # scripts to execute in specific order
        script_order = [
            'create_locations_table.sql',
            'create_evseIds_table.sql',
            'create_connectorGroups_table.sql',
            'create_availabilityLog_table.sql',
            'create_availabilityAggregated_table.sql',
            'create_latest_connectorGroups_newest_revision_view.sql',
            'create_priceGroups_table.sql',
            'create_priceTimeSlots_table.sql',
        ]

        # Get list of tables before edits
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables_before = set(row[0] for row in cursor.fetchall())
        logger.debug(f"Tables before edits: {sorted(tables_before) if tables_before else 'None'}")

        for script_name in script_order: 
            logger.debug(f"Executing script: {script_name}")
            sql_script = resources.read_text('sql_scripts.create', script_name)
            cursor.executescript(sql_script)
        
        # Get list of tables after edits
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables_after = set(row[0] for row in cursor.fetchall())
        logger.debug(f"Tables after edits: {sorted(tables_after)}")

        # Log what tables were added
        tables_added = tables_after - tables_before
        if tables_added:
            logger.info(f"New tables added: {sorted(tables_added)}")
        else:
            logger.debug("No new tables were added")
        
        conn.commit()
        conn.close()

        logger.info(f"Database initialized successfully at {self.name}.db")

        # switching to wal mode: 
        self.enable_wal_mode()


    def insert_row(self, conn, table_name, row_dict):
        """Insert a row into specified table"""
        
        cursor = conn.cursor()

        # ensures that foreign_keys are always enabled.
        cursor.execute("PRAGMA foreign_keys = ON;")
               
        # Build the INSERT statement dynamically
        columns = ', '.join(row_dict.keys())
        placeholders = ', '.join(['?' for _ in row_dict])
        values = list(row_dict.values())
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:                
            cursor.execute("SAVEPOINT sp")
            cursor.execute(sql, values)
            cursor.execute("RELEASE sp")
            logger.debug(f"✅ Successfully inserted row into {table_name}")
            return True, None
        except sqlite3.Error as e:
            cursor.execute("ROLLBACK TO sp")
            cursor.execute("RELEASE sp")
            logger.debug(f"❌ Error inserting into {table_name}: {e}", exc_info=True)
            return False, e

    def insert_row_in_locations_table(self, conn, location):
        # adding these to ensure that if there is ever a case v_coords coordinates or timestamp does not exist then
        # we can still call with get and get nan values
        location_coords = location.get('coordinates', {})
        location_timestamp = location.get('timestamp', {})
        
        data_row = {
            'locationId': location.get('locationId') ,
            'revision': location.get('revision'), 
            'name': location.get('name'),
            'partnerStatus': location.get('partnerStatus'),
            'isRoamingPartner': location.get('isRoamingPartner'),
            'origin': location.get('origin'),
            'coords_lat': location_coords.get('lat'),
            'coords_lng': location_coords.get('lng'),
            'ts_seconds': location_timestamp.get('seconds'),
            'ts_nanoseconds': location_timestamp.get('nanoseconds'),
        }
        
        self.insert_row(conn, 'locations', row_dict=data_row)

    def insert_row_in_connectorGroup_table(self, conn, location:dict, connectorGroup:int, connectorCount:dict):

        data_row = {
            'locationId': location.get('locationId') ,
            'revision': location.get('revision'), 
            'connectorGroup': connectorGroup,
            'plugType': connectorCount.get('plugType'), 
            'speed': connectorCount.get('speed'),
            'count': connectorCount.get('count'),
        }
        
        success, error=self.insert_row(conn, 'connectorGroups', row_dict=data_row)

        return success, error


    def insert_row_in_evseIds_table(self, conn, location, evse:dict): 
        """
        The evse object is a dict and is a value returned from the 'evses' object. 
        """
        connectors = evse.get('connectors', {})
        for evseConnectorId in connectors.keys():
            # I expect there is only one ky in connectors but if there is more I should get an unique error.
            plug_info=connectors[evseConnectorId]

            data_row = {
            'locationId': location.get('locationId', 'returnsError'),
            'revision': location.get('revision', 'returnsError'),
            'evseId': evse.get('evseId', 'returnsError'),
            'isRoamingPartner': location.get('isRoamingPartner'),
            'isRoamingAllowed': location.get('publicAccess',{}).get('isRoamingAllowed'),
            'visibility': location.get('visibility'),
            'vendorName': evse.get('vendorName'),
            'evseConnectorId': plug_info.get('evseConnectorId'), 
            'plugType': plug_info.get('plugType'), 
            'powerType': plug_info.get('powerType'),
            'maxPowerKw': plug_info.get('maxPowerKw'),
            'connectorId': plug_info.get('connectorId'),
            'speed': plug_info.get('speed'),
            }
            success, error=self.insert_row(conn, table_name='evseIds', row_dict=data_row)

    def insert_row_in_availabilityLog_table(self, conn, loc_avail_query):
        evses = loc_avail_query.get('availability', {}).get('evses', {})
        evses_pluginfo = loc_avail_query.get('evses')
        nplugs = len(evses_pluginfo.keys())
        # now we want to loop over all the evses
        
        # XXX: This skips any stations where there is no availability data.
        if len(evses.keys()) == 0: 
            logger.warning(
                "No availability data for locationId=%s",
                loc_avail_query.get('locationId'),
            )
        
        # keep count of successes: 
        nsuccess = 0
        for evse_key in evses.keys():
            try:
                evse = evses.get(evse_key)
                evse_pluginfo=evses_pluginfo.get(evse_key, {})
                
                # Construct a data row
                data_row = {
                'locationId': loc_avail_query.get('locationId', 'returnsError'),
                'revision': loc_avail_query.get('revision', 'returnsError'),
                'evseId': evse.get('evseId', 'returnsError'),
                'status': evse.get('status', 'returnsNoError'),
                'timestamp': evse.get('timestamp')
                }
                success = False
                success, error=self.insert_row(conn, 'availabilityLog', row_dict=data_row)

                if (not success) and (error.sqlite_errorcode == 787):
                    self.insert_row_in_evseIds_table(
                        conn=conn,
                        location=loc_avail_query, 
                        evse=evse_pluginfo
                    )
                    success, error=self.insert_row(conn, 'availabilityLog', row_dict=data_row)
                
                # add to nsuccess
                nsuccess += int(success)

            except AttributeError as e:
                logger.warning(
                    'AttributeError for locationId=%s, evseId=%s',
                    loc_avail_query.get('locationId'),
                    evse_key,
                    exc_info=True)
                continue
        return nsuccess, nplugs

    def select_all_locationIds(self,):
        """Get all locationIds (latest revision only)"""
        
        logger.debug(f"Selecting all locationIds (latest revision only)")

        sql_script=resources.read_text('sql_scripts.select', 'select_all_locationIds.sql')

        conn = sqlite3.connect(f'{self.name}.db', timeout = 30)
        cursor = conn.cursor()
        try:
            cursor.execute(sql_script,)
            results = cursor.fetchall()
        except:
            pass
        
        finally: 
            if conn:
                conn.close()

        return [row[0] for row in results]  # Extract locationIds

    def select_locationIds_by_speed(self, speed:str):
        """Get locationIds with specific speed (latest revision only)"""
        
        logger.debug(f"Selecting locationIds for speed: {speed}")

        sql_script=resources.read_text('sql_scripts.select', 'select_locationIds_by_plugType.sql')

        conn = sqlite3.connect(f'{self.name}.db', timeout = 30)
        cursor = conn.cursor()
        try:
            cursor.execute(sql_script, (speed,))
            results = cursor.fetchall()
        except:
            pass
        
        finally: 
            conn.close()
        
        return [row[0] for row in results]  # Extract locationIds

    def query_for_matching_connectorGroups(self, conn, locationId, plugType, speed):
        revision, connectorGroup = 0, 0
        try:
            cursor = conn.cursor()

            # Load SQL script from file
            sql_script = resources.read_text('sql_scripts.select', 'select_connectorGroup_by_plugType_speed.sql')
                
            cursor.execute(sql_script, (locationId, plugType, speed))
            result = cursor.fetchone()
                
            if result is not None:
                revision, connectorGroup = result
            else:
                logger.warning(f"No matching connectorGroup found for locationId={locationId}, "
                                f"plugType={plugType}, speed={speed}")
                        
            return revision, connectorGroup   
           
        except Exception as e:
            logger.error(f"Database query failed for locationId={locationId}: {e}")

            return revision, connectorGroup 

    def query_priceGroups_for_priceGroupId(self, conn, locationId, evseidsHash):
        cursor = conn.cursor()
        # Load SQL script from file
        sql_script = resources.read_text('sql_scripts.select', 'select_priceGroupId_by_locationId_evseidsHash.sql')
        
        cursor.execute(sql_script, (locationId, evseidsHash))
        result = cursor.fetchone()
        
        if result is not None:
            priceGroupId = result[0]
            return priceGroupId

    def insert_row_in_priceGroups_table(
            self, 
            conn,
            locationId, 
            plugType, 
            speed,
            mixedSpeeds,
            mixedPlugTypes,
            evseIdsHash,
            evseIds,
        ):
        # searching for revision and connectorGroup - Do this after evseidshash search
        revision, connectorGroup = self.query_for_matching_connectorGroups(
            conn,
            locationId=locationId, 
            plugType=plugType,
            speed=speed,
        )

        data_row = {
        'locationId': locationId,
        'revision': revision,
        'connectorGroup': connectorGroup,
        'plugType': plugType,
        'speed': speed,
        'evseIdsRawData': json.dumps(sorted(evseIds)),
        'evseIdsHash': evseIdsHash,
        'mixedPlugTypes': mixedPlugTypes,
        'mixedSpeeds': mixedSpeeds,

        }

        success, error = self.insert_row(conn, 'priceGroups', row_dict=data_row)
        return success, error
    
    def insert_rows_in_priceTimeSlots_table(self, conn, plug_data):
        """
        Insert price data for a specific plug type at a location.
        
        Args:
            plug_data: dict containing plugType, speed, and prices information
        
        """
        locationId = plug_data.get('locationId')
        plugs = plug_data.get('plugs',[])
        if not plugs: 
            logger.warning(f"No plugs found for locationId={locationId}")

        nsuccess_across_plugs, ntotal_across_plugs = 0, 0
        for plugGroup in plugs:
            try:
                connectors = plugGroup.get('connectors', [])
                if not connectors:
                    logger.warning(f"No connectors found for locationId={locationId}")

                # 
                evseIds = sorted(list(set([connector['evseId'] for connector in connectors])))
                plugTypes = list(set([connector['plugType'] for connector in connectors]))
                speeds = list(set([connector['speed'] for connector in connectors]))

                # Compute hash of evseIds
                evseIds_hash = compute_evseids_hash(evseIds)

                mixedPlugTypes, mixedSpeeds = False, False
                if len(plugTypes) > 1: 
                    logger.warning(f'for locationId={locationId},evseIds_hash={evseIds_hash} plugtypes are not homogenous, that is plugtypes {plugTypes} has more than one unique value.')
                    mixedPlugTypes = True
                if len(speeds) > 1: 
                    logger.warning(f'for locationId={locationId},evseIds_hash={evseIds_hash} speeds are not homogenous, that is speeds {speeds} has more than one unique value.')
                    mixedSpeeds = True

                # check if locationId, hash combo exists in priceGroups
                # I Think I will instead just do this upon failure
                # But I have to get priceGroupId anyways...  
                priceGroupId=self.query_priceGroups_for_priceGroupId(
                    conn=conn,
                    locationId=locationId,
                    evseidsHash=evseIds_hash
                )

                if priceGroupId is None:
                    logger.debug(f"Failed to find an existing priceGroup for locationId={locationId}, plugType={plugTypes[0]}, speed={speeds[0]}, evseIdsHash={evseIds_hash}")
                    logger.debug(f"Attempting Insertion")

                    # insert priceGroup Row data
                    self.insert_row_in_priceGroups_table(
                        conn=conn,
                        locationId=locationId,
                        plugType=plugTypes[0],
                        speed=speeds[0],
                        mixedSpeeds=mixedSpeeds,
                        mixedPlugTypes=mixedPlugTypes,
                        evseIds=sorted(evseIds),
                        evseIdsHash=evseIds_hash,
                    )
                    # and query again
                    priceGroupId=self.query_priceGroups_for_priceGroupId(
                        conn=conn,
                        locationId=locationId,
                        evseidsHash=evseIds_hash
                    )
                    
                    if priceGroupId is None: 
                        logger.warning(f'Failed to insert priceGroupId into priceGroup table for locationId={locationId}, plugtypes={plugTypes[0]}, speed={speeds[0]}')
                    else:
                        logger.info(f'Insertion succesful - Created new priceGroupId={priceGroupId} for evseIdsHash={evseIds_hash}')
                else:
                    logger.debug(f'Found existing priceGroupId={evseIds_hash}')

                prices = plugGroup.get('prices', [])
                nsuccess = 0
                ntotal = 0
                for price_entry in prices:
                    product = price_entry.get('product')
                    isFlat = price_entry.get('isFlat')
                    timeTable = price_entry.get('timeTable', [])
                    
                    for i, time_slot in enumerate(timeTable):
                        ntotal += 1
                        
                        # Parse datetime strings (format: "DD.MM.YYYY" and "HH:MM")
                        from_date = time_slot.get('from_date_string')
                        from_time = time_slot.get('from_time_string')
                        to_date = time_slot.get('to_date_string')
                        to_time = time_slot.get('to_time_string')
                        
                        # Convert to proper datetime format for SQLite
                        from_datetime = None
                        to_datetime = None
                        
                        backup_data = False
                        if from_date and from_time:
                            try:
                                # Parse "DD.MM.YYYY HH:MM" format
                                dt_str = f"{from_date} {from_time}"
                                dt_obj = datetime.strptime(dt_str, "%d.%m.%Y %H:%M")
                                from_datetime = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                            except ValueError as e:
                                logger.warning(f"Failed to parse from_datetime '{dt_str}': {e}")
                                backup_data = True
                        
                        if to_date and to_time:
                            try:
                                # Parse "DD.MM.YYYY HH:MM" format
                                dt_str = f"{to_date} {to_time}"
                                dt_obj = datetime.strptime(dt_str, "%d.%m.%Y %H:%M")
                                to_datetime = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                            except ValueError as e:
                                logger.warning(f"Failed to parse to_datetime '{dt_str}': {e}")
                                backup_data=True
                    
                        # validate prices are not none
                        price = time_slot.get('price_string')
                        if price is None: 
                            backup_data=True
                        
                        # backing up data if something crazy happens but otherwise no backup is done.
                        timeTableRawData = None
                        if backup_data:
                            timeTableRawData = json.dumps(time_slot)
                        
                        data_row = {
                            'locationId': locationId,
                            'priceGroupId': priceGroupId,
                            'product': product,
                            'isFlat': isFlat,
                            'from_datetime': from_datetime,
                            'to_datetime': to_datetime,
                            'isCurrent': i == 0, # if i is zero it is the first entry in the table which indicate current price
                            'price': price,
                            'is_next_day': time_slot.get('is_next_day'),
                            'timeTableRawData': timeTableRawData,
                        }
                        
                        success, error = self.insert_row(
                            conn=conn,
                            table_name='priceTimeSlots', 
                            row_dict=data_row
                        )
                        
                        if success:
                            nsuccess += 1
                        else:
                            logger.warning(f"Failed to insert price data for locationId={locationId}, "
                                        f"plugType={plugTypes[0]}, product={product}: {error}")
                
                logger.debug(f"Inserted {nsuccess}/{ntotal} price entries for locationId={locationId}, "
                            f"plugType={plugTypes[0]}, speed={speeds[0]}")
                
                nsuccess_across_plugs += nsuccess
                ntotal_across_plugs += ntotal
            
            except AttributeError as e:
                logger.warning(
                    f'AttributeError for locationId={locationId}, Error: {str(e)}')
                ntotal_across_plugs += ntotal
                continue

        return nsuccess_across_plugs, ntotal_across_plugs
