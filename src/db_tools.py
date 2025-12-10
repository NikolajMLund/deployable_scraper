import os
import sqlite3
import logging
from importlib import resources
from datetime import datetime

# Create module-level logger
logger = logging.getLogger(__name__)

class db:
    def __init__(self, name:str):
        self.name = name

    def check_if_db_exists(self):  
        _exists=os.path.exists(f'{self.name}.db')
        return _exists
    
    def create_db(self): 
        if self.check_if_db_exists():
            logger.debug(f"Database {self.name} already exist - adding tables if they currently do not exist:")
        else: 
            logger.debug(f"Database {self.name} Initialized - adding tables:")
        
        # connect to db 
        conn = sqlite3.connect(f'{self.name}.db')
        cursor = conn.cursor()
        
        # scripts to execute in specific order
        script_order = [
            'create_locations_table.sql',
            'create_evseIds_table.sql',
            'create_connectorGroups_table.sql',
            'create_availabilityLog_table.sql',
            'create_availabilityAggregated_table.sql',
            'create_latest_connectorGroups_newest_revision_view.sql',
            'create_pricesLog_table.sql',
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

    def insert_row(self, table_name, row_dict):
        """Insert a row into specified table"""
        
        conn = sqlite3.connect(f'{self.name}.db')
        cursor = conn.cursor()

        # ensures that foreign_keys are always enabled.
        cursor.execute("PRAGMA foreign_keys = ON;")
               
        try:
            # Build the INSERT statement dynamically
            columns = ', '.join(row_dict.keys())
            placeholders = ', '.join(['?' for _ in row_dict])
            values = list(row_dict.values())
            
            
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, values)
            conn.commit()
            
            logger.debug(f"✅ Successfully inserted row into {table_name}")

            return True, None
            
        except sqlite3.Error as e:
            logger.debug(f"❌ Error inserting into {table_name}: {e}", exc_info=True)

            conn.rollback()
            return False, e
            
        finally:
            conn.close()

    def insert_row_in_locations_table(self, location):
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
        
        self.insert_row('locations', row_dict=data_row)

    def insert_row_in_connectorGroup_table(self, location:dict, connectorGroup:int, connectorCount:dict):

        data_row = {
            'locationId': location.get('locationId') ,
            'revision': location.get('revision'), 
            'connectorGroup': connectorGroup,
            'plugType': connectorCount.get('plugType'), 
            'speed': connectorCount.get('speed'),
            'count': connectorCount.get('count'),
        }
        
        success, error=self.insert_row('connectorGroups', row_dict=data_row)

        return success, error


    def insert_row_in_evseIds_table(self, location, evse:dict): 
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
            success, error=self.insert_row(table_name='evseIds', row_dict=data_row)

    def insert_row_in_availabilityLog_table(self, loc_avail_query):
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
                success, error=self.insert_row('availabilityLog', row_dict=data_row)

                if (not success) and (error.sqlite_errorcode == 787):
                    self.insert_row_in_evseIds_table(location=loc_avail_query, evse=evse_pluginfo)
                    success, error=self.insert_row('availabilityLog', row_dict=data_row)
                
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

    def select_locationIds_by_speed(self, speed:str):
        """Get locationIds with specific speed (latest revision only)"""
        
        logger.debug(f"Selecting locationIds for speed: {speed}")

        sql_script=resources.read_text('sql_scripts.select', 'select_locationIds_by_plugType.sql')

        conn = sqlite3.connect(f'{self.name}.db')
        cursor = conn.cursor()
        try:
            cursor.execute(sql_script, (speed,))
            results = cursor.fetchall()
        except:
            pass
        
        finally: 
            conn.close()
        
        return [row[0] for row in results]  # Extract locationIds

    def query_for_matching_connectorGroups(self, locationId, plugType, speed):
        revision, connectorGroup = 0, 0
        try:
            conn = sqlite3.connect(f'{self.name}.db')
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
        finally:
            conn.close()


    def insert_rows_in_pricesLog_table(self, plug_data):
        """
        Insert price data for a specific plug type at a location.
        
        Args:
            plug_data: dict containing plugType, speed, and prices information
        
        """
        locationId = plug_data.get('locationId')
        
        for plugGroup in plug_data['plugs']:

            connectors = plugGroup.get('connectors', [])
            if not connectors:
                logger.warning(f"No connectors found for locationId={locationId}")
            # 
            evseIds = list(set([connector['evseId'] for connector in connectors]))
            plugTypes = list(set([connector['plugType'] for connector in connectors]))
            speeds = list(set([connector['speed'] for connector in connectors]))

            mixedPlugTypes, mixedSpeeds = False, False
            if len(plugTypes) > 1: 
                logger.warning(f'for {locationId} plugtypes are not homogenous, that is plugtypes {plugTypes} has more than one unique value.')
                mixedPlugTypes = True
            if len(speeds) > 1: 
                logger.warning(f'for {locationId} speeds are not homogenous, that is speeds {speeds} has more than one unique value.')
                mixedSpeeds = True
            # searching for revision and connectorGroup
            revision, connectorGroup = self.query_for_matching_connectorGroups(
                locationId=locationId, 
                plugType=plugTypes[0],
                speed=speeds[0]
            )

            prices = plugGroup.get('prices', [])
            nsuccess = 0
            ntotal = 0
            for price_entry in prices:
                product = price_entry.get('product')
                isFlat = price_entry.get('isFlat')
                timeTable = price_entry.get('timeTable', [])
                
                for time_slot in timeTable:
                    ntotal += 1
                    
                    # Parse datetime strings (format: "DD.MM.YYYY" and "HH:MM")
                    from_date = time_slot.get('from_date_string')
                    from_time = time_slot.get('from_time_string')
                    to_date = time_slot.get('to_date_string')
                    to_time = time_slot.get('to_time_string')
                    
                    # Convert to proper datetime format for SQLite
                    from_datetime = None
                    to_datetime = None
                    
                    if from_date and from_time:
                        try:
                            # Parse "DD.MM.YYYY HH:MM" format
                            dt_str = f"{from_date} {from_time}"
                            dt_obj = datetime.strptime(dt_str, "%d.%m.%Y %H:%M")
                            from_datetime = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError as e:
                            logger.warning(f"Failed to parse from_datetime '{dt_str}': {e}")
                    
                    if to_date and to_time:
                        try:
                            # Parse "DD.MM.YYYY HH:MM" format
                            dt_str = f"{to_date} {to_time}"
                            dt_obj = datetime.strptime(dt_str, "%d.%m.%Y %H:%M")
                            to_datetime = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError as e:
                            logger.warning(f"Failed to parse to_datetime '{dt_str}': {e}")
                    
                    data_row = {
                        'locationId': locationId,
                        'revision': revision,
                        'ConnectorGroup': connectorGroup,
                        'plugType': plugTypes[0],
                        'speed': speeds[0],
                        'product': product,
                        'isFlat': isFlat,
                        'from_datetime': from_datetime,
                        'to_datetime': to_datetime,
                        'price': time_slot.get('price_string'),
                        'is_next_day': time_slot.get('is_next_day'),
                        'timeTableRawData': str(time_slot),
                        'evseIdsRawData': str(evseIds),
                        'mixedPlugTypes': mixedPlugTypes,
                        'mixedSpeeds': mixedSpeeds,
                    }
                    
                    success, error = self.insert_row('pricesLog', row_dict=data_row)
                    if success:
                        nsuccess += 1
                    else:
                        logger.warning(f"Failed to insert price data for locationId={locationId}, "
                                    f"plugType={plugTypes[0]}, product={product}: {error}")
            
            logger.debug(f"Inserted {nsuccess}/{ntotal} price entries for locationId={locationId}, "
                        f"plugType={plugTypes[0]}, speed={speeds[0]}")
        