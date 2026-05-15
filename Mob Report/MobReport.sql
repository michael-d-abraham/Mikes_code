-- Adjust the offset in DATEADD for daylight saving: use -6 or -7 as needed
SELECT 
    regions.name AS Users_Region,
--     MAX(timeclock.id) AS TimeCard_ID,
    timeclock.id AS TimeCard_ID,
    CONCAT(users.first_name, ' ', users.last_name) AS Name,
    FORMAT(DATEADD(HOUR, -7, MAX(user_trips.start_time)), 'M/d/yy h:mm tt') AS MST_Start_Time,
    start_location.display_name AS Start_Location,
    FORMAT(DATEADD(HOUR, -7, MAX(user_trips.end_time)), 'M/d/yy h:mm tt') AS MST_End_Time,
    end_location.display_name AS End_Location,
    user_trips.driver AS Driver,
    fleet.fleet_number,
    user_trips.flight AS Flight
FROM user_trips
LEFT JOIN users ON user_trips.user_id = users.id
LEFT JOIN timeclock ON user_trips.tripable_id = timeclock.id
LEFT JOIN cities AS start_location ON start_location.id = user_trips.start_location_id
LEFT JOIN cities AS end_location ON end_location.id = user_trips.end_location_id
LEFT JOIN fleet ON fleet.id = user_trips.fleet_id
LEFT JOIN regions ON regions.id = users.region_id
WHERE timeclock.deleted_at IS NULL 
  AND fleet.deleted_at IS NULL
	AND user_trips.start_time BETWEEN '2025-01-01 07:00:00' AND '2025-03-31 07:59:59'

GROUP BY 
--   timeclock.id,
    user_trips.id,
    timeclock.id,
    regions.name,
    CONCAT(users.first_name, ' ', users.last_name),
		start_location.display_name,
		end_location.display_name,
    user_trips.driver,
    fleet.fleet_number,
    user_trips.flight
ORDER BY MAX(user_trips.start_time);