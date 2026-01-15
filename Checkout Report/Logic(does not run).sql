-- Not correct sql code, but logic is correct. Going to implement into report but have scout.
-- 
-- All date conversions going to do on the backend instead.
--  
-- EOD issue.  EOD need to be EOD in MST. All values are GMT in database. 
-- Convert GMT to MST than add 11:59:59.
-- Ensure that the date is changed back a day for GMT depending than using that date add 11:59:59
-- 
-- Use variable for others becuase they are subject to change 
-- 
-- super = 11am next day 
-- super = 24 + 11 =  35 hours more
-- BETWEEN(task_day 00:00:00, task_day + super hours)
-- 
-- PM = 2am next day
-- PM = 24 + 13 = 37
-- BETWEEN(task_day 00:00:00, task_day + PM hours)


SELECT

-- convert from GMT TO MSD
tasks.date AS WO_Date,

regions.name AS Region,
tasks.id AS WO_ID,
jobs.name AS Project_Name,
services.abbreviation AS Service,
task_statuses.name AS WO_Status,

CONCAT(foreman.first_name, ' ', foreman.last_name) AS Foreman,
CONCAT(completed_by_foreman.first_name, ' ', completed_by_foreman.last_name) AS FM_Completed_By,


-- FOREMAN
CASE
	WHEN tasks.completed_at IS NOT NULL THEN
-- 	convert GMT -> MST
	tasks.completed_at
	ELSE NULL
END AS FM_Completed_At_MDT,

CASE 
		-- 	Do the time converstion in larvel
	WHEN NOW(GMT) < CONCAT(DATE(tasks.date), + 24 hours) THEN 'Pending'
	WHEN tasks.forman_id IS NULL THEN 'No - foreman is null'
	WHEN tasks.completed_by IS NULL OR tasks.completed_at IS NULL THEN 'No - not completed'
	
-- 	Do the time converstion in larvel
-- issue is grabbing the end of day comparison it needs to be end of day in MST
	WHEN tasks.completed_at > CONCAT(DATE(tasks.date), + 24 ) THEN 'No - past deadline'
	WHEN tasks.completed_at <= CONCAT(DATE(tasks.date), + 24 THEN 'Yes'
	ELSE 'Other'
END AS FM_Checkout,


-- SUPER
CONCAT(superintendent.first_name, ' ', superintendent.last_name AS SuperIntendent,
CONCAT(super_completed_by.first_name, ' ', super_completed_by.last_name AS super_completed_by,

-- gmt -> mtd
    CASE
        WHEN tasks.super_completed_at IS NOT NULL THEN 
            (tasks.super_completed_at
        ELSE NULL
    END AS Super_Completed_At_MDT, 


CASE 
			-- 	Do the time converstion in larvel
	WHEN tasks.super_completed_at IS NULL AND NOW() <= tasks.date (11am the next day) THEN 'Pending'
	WHEN tasks.superintendent_id I NULL THEN "NO superintedent is null"
	WHEN tasks.superintendent_id IS NULL OR tasks.super_completed_at IS NULL THEN 'No - not completed'
	
-- 	comepleted_at (GMT) > tasks.date(GMT) (The next day at 11 MST)
	WHEN tasks.super_completed_at > tasks.date +(day and 11 hours) THEN "No - past deadline"
	
-- 	completed at(GMT) less than or equal to  the next day at 11 MST
--     ?Between the tasks date AND task date + 35 hours?? 
	WHEN tasks.suepr_completed_at <= tasks.date + (day and 11 hours) THEN "Yes"
	
	ELSE "Other"
END AS "Super_Checkout"


-- PROJECT MANAGER

CONCAT(project_manager.first_name, ' ', project_manager.last_name) AS Project_Manager,
CONCAT(PM_completed_by.first_name, ' ', PM_completed_by.last_name) AS PM_Completed_By,


-- gmt -> mtd
    CASE
        WHEN tasks.pm_completed_at IS NOT NULL THEN 
            tasks.pm_completed_at
        ELSE NULL
    END AS PM_Completed_At_MDT, 


CASE 
			-- 	Do the time converstion in larvel
	WHEN tasks.pm_completed_at IS NULL AND NOW() <= tasks.date (2 pm the next day) THEN 'Pending'
	WHEN tasks.pm_completed_at I NULL THEN "NO PM is null"
	WHEN tasks.pm_id IS NULL OR tasks.super_completed_at IS NULL THEN 'No - not completed'
	
-- 	comepleted_at (GMT) > tasks.date(GMT) (The next day at 11 MST)
	WHEN tasks.super_completed_at > tasks.date +(day and 11 hours) THEN "No - past deadline"
	
-- 	completed at(GMT) less than or equal to  the next day at 11 MST
--     ?Between the tasks date AND task date + 35 hours?? 
	WHEN tasks.super_completed_at <= tasks.date + (day and 11 hours) THEN "Yes"
	
	ELSE "Other"
END AS "Super_Checkout"



timeclock.in - timeclock.out AS labor_hours








FROM jobs
LEFT JOIN work_orders ON work_orders.job_id = jobs.id
LEFT JOIN tasks ON work_orders.id = tasks.work_order_id
LEFT JOIN regions on regions.id = jobs.region_id
LEFT JOIN services ON services.id = work_orders.service_id
LEFT JOIN task_statuses ON task_statuses.id = tasks.task_status_id
LEFT JOIN users AS foreman ON foreman.id = tasks.foreman_id
LEFT JOIN users AS completed_by_foreman ON completed_by_foreman.id = tasks.completed_by
LEFT JOIN users AS superintendent ON superintendent.id = tasks.superintendent_id
LEFT JOIN users AS super_completed_by ON super_completed_by.id = tasks.super_completed_by
LEFT JOIN users AS project_manager ON project_manager.id = jobs.manager_id
LEFT JOIN users AS PM_completed_by ON tasks.pm_completed_by = PM_completed_by.id

ORDER BY tasks.date DESC