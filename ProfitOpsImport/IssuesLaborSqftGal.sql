WITH IssueTasks AS (
    SELECT DISTINCT t.id, t.work_order_id
    FROM tasks t
    LEFT JOIN task_fleet tf ON tf.task_id = t.id
    WHERE t.weather_issue = 1 OR t.job_incident = 1 OR tf.has_issue = 1
),

TaskLabor AS (
    SELECT 
        task_id,
        work_orders.id AS work_order_id,
        jobs.id AS job_id,
        SUM(DATEDIFF(MINUTE, [in], [out])) / 60.0 AS labor_hours
    FROM timeclock
    INNER JOIN tasks ON tasks.id = timeclock.task_id
    INNER JOIN work_orders ON work_orders.id = tasks.work_order_id
    INNER JOIN jobs ON jobs.id = work_orders.job_id
    WHERE timeclock.deleted_at IS NULL
    GROUP BY task_id, work_orders.id, jobs.id
),

TaskLocationSums AS (
    SELECT 
        task_id,
        SUM(COALESCE(sq_ft, 0)) AS total_sq_ft,
        SUM(COALESCE(pm_sq_ft, 0)) AS total_pm_sq_ft,
        SUM(COALESCE(gallons, 0)) AS total_gallons,
        SUM(COALESCE(pm_gallons, 0)) AS total_pm_gallons
    FROM task_locations
    WHERE deleted_at IS NULL
    GROUP BY task_id
),

FleetIssues AS (
    SELECT
        task_id,
        MAX(CASE WHEN has_issue = 1 THEN 1 ELSE 0 END) AS has_fleet_issue
    FROM task_fleet
    GROUP BY task_id
),

TasksWithInfo AS (
    SELECT
        tasks.id AS task_id,
        tasks.work_order_id,
        jobs.id AS job_id,
        CASE WHEN tasks.weather_issue = 1 THEN 1 ELSE 0 END AS weather_issue,
        COALESCE(fleet.has_fleet_issue, 0) AS fleet_issue,
        CASE WHEN tasks.job_incident = 1 THEN 1 ELSE 0 END AS job_incident,
        CASE 
            WHEN tasks.weather_issue = 1 OR COALESCE(fleet.has_fleet_issue, 0) = 1 OR tasks.job_incident = 1 THEN 1
            ELSE 0
        END AS is_issue_task,
        COALESCE(labor.labor_hours, 0) AS labor_hours,
        COALESCE(loc.total_sq_ft, 0) AS sq_ft,
        COALESCE(loc.total_pm_sq_ft, 0) AS pm_sq_ft,
        COALESCE(loc.total_gallons, 0) AS gallons,
        COALESCE(loc.total_pm_gallons, 0) AS pm_gallons
    FROM tasks
    LEFT JOIN work_orders ON work_orders.id = tasks.work_order_id
    LEFT JOIN jobs ON jobs.id = work_orders.job_id
    LEFT JOIN TaskLabor labor ON labor.task_id = tasks.id
    LEFT JOIN TaskLocationSums loc ON loc.task_id = tasks.id
    LEFT JOIN FleetIssues fleet ON fleet.task_id = tasks.id
)

SELECT 
    t.job_id,
    t.work_order_id,

    -- Total non-weather labor hours for the WO
FORMAT(
    SUM(CASE 
        WHEN t.is_issue_task = 1 
        THEN t.labor_hours 
        ELSE 0 
    END),
    '0.##'
) AS total_labor_hours,


    -- Number of distinct tasks in the WO that had *any* issue
    COUNT(DISTINCT CASE WHEN t.is_issue_task = 1 THEN t.task_id END) AS [# of WO Issues],

    -- SqFt and Gallon diffs only for issue tasks
    SUM(CASE WHEN t.is_issue_task = 1 THEN t.sq_ft - t.pm_sq_ft ELSE 0 END) AS [WO Issues SqFt],
    SUM(CASE WHEN t.is_issue_task = 1 THEN t.gallons - t.pm_gallons ELSE 0 END) AS [WO Issues Gal]

FROM TasksWithInfo t
WHERE t.job_id = 7820
GROUP BY t.job_id, t.work_order_id
HAVING COUNT(DISTINCT CASE WHEN t.is_issue_task = 1 THEN t.task_id END) > 0
ORDER BY t.job_id, t.work_order_id;
