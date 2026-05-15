-- CTEs
-- Add AggregatedTaskData CTE here:

WITH AggregatedTaskData AS (
    SELECT
        wo.id AS work_order_id,
        SUM(CASE WHEN t.weather_issue = 0 THEN DATEDIFF(MINUTE, tc.[in], tc.[out]) / 60.0 ELSE 0 END) AS total_labor_hours,
        SUM(CASE WHEN t.weather_issue = 1 THEN DATEDIFF(MINUTE, tc.[in], tc.[out]) / 60.0 ELSE 0 END) AS total_weather_issue_hours,
        COUNT(DISTINCT CAST(tc.[in] AS DATE)) * 1.0 / 3.0 AS sum_daily_crews,
        COUNT(DISTINCT CAST(tc.[in] AS DATE)) AS sum_days_touched,
        SUM(
            CASE
                WHEN total_hours.total_hours BETWEEN 0 AND 3 THEN 0.3
                WHEN total_hours.total_hours BETWEEN 3 AND 5 THEN 0.5
                WHEN total_hours.total_hours BETWEEN 5 AND 8 THEN 0.75
                WHEN total_hours.total_hours > 8 THEN 1
                ELSE 0
            END
        ) AS total_crew_size,
        SUM(CASE WHEN total_hours.total_hours > 10 THEN total_hours.total_hours - 10 ELSE 0 END) AS total_adjusted_hours,
        COUNT(DISTINCT CASE WHEN total_hours.total_hours > 10 THEN tc.user_id END) AS total_adjusted_users
    FROM work_orders wo
    LEFT JOIN tasks t ON t.work_order_id = wo.id
    LEFT JOIN timeclock tc ON tc.task_id = t.id AND tc.deleted_at IS NULL
    OUTER APPLY (
        SELECT
            SUM(DATEDIFF(MINUTE, tc2.[in], tc2.[out])) / 60.0 AS total_hours
        FROM timeclock tc2
        WHERE tc2.task_id = tc.task_id AND tc2.user_id = tc.user_id AND tc2.deleted_at IS NULL
    ) AS total_hours
    WHERE wo.deleted_at IS NULL
    GROUP BY wo.id
),

CorrectAddEquipHrs AS (
    SELECT 
        wo.id AS work_order_id,
        SUM(
            CASE
                WHEN lh.total_labor_hours > (uc.user_count * 10) 
                THEN (lh.total_labor_hours - (uc.user_count * 10)) / NULLIF(uc.user_count, 0)
                ELSE NULL
            END
        ) AS addl_equip_hours
    FROM work_orders wo
    JOIN tasks t ON t.work_order_id = wo.id
    LEFT JOIN (
        SELECT
            task_id,
            SUM(DATEDIFF(MINUTE, [in], [out])) / 60.0 AS total_labor_hours
        FROM timeclock
        WHERE deleted_at IS NULL
        GROUP BY task_id
    ) lh ON lh.task_id = t.id
    LEFT JOIN (
        SELECT
            task_id,
            COUNT(DISTINCT user_id) AS user_count
        FROM timeclock
        WHERE deleted_at IS NULL
        GROUP BY task_id
    ) uc ON uc.task_id = t.id
    WHERE wo.deleted_at IS NULL
    GROUP BY wo.id
),

CorrectCrewSize AS (
    SELECT 
        wo.id AS work_order_id,
        ROUND(SUM(task_user_counts.user_count * 1.0) / NULLIF(COUNT(t.id), 0), 2) AS avg_crew_size
    FROM work_orders wo
    JOIN tasks t ON t.work_order_id = wo.id
    JOIN (
        SELECT 
            tc.task_id,
            COUNT(DISTINCT tc.user_id) AS user_count
        FROM timeclock tc
        WHERE tc.deleted_at IS NULL
        GROUP BY tc.task_id
    ) AS task_user_counts ON task_user_counts.task_id = t.id
    WHERE wo.deleted_at IS NULL
    GROUP BY wo.id
),

CorrectDailyCrews AS (
    SELECT 
        wo.id AS work_order_id,
        ROUND(SUM(user_counts.user_count * 1.0 / 3.0) / NULLIF(COUNT(t.id), 0), 2) AS daily_crews
    FROM work_orders wo
    JOIN tasks t ON t.work_order_id = wo.id
    JOIN (
        SELECT
            tc.task_id,
            COUNT(DISTINCT tc.user_id) AS user_count
        FROM timeclock tc
        WHERE tc.deleted_at IS NULL
        GROUP BY tc.task_id
    ) AS user_counts ON user_counts.task_id = t.id
    WHERE wo.deleted_at IS NULL
    GROUP BY wo.id
),

CorrectDaysTouched AS (
    SELECT 
        wo.id AS work_order_id,
        SUM(
            CASE 
                WHEN ROUND(lh.total_labor_hours / NULLIF(uc.user_count, 0) * 0.1 * 1.05, 2) < 1
                    THEN ROUND(lh.total_labor_hours / NULLIF(uc.user_count, 0) * 0.1 * 1.05, 2)
                ELSE 1
            END
        ) AS sum_days_touched
    FROM work_orders wo
    JOIN tasks t ON t.work_order_id = wo.id
    LEFT JOIN (
        SELECT
            task_id,
            SUM(DATEDIFF(MINUTE, [in], [out])) / 60.0 AS total_labor_hours
        FROM timeclock
        WHERE deleted_at IS NULL
        GROUP BY task_id
    ) lh ON lh.task_id = t.id
    LEFT JOIN (
        SELECT
            task_id,
            COUNT(DISTINCT user_id) AS user_count
        FROM timeclock
        WHERE deleted_at IS NULL
        GROUP BY task_id
    ) uc ON uc.task_id = t.id
    WHERE wo.deleted_at IS NULL
    GROUP BY wo.id
),


SupportServiceHours AS (
    SELECT 
        j.id AS job_id,
        SUM(CASE WHEN wo.service_id = 1  THEN DATEDIFF(MINUTE, tc.[in], tc.[out]) / 60.0 ELSE 0 END) AS Noticing_Labor_Hrs,
        SUM(CASE WHEN wo.service_id = 52 THEN DATEDIFF(MINUTE, tc.[in], tc.[out]) / 60.0 ELSE 0 END) AS Shuttle_Labor_Hrs,
        SUM(CASE WHEN wo.service_id = 57 THEN DATEDIFF(MINUTE, tc.[in], tc.[out]) / 60.0 ELSE 0 END) AS TrafficControl_Labor_Hrs,
        SUM(CASE WHEN wo.service_id = 15 THEN DATEDIFF(MINUTE, tc.[in], tc.[out]) / 60.0 ELSE 0 END) AS CleaningPJO_Labor_Hrs,
        SUM(CASE WHEN wo.service_id = 74 THEN DATEDIFF(MINUTE, tc.[in], tc.[out]) / 60.0 ELSE 0 END) AS PreCleanTruck_Labor_Hrs
    FROM jobs j
    JOIN work_orders wo ON wo.job_id = j.id AND wo.deleted_at IS NULL
    JOIN tasks t        ON t.work_order_id = wo.id
    JOIN timeclock tc   ON tc.task_id = t.id AND tc.deleted_at IS NULL
    WHERE j.deleted_at IS NULL
      AND wo.service_id IN (1,52,57,15,74)
    GROUP BY j.id
),


AGCSummary AS (
    SELECT
        jobs.id AS job_id,
        work_orders.id AS work_order_id,
        CASE 
            WHEN COUNT(task_agcs.id) > 0 THEN 'Yes'
            ELSE 'No'
        END AS has_agc,
        SUM(task_agcs.sqft) AS total_agc_sqft,
        SUM(task_agcs.labor_hours) AS total_agc_hours
    FROM jobs
    JOIN work_orders ON work_orders.job_id = jobs.id
    JOIN tasks ON tasks.work_order_id = work_orders.id
    LEFT JOIN task_agcs ON task_agcs.task_id = tasks.id AND task_agcs.deleted_at IS NULL
    WHERE jobs.deleted_at IS NULL
      AND work_orders.deleted_at IS NULL
    GROUP BY jobs.id, work_orders.id
),

LaborHours AS (
    SELECT
        jobs.id AS job_id,
        work_orders.id AS work_order_id,
        SUM(CAST(ROUND(
            CASE
                WHEN timeclock.[out] IS NOT NULL AND timeclock.[in] IS NOT NULL
                THEN DATEDIFF(MINUTE, timeclock.[in], timeclock.[out]) / 60.0
                ELSE 0
            END, 1) AS FLOAT)) AS total_labor_hours
    FROM timeclock
    INNER JOIN tasks ON tasks.id = timeclock.task_id
    INNER JOIN work_orders ON work_orders.id = tasks.work_order_id AND work_orders.deleted_at IS NULL
    INNER JOIN services ON services.id = work_orders.service_id
    INNER JOIN jobs ON jobs.id = work_orders.job_id AND jobs.deleted_at IS NULL
    LEFT JOIN project_stages ON project_stages.id = jobs.project_stage_id
    LEFT JOIN users ON users.id = timeclock.user_id
        AND LOWER(users.first_name) <> 'test'
    WHERE
        project_stages.id <> 10
        AND timeclock.deleted_at IS NULL
    GROUP BY jobs.id, work_orders.id
),

TaskLocationSums AS (
    SELECT
        jobs.id AS job_id,
        work_orders.id AS work_order_id,
        SUM(COALESCE(tl.pm_sq_ft, tl.sq_ft, 0)) AS Adj_SqFt,
        SUM(COALESCE(tl.pm_gallons, tl.gallons, 0)) AS Adj_Gal
    FROM task_locations tl
    INNER JOIN tasks ON tasks.id = tl.task_id
    INNER JOIN work_orders ON work_orders.id = tasks.work_order_id
    INNER JOIN services ON services.id = work_orders.service_id
    INNER JOIN jobs ON jobs.id = work_orders.job_id
    WHERE
        tl.deleted_at IS NULL
        AND jobs.deleted_at IS NULL
        AND work_orders.deleted_at IS NULL
        AND jobs.project_stage_id <> 10
    GROUP BY jobs.id, work_orders.id
),

TaskMaterialSums AS (
    SELECT
        jobs.id AS job_id,
        work_orders.id AS work_order_id,
        SUM(ISNULL(tm.actual, 0)) AS Actual_Gal
    FROM task_material tm
    INNER JOIN tasks ON tasks.id = tm.task_id
    INNER JOIN work_orders ON work_orders.id = tasks.work_order_id
    INNER JOIN services ON services.id = work_orders.service_id
    INNER JOIN jobs ON jobs.id = work_orders.job_id
    WHERE
        tm.deleted_at IS NULL
        AND jobs.deleted_at IS NULL
        AND work_orders.deleted_at IS NULL
        AND jobs.project_stage_id <> 10
    GROUP BY jobs.id, work_orders.id
),

CountWOs AS (
    SELECT
        j.id AS job_id,
        COUNT(DISTINCT wo.id) AS wo_count
    FROM work_orders wo
    INNER JOIN services s ON s.id = wo.service_id
    INNER JOIN jobs j ON j.id = wo.job_id
    INNER JOIN tasks t ON t.work_order_id = wo.id
    WHERE j.deleted_at IS NULL
        AND wo.deleted_at IS NULL
        AND j.project_stage_id <> 10
    GROUP BY j.id
),

IssueTasks AS (
    SELECT DISTINCT t.id AS task_id
    FROM tasks t
    LEFT JOIN task_fleet tf ON tf.task_id = t.id
    WHERE t.weather_issue = 1 OR t.job_incident = 1 OR tf.has_issue = 1
),

IssueTaskLabor AS (
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

IssueTaskLocationSums AS (
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

IssueFleetIssues AS (
    SELECT
        task_id,
        MAX(CASE WHEN has_issue = 1 THEN 1 ELSE 0 END) AS has_fleet_issue
    FROM task_fleet
    GROUP BY task_id
),

IssueTasksWithInfo AS (
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
    LEFT JOIN IssueTaskLabor labor ON labor.task_id = tasks.id
    LEFT JOIN IssueTaskLocationSums loc ON loc.task_id = tasks.id
    LEFT JOIN IssueFleetIssues fleet ON fleet.task_id = tasks.id
),

IssueSummary AS (
    SELECT 
        t.job_id,
        t.work_order_id,
        FORMAT(SUM(CASE WHEN t.is_issue_task = 1 THEN t.labor_hours ELSE 0 END), '0.##') AS Issues_Labor,
        COUNT(DISTINCT CASE WHEN t.is_issue_task = 1 THEN t.task_id END) AS WO_Issues_Count,
        SUM(CASE WHEN t.is_issue_task = 1 THEN t.sq_ft - t.pm_sq_ft ELSE 0 END) AS WO_Issues_SqFt,
        SUM(CASE WHEN t.is_issue_task = 1 THEN t.gallons - t.pm_gallons ELSE 0 END) AS WO_Issues_Gal
    FROM IssueTasksWithInfo t
    GROUP BY t.job_id, t.work_order_id
)

-- Main Query
SELECT
    jobs.id AS job_id,
    work_orders.service_id AS service_type_id,
    services.abbreviation AS service_name,
    work_orders.id AS service_id,
    work_order_statuses.name AS ServiceStatus,

    (
        ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.completion_season'), 'TBD') 
        + ' - ' + 
        ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.completion_year'), 'TBD')
    ) AS TCD,

    -- JSON Fields
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.reflect_button_qty'), '0') AS [Reflect Button Wrap Qty],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.bumper_block_qty'), '0') AS [Bumper Block Wrap Qty],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.pole_wrap_qty'), '0') AS [Pole Wrap Qty],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.oil_spots_qty'), '0') AS [Oil Spots Qty],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.overhead_parking'), '0') AS [Overhead Parking],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.garage_doors'), '0') AS [Garage Doors 10ft],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.valley_gutters'), '0') AS [Valley Gutters],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.day_split_qty'), '0') AS [Day Split],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.supportServices.requires_noticing'), '0') AS [Req Noticing],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.supportServices.requires_shuttle'), '0') AS [Req Shuttle],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.supportServices.requires_traffic_control'), '0') AS [Req Traffic Control],

		adps.id AS ADP,

    -- Aggregated Sales & PM Data
    (
        SELECT SUM(TRY_CAST(JSON_VALUE([value], '$.area_sq_ft_total') AS FLOAT))
        FROM OPENJSON(work_order_services_json.[fields], '$.areaDetails')
    ) AS [Sales SqFt],
    (
        SELECT CAST(SUM(TRY_CAST(JSON_VALUE([value], '$.area_sq_ft_total') AS FLOAT)) / 9.0 AS INT)
        FROM OPENJSON(work_order_services_json.[fields], '$.areaDetails')
    ) AS [Sales Sq Yd],
    (
        SELECT CAST(SUM(
            TRY_CAST(JSON_VALUE([value], '$.area_sq_ft_total') AS FLOAT)
            / NULLIF(TRY_CAST(JSON_VALUE([value], '$.area_new_app_rate') AS FLOAT), 0)
        ) AS INT)
        FROM OPENJSON(work_order_services_json.[fields], '$.areaDetails')
    ) AS [Sales Est Gallons],
    (
        SELECT SUM(
            TRY_CAST(
                COALESCE(
                    JSON_VALUE([value], '$.area_ops_sq_ft_total'),
                    JSON_VALUE([value], '$.area_sq_ft_total')
                ) AS FLOAT
            )
        )
        FROM OPENJSON(work_order_services_json.[fields], '$.areaDetails')
    ) AS [PM SqFt],
    (
        SELECT CAST(SUM(TRY_CAST(JSON_VALUE([value], '$.area_ops_sq_ft_total') AS FLOAT)) / 9.0 AS INT)
        FROM OPENJSON(work_order_services_json.[fields], '$.areaDetails')
    ) AS [PM Sq Yd],
    (
        SELECT CAST(SUM(
            TRY_CAST(
                COALESCE(
                    JSON_VALUE([value], '$.area_ops_sq_ft_total'),
                    JSON_VALUE([value], '$.area_sq_ft_total')
                ) AS FLOAT
            ) / NULLIF(
                TRY_CAST(
                    COALESCE(
                        JSON_VALUE([value], '$.area_new_app_rate'),
                        JSON_VALUE([value], '$.area_app_rate')
                    ) AS FLOAT
                ), 0)
        ) AS INT)
        FROM OPENJSON(work_order_services_json.[fields], '$.areaDetails')
    ) AS [PM Est Gallons],

    -- WO metrics
    agc.has_agc AS [Aggressive Spot Cleaning],
    agc.total_agc_sqft AS [AGC SqFt],
    agc.total_agc_hours AS [AGC Hours],
    '' AS [AGC Revenue],
    tls.Adj_SqFt AS [WO SqFt],
    tls.Adj_Gal AS [WO Est Gallons],
    ISNULL(iss.WO_Issues_SqFt, 0) AS [WO Issues SqFt],
    ISNULL(iss.WO_Issues_Gal, 0) AS [WO Issues Gal],
    ISNULL(iss.WO_Issues_Count, 0) AS [# of WO issues],
    ISNULL(iss.Issues_Labor, '0.00') AS [Issues Labor],
    CountWOs.wo_count AS [# WO's],
    tms.Actual_Gal AS [TtlActQty],
		CASE
				WHEN tls.Adj_Gal IS NULL OR tls.Adj_Gal = 0 THEN NULL
				    ELSE CAST(CAST(ROUND((tms.Actual_Gal / tls.Adj_Gal) * 100, 0) AS INT) AS VARCHAR) + '%'
		END AS [%CMPT],
    tsh.total_labor_hours AS [Total Labor Hours],

-- New MPLH logic: TtlActQty / (Total Labor Hours - Issues Labor)
-- Formatted MPLH as 51.40
FORMAT(
    tms.Actual_Gal / 
    NULLIF(
        tsh.total_labor_hours - TRY_CAST(iss.Issues_Labor AS FLOAT),
        0
    ),
    'N2'
) AS [MPLH],

FORMAT(ssh.Noticing_Labor_Hrs, 'N2') AS [Noticing Labor Hrs],
FORMAT(ssh.Shuttle_Labor_Hrs, 'N2') AS [Shuttle Labor Hrs],
FORMAT(ssh.TrafficControl_Labor_Hrs, 'N2') AS [Traffic Control Labor Hrs],
FORMAT(ssh.CleaningPJO_Labor_Hrs, 'N2') AS [Cleaning PJO Labor Hrs],
FORMAT(ssh.PreCleanTruck_Labor_Hrs, 'N2') AS [PreClean Truck Labor Hrs],


FORMAT(cdc.daily_crews, '0.##') AS [Daily Crews],

FORMAT(cdt.sum_days_touched, '0.##') AS [Days Touched],


FORMAT(ccs.avg_crew_size, '0.##') AS [Crew Size],

FORMAT(cae.addl_equip_hours, '0.##') AS [Add'l Equip Hrs],


    CAST(NULL AS VARCHAR) AS [Days Based on Hrs],
		CAST(NULL AS VARCHAR) AS [Proposal Amount],
		CAST(NULL AS VARCHAR) AS [NS Invoice #],
		CAST(NULL AS VARCHAR) AS [NS Invoice Amount]
		

FROM jobs
LEFT JOIN work_orders ON work_orders.job_id = jobs.id
LEFT JOIN services ON services.id = work_orders.service_id
LEFT JOIN work_order_services_json ON work_order_services_json.work_order_id = work_orders.id
LEFT JOIN TaskLocationSums tls ON tls.work_order_id = work_orders.id AND tls.job_id = jobs.id
LEFT JOIN TaskMaterialSums tms ON tms.work_order_id = work_orders.id AND tms.job_id = jobs.id
LEFT JOIN LaborHours tsh ON tsh.work_order_id = work_orders.id AND tsh.job_id = jobs.id
LEFT JOIN adps ON adps.project_id = jobs.id AND work_orders.id = adps.service_id
LEFT JOIN CountWOs ON CountWOs.job_id = jobs.id
LEFT JOIN AggregatedTaskData atd ON atd.work_order_id = work_orders.id
LEFT JOIN work_order_statuses ON work_order_statuses.id = work_orders.work_order_status_id
LEFT JOIN AGCSummary agc ON agc.job_id = jobs.id AND agc.work_order_id = work_orders.id
LEFT JOIN IssueSummary iss ON iss.job_id = jobs.id AND iss.work_order_id = work_orders.id
LEFT JOIN SupportServiceHours ssh ON ssh.job_id = jobs.id
LEFT JOIN CorrectDailyCrews cdc ON cdc.work_order_id = work_orders.id
LEFT JOIN CorrectDaysTouched cdt ON cdt.work_order_id = work_orders.id
LEFT JOIN CorrectCrewSize ccs ON ccs.work_order_id = work_orders.id
LEFT JOIN CorrectAddEquipHrs cae ON cae.work_order_id = work_orders.id






WHERE jobs.deleted_at IS NULL
  AND work_orders.deleted_at IS NULL
  AND jobs.id = 6788
-- 	ONLY doing sealCoast - STD the support servies wont be picked up
--   AND work_orders.service_id = 10
AND services.service_categories_id = 1

ORDER BY jobs.id;