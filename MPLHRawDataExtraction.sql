-- ============================================================================
-- TASK MATERIAL USAGE REPORT
-- ============================================================================
-- One row per task showing material usage, labor hours, and performance metrics
-- Filters to only include tasks using materials with material_category_id IN (1,2)
-- ============================================================================

WITH TaskLaborHours AS (
    -- Calculate total labor hours per task from timeclock records
    SELECT
        tasks.id AS task_id,
        SUM(CAST(ROUND(
            CASE
                WHEN timeclock.[out] IS NOT NULL AND timeclock.[in] IS NOT NULL
                THEN DATEDIFF(MINUTE, timeclock.[in], timeclock.[out]) / 60.0
                ELSE 0
            END, 1) AS FLOAT)) AS total_labor_hours
    FROM tasks
    INNER JOIN timeclock ON timeclock.task_id = tasks.id
    LEFT JOIN users ON users.id = timeclock.user_id
    WHERE timeclock.deleted_at IS NULL
      AND (users.id IS NULL OR LOWER(users.first_name) <> 'test')
    GROUP BY tasks.id
),

TaskLocationMetrics AS (
    -- Get square footage and estimated materials per task from task_locations
    -- Falls back to tasks.daily_sqft if task_locations data is null
    SELECT
        tasks.id AS task_id,
        COALESCE(
            SUM(COALESCE(tl.pm_sq_ft, tl.sq_ft)),
            MAX(tasks.daily_sqft),
            0
        ) AS wo_sqft
    FROM tasks
    LEFT JOIN task_locations tl ON tl.task_id = tasks.id AND tl.deleted_at IS NULL
    GROUP BY tasks.id
),

TaskMaterialMetrics AS (
    -- Get actual materials, estimated materials, and material ID per task from task_material
    -- Multiple task_material rows per task will be summed
    SELECT
        tasks.id AS task_id,
        MAX(tm.material_id) AS material_id,
        ISNULL(ROUND(SUM(tm.actual), 0), 0) AS actual_materials,
        COALESCE(
            SUM(COALESCE(tl.pm_gallons, tl.gallons)),
            SUM(tm.estimated),
            0
        ) AS est_materials
    FROM tasks
    LEFT JOIN task_material tm ON tm.task_id = tasks.id AND tm.deleted_at IS NULL
    LEFT JOIN task_locations tl ON tl.task_id = tasks.id AND tl.deleted_at IS NULL
    GROUP BY tasks.id
)

-- Main Query
SELECT
    -- Project and Location Information
    jobs.id AS [Project ID],
    
    CASE
        WHEN regions.id = 1 THEN 'HAU'
        WHEN regions.id = 5 THEN 'HAU - TX'
        WHEN regions.id = 4 THEN 'HAU - SE'
        WHEN regions.id = 2 THEN 'HAA'
        WHEN regions.id IS NULL THEN 'Null'
        ELSE 'Unknown'
    END AS Region,
    
    (job_area.state + ' - ' + job_area.area) AS SubRegion,
    locations.short_code AS Yard,
    jobs.zipcode AS [Zip Code],
    job_category.category AS Category,
    
    -- Service Information
    services.abbreviation AS Service,
    
    -- Task Information
    tasks.id AS WO,
    tasks.date AS [Date],
    YEAR(tasks.date) AS [Year],
    MONTH(tasks.date) AS [Month],
    
    -- Material Information
    tmm.material_id AS [Material ID],
    materials.material_category_id AS [Material Category],
    
    -- Area and Material Metrics
    CAST(ISNULL(tlm.wo_sqft, 0) AS DECIMAL(10,1)) AS [WO SqFt],
    CAST(ISNULL(tmm.est_materials, 0) AS DECIMAL(10,1)) AS [Est Materials],
    ISNULL(tmm.actual_materials, 0) AS [Actual Materials],
    
    -- AppRate: WO SqFt / Est Materials
    CASE 
        WHEN tmm.est_materials IS NOT NULL AND tmm.est_materials <> 0 
        THEN CAST(ROUND(tlm.wo_sqft / tmm.est_materials, 1) AS DECIMAL(10,1))
        ELSE NULL
    END AS AppRate,
    
    -- Act AppRate: WO SqFt / Actual Materials
    CASE 
        WHEN tmm.actual_materials IS NOT NULL AND tmm.actual_materials <> 0 
        THEN CAST(ROUND(tlm.wo_sqft / tmm.actual_materials, 1) AS DECIMAL(10,1))
        ELSE NULL
    END AS [Act AppRate],
    
    -- Round Actual Materials to nearest 50 (whole number)
    CAST(ROUND(tmm.actual_materials / 50.0, 0) * 50 AS INT) AS [Round],
    
    -- Labor Hours
    ISNULL(tlh.total_labor_hours, 0) AS [Total Labor Hrs],
    
    -- MPLH: Materials Per Labor Hour
    CASE 
        WHEN tlh.total_labor_hours IS NOT NULL AND tlh.total_labor_hours <> 0 
        THEN ROUND(tmm.actual_materials / tlh.total_labor_hours, 2)
        ELSE NULL
    END AS MPLH

FROM tasks
INNER JOIN work_orders ON work_orders.id = tasks.work_order_id
INNER JOIN jobs ON jobs.id = work_orders.job_id
INNER JOIN services ON services.id = work_orders.service_id
LEFT JOIN regions ON regions.id = jobs.region_id
LEFT JOIN job_area ON job_area.id = jobs.sub_region_id
LEFT JOIN locations ON locations.id = jobs.location_id
LEFT JOIN job_category ON job_category.id = jobs.category
LEFT JOIN project_stages ON project_stages.id = jobs.project_stage_id

-- Join CTEs
LEFT JOIN TaskLaborHours tlh ON tlh.task_id = tasks.id
LEFT JOIN TaskLocationMetrics tlm ON tlm.task_id = tasks.id
LEFT JOIN TaskMaterialMetrics tmm ON tmm.task_id = tasks.id

-- Join materials table to filter by category
INNER JOIN materials ON materials.id = tmm.material_id

WHERE jobs.deleted_at IS NULL
  AND work_orders.deleted_at IS NULL
  AND tasks.task_status_id <> 8  -- Exclude cancelled/skipped tasks
  AND jobs.project_stage_id <> 10  -- Exclude closed projects
  AND materials.material_category_id IN (1,2)  -- Only material categories 1 and 2
  AND tmm.material_id IS NOT NULL  -- Only tasks with materials

ORDER BY jobs.id, tasks.date, tasks.id;
