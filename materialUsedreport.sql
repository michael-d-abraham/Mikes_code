DECLARE @start_date DATE = '2025-07-01';
DECLARE @end_date   DATE = '2025-07-31';

WITH labor_per_task_date AS (
    SELECT
        t.id AS task_id,
        t.work_order_id,
        t.date,
        SUM(
            DATEDIFF(MINUTE, tc.[in], tc.[out]) / 60.0
        ) AS Total_Labor_Hrs
    FROM tasks t
    INNER JOIN timeclock tc
        ON tc.task_id = t.id
        AND tc.deleted_at IS NULL
        AND tc.[out] IS NOT NULL
    WHERE t.date BETWEEN @start_date AND @end_date
      AND t.task_status_id <> 8
    GROUP BY
        t.id,
        t.work_order_id,
        t.date
),

materials_per_task_date AS (
    SELECT
        t.id AS task_id,
        t.work_order_id,
        t.date,
        s.material_id,
        SUM(tm.actual) AS Actual_Materials
    FROM tasks t
    INNER JOIN work_orders wo
        ON wo.id = t.work_order_id
        AND wo.deleted_at IS NULL
    INNER JOIN services s
        ON s.id = wo.service_id
    INNER JOIN task_material tm
        ON tm.task_id = t.id
        AND tm.deleted_at IS NULL
        AND tm.material_id = s.material_id
    WHERE t.date BETWEEN @start_date AND @end_date
      AND t.task_status_id <> 8
    GROUP BY
        t.id,
        t.work_order_id,
        t.date,
        s.material_id
)

SELECT
    j.id AS [Project ID],
    j.name AS [Project Name],

    CASE
        WHEN j.region_id = 1 THEN 'HAU'
        WHEN j.region_id = 5 THEN 'HAU - TX'
        WHEN j.region_id = 4 THEN 'HAU - SE'
        WHEN j.region_id = 2 THEN 'HAA'
        ELSE 'Unknown'
    END AS [Region],

    COALESCE(job_area.state, '') +
        CASE
            WHEN job_area.state IS NOT NULL AND job_area.area IS NOT NULL THEN ' - '
            ELSE ''
        END +
        COALESCE(job_area.area, '') AS [SubRegion],

    locations.short_code AS [Yard],

    sc.name AS [Category],
    s.name AS [Service],

    wo.id AS [ServiceId],      -- service_id in your output = work_orders.id
    mpd.task_id AS [WO ID],    -- work order id in your output = tasks.id

    mpd.date AS [Date],
    mpd.material_id AS [Material ID],
    mpd.Actual_Materials AS [Actual Materials],
    CAST(ROUND(ISNULL(ltd.Total_Labor_Hrs, 0), 2) AS DECIMAL(10,2)) AS [Total Labor Hrs],

    CASE
        WHEN ISNULL(ltd.Total_Labor_Hrs, 0) = 0 THEN NULL
        ELSE CAST(ROUND(mpd.Actual_Materials / ltd.Total_Labor_Hrs, 2) AS DECIMAL(10,2))
    END AS [MPLH]

FROM materials_per_task_date mpd
INNER JOIN work_orders wo
    ON wo.id = mpd.work_order_id
    AND wo.deleted_at IS NULL
INNER JOIN jobs j
    ON j.id = wo.job_id
    AND j.deleted_at IS NULL
    AND j.project_stage_id <> 10
LEFT JOIN services s
    ON s.id = wo.service_id
LEFT JOIN service_categories sc
    ON sc.id = s.service_categories_id
LEFT JOIN labor_per_task_date ltd
    ON ltd.task_id = mpd.task_id
    AND ltd.date = mpd.date
LEFT JOIN job_area
    ON job_area.id = j.sub_region_id
LEFT JOIN locations
    ON locations.id = j.location_id

ORDER BY
    j.id,
    wo.id,
    mpd.task_id,
    mpd.date,
    mpd.material_id;