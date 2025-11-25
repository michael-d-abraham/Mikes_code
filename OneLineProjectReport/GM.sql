WITH Next_WO_Date_Calculations AS (
    SELECT
        wo.id AS work_order_id,
        FORMAT(MIN(t.date), 'MM-dd-yyyy') AS Next_WO_Date
    FROM tasks t
    JOIN work_orders wo ON t.work_order_id = wo.id
    WHERE t.date > GETDATE()
    GROUP BY wo.id
),
Last_WO_Date_Calculations AS (
    SELECT
        wo.id AS work_order_id,
        FORMAT(MAX(t.date), 'MM-dd-yyyy') AS Last_WO_Date
    FROM tasks t
    JOIN work_orders wo ON t.work_order_id = wo.id
    WHERE t.date < GETDATE()
    GROUP BY wo.id
),
ST_Base AS (
    SELECT
        j.id AS job_id,
        wo.work_order_status_id AS ServiceStatusId,
        lwd.Last_WO_Date,
        nwd.Next_WO_Date,
        ROW_NUMBER() OVER (
            PARTITION BY j.id
            ORDER BY s.order_by_category, s.order_by_service, wo.id
        ) AS rn
    FROM jobs j
    INNER JOIN work_orders wo
        ON wo.job_id = j.id
       AND wo.deleted_at IS NULL
    INNER JOIN services s
        ON s.id = wo.service_id
       AND s.service_categories_id = 1
    LEFT JOIN work_order_services_json wosj
        ON wosj.work_order_id = wo.id
    LEFT JOIN Last_WO_Date_Calculations lwd
        ON lwd.work_order_id = wo.id
    LEFT JOIN Next_WO_Date_Calculations nwd
        ON nwd.work_order_id = wo.id
    WHERE
        j.deleted_at IS NULL
        AND j.project_stage_id <> 10
),
ST_YR_CMPLT AS (
    SELECT
        b.job_id,
        CASE 
            WHEN SUM(CASE WHEN b.Next_WO_Date IS NOT NULL THEN 1 ELSE 0 END) = 0
             AND SUM(CASE WHEN ISNULL(b.ServiceStatusId, -1) NOT IN (3,4,7) THEN 1 ELSE 0 END) = 0
             AND MAX(b.Last_WO_Date) IS NOT NULL
            THEN CONVERT(VARCHAR(4), DATEPART(YEAR, MAX(b.Last_WO_Date)))
            ELSE ''
        END AS [YR CMPLT]
    FROM ST_Base b
    GROUP BY b.job_id
)
SELECT
    '' AS [Blank],
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

    jobs.id AS ProjectID,
    jobs.name AS ProjectName,
    project_stages.display_name AS [Project Status],
    CONCAT(sales_rep.first_name, ' ', sales_rep.last_name) AS SalesRep,
    CONCAT(PM.first_name, ' ', PM.last_name) AS [PM],
    ISNULL(styr.[YR CMPLT], '') AS [YR CMPLT],
    '' AS [NS INV YR],
    '' AS [Blank],
    '' AS [ST Revenue],
    '' AS [ST Cost],
    '' AS [ST GM],
    '' AS [ST GM%],
    '' AS [Blank],
    '' AS [Repair Rev],
    '' AS [Repair Cost],
    '' AS [Repair GM],
    '' AS [Repair GM%],
    '' AS [Blank],
    '' AS [Ttl Revenue],
    '' AS [Ttl Cost],
    '' AS [Ttl GM],
    '' AS [Ttl GM %]

FROM jobs
LEFT JOIN ST_YR_CMPLT styr
    ON styr.job_id = jobs.id
LEFT JOIN customers         ON customers.id = jobs.customer_id
LEFT JOIN customers end_customer ON end_customer.id = jobs.end_customer_id
LEFT JOIN regions           ON regions.id = jobs.region_id
LEFT JOIN job_area          ON job_area.id = jobs.sub_region_id
LEFT JOIN locations         ON locations.id = jobs.location_id
LEFT JOIN users sales_rep   ON sales_rep.id = jobs.estimator_id
LEFT JOIN users sales_support ON sales_support.id = jobs.sales_support_id
LEFT JOIN users PM          ON PM.id = jobs.manager_id
LEFT JOIN project_populations ON project_populations.id = jobs.project_population_id
LEFT JOIN job_category      ON job_category.id = jobs.category
LEFT JOIN project_stages    ON project_stages.id = jobs.project_stage_id

WHERE jobs.deleted_at IS NULL
  AND jobs.project_status_id <> 10
  AND regions.active = 1
  AND customers.enabled = 1
  AND customers.deleted_at IS NULL
  AND job_area.active = 1
  AND job_category.active = 1
  AND LOWER(jobs.name) NOT LIKE '%warranty%'
  AND LOWER(jobs.name) NOT LIKE '%test%';
