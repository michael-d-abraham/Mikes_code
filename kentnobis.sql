-- ============================================================================
-- SIMPLIFIED REPORT - One row per work order, all service categories included
-- ============================================================================

WITH ProjectInfo AS (
    SELECT
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
        jobs.id   AS ProjectID,
        jobs.name AS ProjectName,
        project_stages.display_name AS [Project Status],
        CONCAT(sales_rep.first_name, ' ', sales_rep.last_name) AS SalesRep,
        CONCAT(PM.first_name, ' ', PM.last_name)               AS PM
    FROM jobs
    LEFT JOIN customers              ON customers.id           = jobs.customer_id
    LEFT JOIN customers end_customer ON end_customer.id        = jobs.end_customer_id
    LEFT JOIN regions                ON regions.id             = jobs.region_id
    LEFT JOIN job_area               ON job_area.id            = jobs.sub_region_id
    LEFT JOIN locations              ON locations.id           = jobs.location_id
    LEFT JOIN users sales_rep        ON sales_rep.id           = jobs.estimator_id
    LEFT JOIN users sales_support    ON sales_support.id       = jobs.sales_support_id
    LEFT JOIN users PM               ON PM.id                  = jobs.manager_id
    LEFT JOIN project_populations    ON project_populations.id = jobs.project_population_id
    LEFT JOIN job_category           ON job_category.id        = jobs.category
    LEFT JOIN project_stages         ON project_stages.id      = jobs.project_stage_id
    WHERE jobs.deleted_at IS NULL
      AND jobs.project_status_id <> 10
      AND LOWER(jobs.name) NOT LIKE '%warranty%'
      AND LOWER(jobs.name) NOT LIKE '%test%'
      AND jobs.estimator_id = 648
),

Next_WO_Date_Calculations AS (
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

LaborHours AS (
    SELECT
        jobs.id        AS job_id,
        work_orders.id AS work_order_id,
        SUM(CAST(ROUND(
            CASE
                WHEN timeclock.[out] IS NOT NULL
                 AND timeclock.[in]  IS NOT NULL
                THEN DATEDIFF(MINUTE, timeclock.[in], timeclock.[out]) / 60.0
                ELSE 0
            END, 1) AS FLOAT)) AS total_labor_hours
    FROM timeclock
    INNER JOIN tasks        ON tasks.id = timeclock.task_id
    INNER JOIN work_orders  ON work_orders.id = tasks.work_order_id
                           AND work_orders.deleted_at IS NULL
    INNER JOIN services     ON services.id = work_orders.service_id
    INNER JOIN jobs         ON jobs.id = work_orders.job_id
                           AND jobs.deleted_at IS NULL
    LEFT JOIN project_stages ON project_stages.id = jobs.project_stage_id
    LEFT JOIN users          ON users.id = timeclock.user_id
                            AND LOWER(users.first_name) <> 'test'
    WHERE
        project_stages.id <> 10
        AND timeclock.deleted_at IS NULL
    GROUP BY jobs.id, work_orders.id
),

Est_Material_Calculations AS (
    SELECT
        wosj.work_order_id,
        wosj.service_id,
        CASE
            WHEN wosj.service_id = 9 THEN ROUND(SUM(
                CASE
                    WHEN LTRIM(RTRIM(area_details.area_new_app_rate)) <> '' 
                         AND area_details.area_new_app_rate IS NOT NULL
                    THEN ISNULL(TRY_CAST(area_details.area_sq_ft_total AS FLOAT), 0) 
                         / NULLIF(ISNULL(TRY_CAST(area_details.area_new_app_rate AS FLOAT), 0), 0)
                    ELSE ISNULL(TRY_CAST(area_details.area_sq_ft_total AS FLOAT), 0) 
                         / NULLIF(ISNULL(TRY_CAST(area_details.area_app_rate AS FLOAT), 0), 0)
                END
            ), 0)
            WHEN wosj.service_id = 7 THEN ROUND(SUM(ISNULL(TRY_CAST(area_details.area_est_box_count AS FLOAT), 0)), 0)
            WHEN wosj.service_id IN (26, 30, 17) THEN ROUND(SUM(COALESCE(TRY_CAST(area_details.area_est_box_count AS FLOAT), 0)), 0)
            WHEN wosj.service_id IN (10, 12, 27, 28) THEN ROUND(SUM(
                ISNULL(TRY_CAST(area_details.area_sq_ft_total AS FLOAT), 0) /
                COALESCE(
                    NULLIF(TRY_CAST(area_details.area_new_app_rate AS FLOAT), 0),
                    NULLIF(TRY_CAST(area_details.area_app_rate AS FLOAT), 0)
                )
            ), 0)
            ELSE NULL
        END AS Est_Material
    FROM work_order_services_json wosj
    CROSS APPLY OPENJSON(wosj.[fields], '$.areaDetails') AS j
    CROSS APPLY OPENJSON(j.value) WITH (
        area_sq_ft_total   VARCHAR(255) '$.area_sq_ft_total',
        area_app_rate      VARCHAR(255) '$.area_app_rate',
        area_new_app_rate  VARCHAR(255) '$.area_new_app_rate',
        area_est_box_count VARCHAR(255) '$.area_est_box_count'
    ) AS area_details
    WHERE wosj.deleted_at IS NULL
    GROUP BY wosj.work_order_id, wosj.service_id
),

Act_Material_Calculations AS (
    SELECT
        wo.id AS work_order_id,
        ROUND(SUM(tm.actual), 0) AS Act_Material
    FROM tasks t
    LEFT JOIN task_material tm ON tm.task_id = t.id
    LEFT JOIN work_orders wo   ON t.work_order_id = wo.id
    WHERE
        t.material_complete IS NOT NULL
        AND tm.material_id = (
            SELECT s.material_id FROM services s WHERE s.id = wo.service_id
        )
        AND tm.deleted_at IS NULL
    GROUP BY wo.id
),

TaskLocationSums AS (
    SELECT
        jobs.id        AS job_id,
        work_orders.id AS work_order_id,
        SUM(COALESCE(tl.pm_sq_ft, tl.sq_ft, 0))     AS Adj_SqFt,
        SUM(COALESCE(tl.pm_gallons, tl.gallons, 0)) AS Adj_Gal
    FROM task_locations tl
    INNER JOIN tasks        ON tasks.id = tl.task_id
    INNER JOIN work_orders  ON work_orders.id = tasks.work_order_id
    INNER JOIN services     ON services.id = work_orders.service_id
    INNER JOIN jobs         ON jobs.id = work_orders.job_id
    WHERE
        tl.deleted_at IS NULL
        AND jobs.deleted_at IS NULL
        AND work_orders.deleted_at IS NULL
        AND jobs.project_stage_id <> 10
    GROUP BY jobs.id, work_orders.id
),

TaskMaterialSums AS (
    SELECT
        jobs.id        AS job_id,
        work_orders.id AS work_order_id,
        SUM(ISNULL(tm.actual, 0)) AS Actual_Gal
    FROM task_material tm
    INNER JOIN tasks        ON tasks.id = tm.task_id
    INNER JOIN work_orders  ON work_orders.id = tasks.work_order_id
    INNER JOIN services     ON services.id = work_orders.service_id
    INNER JOIN jobs         ON jobs.id = work_orders.job_id
    WHERE
        tm.deleted_at IS NULL
        AND jobs.deleted_at IS NULL
        AND work_orders.deleted_at IS NULL
        AND jobs.project_stage_id <> 10
    GROUP BY jobs.id, work_orders.id
),

CountTasks AS (
    SELECT
        j.id AS job_id,
        wo.id AS work_order_id,
        wo.service_id,
        COUNT(DISTINCT CASE WHEN t.task_status_id <> 8 THEN t.id END) AS task_count
    FROM work_orders wo
    INNER JOIN services s ON s.id = wo.service_id
    INNER JOIN jobs j     ON j.id = wo.job_id
    INNER JOIN tasks t    ON t.work_order_id = wo.id
    WHERE j.deleted_at IS NULL
      AND wo.deleted_at IS NULL
      AND j.project_stage_id <> 10
    GROUP BY j.id, wo.id, wo.service_id
),

ccj_crack AS (
    SELECT 
        wosj.work_order_id,
        wosj.service_id,
        SUM(CASE WHEN area_details.area_type = 'crack' THEN ISNULL(TRY_CAST(area_details.area_length AS FLOAT), 0) ELSE 0 END) AS RepArea,
        SUM(CASE WHEN area_details.area_type = 'ccj'   THEN ISNULL(TRY_CAST(area_details.area_length AS FLOAT), 0) ELSE 0 END) AS CCJ
    FROM work_order_services_json wosj
    INNER JOIN services s   ON s.id = wosj.service_id
    INNER JOIN work_orders wo ON wo.id = wosj.work_order_id
    INNER JOIN jobs j        ON j.id = wo.job_id
    CROSS APPLY OPENJSON(wosj.[fields], '$.areaDetails') jAreas
    CROSS APPLY OPENJSON(jAreas.value)
    WITH (
        area_type   VARCHAR(255) '$.area_type',
        area_length VARCHAR(255) '$.area_length'
    ) area_details
    WHERE
        j.deleted_at IS NULL
        AND wo.deleted_at IS NULL
        AND j.project_stage_id <> 10
        AND wosj.deleted_at IS NULL
    GROUP BY wosj.work_order_id, wosj.service_id
),

EquipCount AS (
    SELECT
        wo.id AS work_order_id,
        COUNT(DISTINCT tm.fleet_id) AS fleet_count
    FROM work_orders wo
    INNER JOIN services s ON s.id = wo.service_id
    INNER JOIN tasks t ON t.work_order_id = wo.id
    INNER JOIN task_material tm ON tm.task_id = t.id
    WHERE
        wo.deleted_at IS NULL
        AND t.task_status_id <> 8
        AND tm.deleted_at IS NULL
        AND tm.actual IS NOT NULL
    GROUP BY wo.id
),

SubcontractorSums AS (
    SELECT
        wos.work_order_id AS work_order_id,
        SUM(ISNULL(TRY_CAST(wos.bid AS FLOAT), 0)) AS sub_cost,
        SUM(ISNULL(TRY_CAST(wos.revenue AS FLOAT), 0)) AS sub_revenue
    FROM work_order_subcontractors wos
    WHERE wos.deleted_at IS NULL
    GROUP BY wos.work_order_id
),

WorkOrderBase AS (
    SELECT
        jobs.id                                   AS job_id,
        work_orders.id                           AS service_id,  -- This is the work order ID
        services.service_categories_id           AS service_category_id,
        services.abbreviation                    AS service_name,
        work_order_statuses.name                 AS ServiceStatus,
        work_orders.work_order_status_id         AS ServiceStatusId,
        
        CASE 
            -- Use subcontractor TCD when the service category is 4
            WHEN services.service_categories_id = 4 THEN
                ISNULL(wos.tcd_qtr, 'TBD')
                + ' - ' +
                ISNULL(wos.tcd_year, 'TBD')
            ELSE
                ISNULL(
                    JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.completion_season'),
                    'TBD'
                )
                + ' - ' +
                ISNULL(
                    JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.completion_year'),
                    'TBD'
                )
        END AS TCD,
        
        -- RepArea calculation
        (
            SELECT SUM(TRY_CAST(JSON_VALUE([value], '$.area_sq_ft_total') AS FLOAT))
            FROM OPENJSON(work_order_services_json.[fields], '$.areaDetails')
        ) AS RepArea_ST,
        
        -- Crack/CCJ for repair services
        cc.RepArea                               AS Sales_Crack_LnFt,
        CASE 
            WHEN services.id = 7 THEN ISNULL(cc.CCJ, 0)
            ELSE NULL
        END                                      AS Sales_CCJ_LnFt,
        
        tms.Actual_Gal                           AS TtlActQty,
        em.Est_Material                          AS Est_Material,
        am.Act_Material                          AS Act_Material,
        lh.total_labor_hours                     AS Total_Labor_Hours,
        ec.fleet_count                           AS Fleet_Count,
        ct.task_count                            AS task_count,
        sc.sub_cost                  AS sub_cost,
        sc.sub_revenue               AS sub_revenue,
        lwd.Last_WO_Date,
        nwd.Next_WO_Date
    FROM jobs
    INNER JOIN work_orders 
        ON work_orders.job_id = jobs.id
       AND work_orders.deleted_at IS NULL
    INNER JOIN services 
        ON services.id = work_orders.service_id
    LEFT JOIN work_order_services_json 
        ON work_order_services_json.work_order_id = work_orders.id
       AND work_order_services_json.deleted_at IS NULL
    LEFT JOIN TaskLocationSums tls 
        ON tls.work_order_id = work_orders.id 
       AND tls.job_id = jobs.id
    LEFT JOIN TaskMaterialSums tms 
        ON tms.work_order_id = work_orders.id 
       AND tms.job_id = jobs.id
    LEFT JOIN LaborHours lh 
        ON lh.work_order_id = work_orders.id 
       AND lh.job_id = jobs.id
    LEFT JOIN CountTasks ct
        ON ct.job_id = jobs.id
       AND ct.work_order_id = work_orders.id
    LEFT JOIN Est_Material_Calculations em
        ON em.work_order_id = work_orders.id
       AND em.service_id = work_orders.service_id
    LEFT JOIN Act_Material_Calculations am
        ON am.work_order_id = work_orders.id
    LEFT JOIN work_order_statuses 
        ON work_order_statuses.id = work_orders.work_order_status_id
    LEFT JOIN EquipCount ec
        ON ec.work_order_id = work_orders.id
    LEFT JOIN SubcontractorSums sc
        ON sc.work_order_id = work_orders.id
    LEFT JOIN Next_WO_Date_Calculations nwd 
        ON nwd.work_order_id = work_orders.id
    LEFT JOIN Last_WO_Date_Calculations lwd 
        ON lwd.work_order_id = work_orders.id
    LEFT JOIN ccj_crack cc
        ON cc.work_order_id = work_orders.id
       AND cc.service_id = work_orders.service_id
    LEFT JOIN work_order_subcontractors wos
        ON wos.work_order_id = work_orders.id
    WHERE 
        jobs.deleted_at IS NULL
      AND jobs.project_stage_id <> 10
      AND jobs.estimator_id = 648
)

-- Final SELECT - One row per work order
SELECT
    -- Project info
    pi.Region,
    pi.SubRegion,
    pi.Yard,
    pi.ProjectID,
    pi.ProjectName,
    pi.[Project Status],
    pi.SalesRep,
    pi.PM,
    
    -- Work order info
    wob.service_id                              AS [Service ID],
    wob.service_category_id                     AS [Service Category ID],
    wob.service_name                            AS [Service Name],
    wob.ServiceStatus                           AS [Service Status],
    wob.TCD                                     AS [TCD],
    
    -- Dates
    CASE 
        WHEN wob.Next_WO_Date IS NULL
         AND ISNULL(wob.ServiceStatusId, -1) IN (3,4,7)
         AND wob.Last_WO_Date IS NOT NULL
         AND wob.Last_WO_Date <> 'None'
        THEN RIGHT(wob.Last_WO_Date, 4)  -- Extract year from MM-dd-yyyy format
        ELSE ''
    END                                         AS [YR CMPLT],
    
    -- Area/Material quantities
    wob.RepArea_ST                              AS [RepArea - ST],
    wob.Sales_Crack_LnFt                        AS [Sales Crack LnFt],
    wob.Sales_CCJ_LnFt                          AS [Sales CCJ LnFt],
    wob.TtlActQty                               AS [ActQty],
    wob.Est_Material                            AS [Est Material],
    wob.Act_Material                            AS [Act Material],
    
    -- Completion percentage
    CAST(
        ROUND(
            COALESCE(
                CASE 
                    WHEN wob.Est_Material > 0 THEN 
                        (wob.Act_Material * 1.0 / wob.Est_Material) * 100
                    ELSE NULL
                END, 
                0
            ),
            0
        )
    AS NVARCHAR(20)) + '%'                      AS [%CMPT],
    
    -- Task and labor info
    wob.task_count                              AS [# of Tasks],
    wob.Total_Labor_Hours                       AS [Ttl Labor Hrs],
    wob.Fleet_Count                             AS [# of Equip],
    wob.sub_revenue                             AS [sub-revenue],
    wob.sub_cost                                AS [sub-cost],
    
    -- Dates
    COALESCE(wob.Last_WO_Date, 'None')         AS [LastWO],
    COALESCE(wob.Next_WO_Date, ' ')            AS [NextWO]
    
FROM WorkOrderBase wob
INNER JOIN ProjectInfo pi
    ON pi.ProjectID = wob.job_id
ORDER BY 
    pi.ProjectID,
    wob.service_category_id,
    wob.service_id;
