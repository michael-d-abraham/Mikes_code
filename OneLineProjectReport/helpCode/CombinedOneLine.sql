-- Option A: filter by category (1 = ST, 2/3 = Repair, 4 = Sub, 5 = Support)
DECLARE @ServiceCategoryFilter INT = NULL;  -- e.g. 1, 2, 3, 4, 5, or NULL for no category filter

-- Option B: filter by specific service_id (work_orders.service_id)
DECLARE @ServiceIdFilter INT = NULL;        -- e.g. 7, 9, 26, etc., or NULL for no service-id filter

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
),

/* ---------- Shared Date / Labor CTEs ---------- */

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

/* ---------- SURFACE TREATMENT (service_categories_id = 1) ---------- */

ST_Est_Material_Calculations AS (
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

ST_Act_Material_Calculations AS (
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

-- FIXED: ST_EquipCount – Surface Treatment only, distinct fleet per job
ST_EquipCount AS (
    SELECT
        job_id,
        SUM(distinct_fleet_count) AS fleet_count
    FROM (
        SELECT
            j.id AS job_id,
            t.id AS task_id,
            COUNT(DISTINCT tm.fleet_id) AS distinct_fleet_count
        FROM jobs j
        INNER JOIN work_orders wo
            ON wo.job_id = j.id
           AND wo.deleted_at IS NULL
        INNER JOIN services s
            ON s.id = wo.service_id
           AND s.service_categories_id = 1          -- Surface Treatment only
        INNER JOIN tasks t
            ON t.work_order_id = wo.id
           AND t.task_status_id <> 8                -- exclude cancelled / skipped
        INNER JOIN task_material tm
            ON tm.task_id = t.id
           AND tm.deleted_at IS NULL
           AND tm.actual IS NOT NULL                -- only “real” material rows
        WHERE
            j.deleted_at IS NULL
            AND j.project_stage_id <> 10
        GROUP BY
            j.id,
            t.id
    ) tf
    GROUP BY job_id
),

ST_TaskLocationSums AS (
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

ST_TaskMaterialSums AS (
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

ST_CountWOs AS (
    SELECT
        j.id AS job_id,
        COUNT(wo.id) AS wo_count
    FROM work_orders wo
    INNER JOIN services s ON s.id = wo.service_id
    INNER JOIN jobs j     ON j.id = wo.job_id
    WHERE j.deleted_at IS NULL
      AND wo.deleted_at IS NULL
      AND j.project_stage_id <> 10
      AND s.service_categories_id = 1
    GROUP BY j.id
),

ST_CountTasks AS (
    SELECT
        j.id AS job_id,
        wo.service_id,
        COUNT(DISTINCT CASE WHEN t.task_status_id <> 8 THEN t.id END) AS task_count
    FROM work_orders wo
    INNER JOIN services s ON s.id = wo.service_id
    INNER JOIN jobs j     ON j.id = wo.job_id
    INNER JOIN tasks t    ON t.work_order_id = wo.id
    WHERE j.deleted_at IS NULL
      AND wo.deleted_at IS NULL
      AND j.project_stage_id <> 10
      AND s.service_categories_id = 1
    GROUP BY j.id, wo.service_id
),

ST_Base AS (
    SELECT
        jobs.id                                   AS job_id,
        COALESCE(ST_CountWOs.wo_count, 0)        AS wo_count,
        COALESCE(ST_CountTasks.task_count, 0)    AS task_count,
        work_orders.service_id                   AS service_id,
        services.abbreviation                    AS service_name,
        work_order_statuses.name                 AS ServiceStatus,
        work_orders.work_order_status_id         AS ServiceStatusId,
        (
            ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.completion_season'), 'TBD') 
            + ' - ' + 
            ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.completion_year'), 'TBD')
        ) AS TCD,
        work_orders.id                           AS service_row_id,  -- internal row id if needed

        (
            SELECT SUM(TRY_CAST(JSON_VALUE([value], '$.area_sq_ft_total') AS FLOAT))
            FROM OPENJSON(work_order_services_json.[fields], '$.areaDetails')
        ) AS RepArea_ST,

        st_tms.Actual_Gal           AS TtlActQty,
        st_em.Est_Material          AS Est_Material,
        st_am.Act_Material          AS Act_Material,
        lh.total_labor_hours        AS Total_Labor_Hours,
        st_ec.fleet_count           AS Fleet_Count,
        lwd.Last_WO_Date,
        nwd.Next_WO_Date,
        ROW_NUMBER() OVER (
            PARTITION BY jobs.id
            ORDER BY services.order_by_category, services.order_by_service, work_orders.id
        ) AS rn
    FROM jobs
    LEFT JOIN work_orders 
        ON work_orders.job_id = jobs.id
    LEFT JOIN services 
        ON services.id = work_orders.service_id
    LEFT JOIN work_order_services_json 
        ON work_order_services_json.work_order_id = work_orders.id

    LEFT JOIN ST_TaskLocationSums st_tls 
        ON st_tls.work_order_id = work_orders.id 
       AND st_tls.job_id       = jobs.id

    LEFT JOIN ST_TaskMaterialSums st_tms 
        ON st_tms.work_order_id = work_orders.id 
       AND st_tms.job_id       = jobs.id

    LEFT JOIN LaborHours lh 
        ON lh.work_order_id = work_orders.id 
       AND lh.job_id       = jobs.id

    LEFT JOIN ST_CountWOs 
        ON ST_CountWOs.job_id = jobs.id

    LEFT JOIN ST_CountTasks 
        ON ST_CountTasks.job_id     = jobs.id
       AND ST_CountTasks.service_id = work_orders.service_id

    LEFT JOIN ST_Est_Material_Calculations st_em
        ON st_em.work_order_id = work_orders.id
       AND st_em.service_id    = work_orders.service_id

    LEFT JOIN ST_Act_Material_Calculations st_am
        ON st_am.work_order_id = work_orders.id

    LEFT JOIN work_order_statuses 
        ON work_order_statuses.id = work_orders.work_order_status_id

    LEFT JOIN ST_EquipCount st_ec
        ON st_ec.job_id = jobs.id

    LEFT JOIN Next_WO_Date_Calculations nwd 
        ON nwd.work_order_id = work_orders.id

    LEFT JOIN Last_WO_Date_Calculations lwd 
        ON lwd.work_order_id = work_orders.id

    WHERE jobs.deleted_at IS NULL
      AND work_orders.deleted_at IS NULL
      AND jobs.project_stage_id <> 10
      AND services.service_categories_id = 1
),

-- NEW: list of ST service IDs per job
ST_ServiceList AS (
    SELECT
        job_id,
        STRING_AGG(CAST(service_row_id AS VARCHAR(20)), ', ') AS ServiceIDs
    FROM (
        SELECT DISTINCT
            job_id,
            service_row_id
        FROM ST_Base
        WHERE service_row_id IS NOT NULL
    ) ds
    GROUP BY job_id
),

ST_ServiceNames AS (
    SELECT
        job_id,
        STRING_AGG(service_name, ', ') WITHIN GROUP (ORDER BY first_rn) AS ServiceNames
    FROM (
        SELECT
            job_id,
            service_name,
            MIN(rn) AS first_rn
        FROM ST_Base
        WHERE service_name IS NOT NULL
        GROUP BY job_id, service_name
    ) names
    GROUP BY job_id
),

SurfaceAgg AS (
    SELECT
        b.job_id,

        MAX(b.wo_count)                               AS [# in Category],
        sn.ServiceNames                               AS Service,
        MAX(CASE WHEN b.rn = 1 THEN b.ServiceStatus END) AS ServiceStatus,
        MAX(CASE WHEN b.rn = 1 THEN b.TCD           END) AS TCD,
        CASE 
            WHEN SUM(CASE WHEN b.Next_WO_Date IS NOT NULL THEN 1 ELSE 0 END) = 0
             AND SUM(CASE WHEN ISNULL(b.ServiceStatusId, -1) NOT IN (3,4,7) THEN 1 ELSE 0 END) = 0
             AND MAX(b.Last_WO_Date) IS NOT NULL
            THEN CONVERT(VARCHAR(4), DATEPART(YEAR, MAX(b.Last_WO_Date)))
            ELSE ''
        END                                            AS [YR CMPLT],
        sl.ServiceIDs                                AS [Service ID],

        ''                                           AS [NS Revenue],

        SUM(b.RepArea_ST)                            AS [RepArea - ST],
        SUM(b.TtlActQty)                             AS ActQty,

        CAST(
            ROUND(
                COALESCE(
                    CASE 
                        WHEN SUM(b.Est_Material) > 0 THEN 
                            (SUM(b.Act_Material) * 1.0 / SUM(b.Est_Material)) * 100
                        ELSE NULL
                    END, 
                    0
                ),
                0
            )
        AS NVARCHAR(20)) + '%'                       AS [%CMPT],

        ''                                           AS [Total Mat Cost],

        SUM(b.task_count)                            AS [# of WO's],
        SUM(b.Total_Labor_Hours)                     AS [Ttl Labor Hrs],
        ''                                           AS [Ttl Labor Cost],

        MAX(b.Fleet_Count)                           AS [# of Equip],
        ''                                           AS [Ttl Equip Cost],

        COALESCE(
            MAX(CASE WHEN b.rn = 1 THEN b.Last_WO_Date END),
            'None'
        )                                            AS LastWO,

        COALESCE(
            MAX(CASE WHEN b.rn = 1 THEN b.Next_WO_Date END),
            ' '
        )                                            AS NextWO

    FROM ST_Base b
    LEFT JOIN ST_ServiceList sl
        ON sl.job_id = b.job_id
    LEFT JOIN ST_ServiceNames sn
        ON sn.job_id = b.job_id
    GROUP BY b.job_id, sl.ServiceIDs, sn.ServiceNames
),

/* ---------- SUPPORT SERVICES (service_categories_id = 5) ---------- */

SS_CountWOs AS (
    SELECT
        j.id AS job_id,
        COUNT(DISTINCT CASE WHEN t.task_status_id <> 8 THEN wo.id END) AS wo_count
    FROM work_orders wo
    INNER JOIN services s ON s.id = wo.service_id
    INNER JOIN jobs j     ON j.id = wo.job_id
    INNER JOIN tasks t    ON t.work_order_id = wo.id
    WHERE
        j.deleted_at IS NULL
        AND wo.deleted_at IS NULL
        AND j.project_stage_id <> 10
        AND t.task_status_id <> 8
        AND s.service_categories_id = 5  
    GROUP BY j.id
),

SS_CountTasks AS (
    SELECT
        j.id AS job_id,
        wo.service_id,
        COUNT(DISTINCT CASE WHEN t.task_status_id <> 8 THEN t.id END) AS task_count
    FROM work_orders wo
    INNER JOIN services s ON s.id = wo.service_id
    INNER JOIN jobs j     ON j.id = wo.job_id
    INNER JOIN tasks t    ON t.work_order_id = wo.id
    WHERE
        j.deleted_at IS NULL
        AND wo.deleted_at IS NULL
        AND j.project_stage_id <> 10
        AND t.task_status_id <> 8
        AND s.service_categories_id = 5  
    GROUP BY j.id, wo.service_id
),

SS_Base AS (
    SELECT
        jobs.id               AS job_id,
        wo.service_id         AS service_id,
        s.abbreviation        AS service_name,
        wo.work_order_status_id AS work_order_status_id,
        sswo.wo_count         AS wo_count,
        sst.task_count        AS task_count,
        lh.total_labor_hours  AS Total_Labor_Hrs,
        lwd.Last_WO_Date      AS Last_WO_Date,
        nwd.Next_WO_Date      AS Next_WO_Date,
        ROW_NUMBER() OVER (
            PARTITION BY jobs.id
            ORDER BY s.order_by_category, s.order_by_service, wo.id
        ) AS rn
    FROM jobs
    LEFT JOIN work_orders wo
        ON wo.job_id = jobs.id
    LEFT JOIN services s
        ON s.id = wo.service_id
    LEFT JOIN LaborHours lh
        ON lh.job_id        = jobs.id
       AND lh.work_order_id = wo.id
    LEFT JOIN SS_CountWOs sswo
        ON sswo.job_id = jobs.id
    LEFT JOIN SS_CountTasks sst
        ON sst.job_id     = jobs.id
       AND sst.service_id = wo.service_id
    LEFT JOIN Last_WO_Date_Calculations lwd
        ON lwd.work_order_id = wo.id
    LEFT JOIN Next_WO_Date_Calculations nwd
        ON nwd.work_order_id = wo.id
    WHERE
        jobs.deleted_at IS NULL
        AND wo.deleted_at  IS NULL
        AND s.service_categories_id = 5
),

SS_ServiceNames AS (
    SELECT
        job_id,
        STRING_AGG(service_name, ', ') WITHIN GROUP (ORDER BY first_rn) AS ServiceNames
    FROM (
        SELECT
            job_id,
            service_name,
            MIN(rn) AS first_rn
        FROM SS_Base
        WHERE service_name IS NOT NULL
        GROUP BY job_id, service_name
    ) names
    GROUP BY job_id
),

SS_Agg AS (
    SELECT
        b.job_id,
        MAX(b.wo_count)             AS wo_count,
        SUM(b.task_count)           AS total_task_count,
        SUM(b.Total_Labor_Hrs)      AS total_labor_hrs,
        sn.ServiceNames             AS ServiceNames,
        CASE 
            WHEN SUM(CASE WHEN b.Next_WO_Date IS NOT NULL THEN 1 ELSE 0 END) = 0
             AND SUM(CASE WHEN ISNULL(b.work_order_status_id, -1) NOT IN (3,4,7) THEN 1 ELSE 0 END) = 0
             AND MAX(b.Last_WO_Date) IS NOT NULL
            THEN CONVERT(VARCHAR(4), DATEPART(YEAR, MAX(b.Last_WO_Date)))
            ELSE ''
        END                         AS [YR CMPLT],
        COALESCE(MAX(b.Last_WO_Date), 'None') AS LastWO,
        COALESCE(MAX(b.Next_WO_Date), ' ')    AS NextWO
    FROM SS_Base b
    LEFT JOIN SS_ServiceNames sn
        ON sn.job_id = b.job_id
    GROUP BY b.job_id, sn.ServiceNames
),

/* ---------- CAT 2 & 3 (service_categories_id IN (2,3)) ---------- */

C23_Est_Material_Calculations AS (
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
    INNER JOIN work_orders wo ON wo.id = wosj.work_order_id
    INNER JOIN jobs j         ON j.id = wo.job_id
    INNER JOIN services s     ON s.id = wosj.service_id
    CROSS APPLY OPENJSON(wosj.[fields], '$.areaDetails') AS jAreas
    CROSS APPLY OPENJSON(jAreas.value) WITH (
        area_sq_ft_total   VARCHAR(255) '$.area_sq_ft_total',
        area_app_rate      VARCHAR(255) '$.area_app_rate',
        area_new_app_rate  VARCHAR(255) '$.area_new_app_rate',
        area_est_box_count VARCHAR(255) '$.area_est_box_count'
    ) AS area_details
    WHERE
        wosj.deleted_at IS NULL
        AND j.deleted_at IS NULL
        AND wo.deleted_at IS NULL
        AND j.project_stage_id <> 10
        AND s.service_categories_id IN (2,3)
        AND wosj.service_id IN (9, 7, 26, 30, 17, 10, 12, 27, 28)
    GROUP BY
        wosj.work_order_id,
        wosj.service_id
),

C23_TaskMaterialFiltered AS (
    SELECT
        j.id             AS job_id,
        wo.id            AS work_order_id,
        t.id             AS task_id,
        s.id             As service_id,
        s.material_id    AS service_material_id,
        tm.material_id   AS material_id,
        tm.fleet_id,
        tm.actual,
        t.material_complete
    FROM task_material tm
    INNER JOIN tasks        t  ON t.id  = tm.task_id
    INNER JOIN work_orders  wo ON wo.id = t.work_order_id
    INNER JOIN services     s  ON s.id  = wo.service_id
    INNER JOIN jobs         j  ON j.id  = wo.job_id
    WHERE
        tm.deleted_at IS NULL
        AND tm.actual    IS NOT NULL
        AND j.deleted_at IS NULL
        AND wo.deleted_at IS NULL
        AND j.project_stage_id <> 10
        AND s.service_categories_id IN (2,3)
        AND t.task_status_id <> 8
),

C23_Act_Material_Calculations AS (
    SELECT
        work_order_id,
        ROUND(SUM(actual), 0) AS Act_Material
    FROM C23_TaskMaterialFiltered
    WHERE
        material_complete IS NOT NULL
        AND material_id = service_material_id
    GROUP BY work_order_id
),

C23_TaskMaterialSums AS (
    SELECT
        job_id,
        work_order_id,
        SUM(ISNULL(actual, 0)) AS Actual_Gal
    FROM C23_TaskMaterialFiltered
    GROUP BY job_id, work_order_id
),

-- FIXED: C23_EquipCount – distinct fleet per job for cat 2 & 3
C23_EquipCount AS (
    SELECT
        job_id,
        SUM(distinct_fleet_count) AS fleet_count
    FROM (
        SELECT
            job_id,
            task_id,
            COUNT(DISTINCT fleet_id) AS distinct_fleet_count
        FROM C23_TaskMaterialFiltered
        GROUP BY job_id, task_id
    ) tf
    GROUP BY job_id
),

C23_LaborHours AS (
    SELECT
        jobs.id         AS job_id,
        work_orders.id  AS work_order_id,
        SUM(CAST(ROUND(
            CASE
                WHEN timeclock.[out] IS NOT NULL AND timeclock.[in] IS NOT NULL
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
        AND services.service_categories_id IN (2,3)
        AND tasks.task_status_id <> 8
    GROUP BY jobs.id, work_orders.id
),

C23_CountWOs AS (
    SELECT
        j.id AS job_id,
        COUNT(wo.id) AS wo_count
    FROM work_orders wo
    INNER JOIN services s ON s.id = wo.service_id
    INNER JOIN jobs j     ON j.id = wo.job_id
    WHERE
        j.deleted_at IS NULL
        AND wo.deleted_at IS NULL
        AND j.project_stage_id <> 10
        AND s.service_categories_id IN (2,3)
    GROUP BY j.id
),

C23_CountTasks AS (
    SELECT
        j.id AS job_id,
        wo.service_id,
        COUNT(DISTINCT CASE WHEN t.task_status_id <> 8 THEN t.id END) AS task_count
    FROM work_orders wo
    INNER JOIN services s ON s.id = wo.service_id
    INNER JOIN jobs j     ON j.id = wo.job_id
    INNER JOIN tasks t    ON t.work_order_id = wo.id
    WHERE j.deleted_at IS NULL
      AND wo.deleted_at IS NULL
      AND j.project_stage_id <> 10
      AND t.task_status_id <> 8
      AND s.service_categories_id IN (2,3)
    GROUP BY j.id, wo.service_id
),

C23_ccj_crack AS (
    SELECT 
        j.id AS job_id,
        wosj.work_order_id,
        wosj.service_id,
        SUM(CASE WHEN area_details.area_type = 'crack' THEN ISNULL(TRY_CAST(area_details.area_length AS FLOAT), 0) ELSE 0 END) AS RepArea,
        SUM(CASE WHEN area_details.area_type = 'ccj'   THEN ISNULL(TRY_CAST(area_details.area_length AS FLOAT), 0) ELSE 0 END) AS CCJ
    FROM work_order_services_json wosj
    INNER JOIN services s ON s.id = wosj.service_id
    INNER JOIN work_orders wo ON wo.id = wosj.work_order_id
    INNER JOIN jobs j        ON j.id = wo.job_id
    CROSS APPLY OPENJSON(wosj.[fields], '$.areaDetails') jAreas
    CROSS APPLY OPENJSON(jAreas.value)
    WITH (
        area_type   VARCHAR(255) '$.area_type',
        area_length VARCHAR(255) '$.area_length'
    ) area_details
    WHERE
        wosj.deleted_at IS NULL
        AND j.deleted_at IS NULL
        AND wo.deleted_at IS NULL
        AND j.project_stage_id <> 10
        AND s.service_categories_id IN (2,3)
    GROUP BY j.id, wosj.work_order_id, wosj.service_id
),

C23_RepAreaTotals AS (
    SELECT
        job_id,
        SUM(RepArea) AS total_rep_area
    FROM C23_ccj_crack
    GROUP BY job_id
),

C23_Base AS (
    SELECT
        jobs.id                                   AS job_id,
        COALESCE(C23_CountWOs.wo_count, 0)       AS wo_count,
        COALESCE(C23_CountTasks.task_count, 0)   AS task_count,
        work_orders.service_id                   AS service_id,
        services.abbreviation                    AS service_name,
        work_order_statuses.name                 AS ServiceStatus,
        work_orders.work_order_status_id         AS ServiceStatusId,
        (
            ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.completion_season'), 'TBD') 
            + ' - ' + 
            ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.completion_year'), 'TBD')
        ) AS TCD,
        work_orders.id                           AS service_row_id,

        c23ccj.RepArea                           AS Sales_Crack_LnFt,

        CASE 
            WHEN services.id = 7 THEN ISNULL(c23ccj.CCJ, 0)
            ELSE NULL
        END                                      AS Sales_CCJ_LnFt,

        c23_tms.Actual_Gal                       AS TtlActQty,
        c23_em.Est_Material                      AS Est_Material,
        c23_am.Act_Material                      AS Act_Material,
        c23_lh.total_labor_hours                 AS Total_Labor_Hours,
        c23_ec.fleet_count                       AS Fleet_Count,
        lwd.Last_WO_Date,
        nwd.Next_WO_Date,
        ROW_NUMBER() OVER (
            PARTITION BY jobs.id
            ORDER BY services.order_by_category, services.order_by_service, work_orders.id
        ) AS rn
    FROM jobs
    LEFT JOIN work_orders 
        ON work_orders.job_id = jobs.id
    LEFT JOIN services 
        ON services.id = work_orders.service_id
    LEFT JOIN work_order_services_json 
        ON work_order_services_json.work_order_id = work_orders.id

    LEFT JOIN C23_TaskMaterialSums c23_tms 
        ON c23_tms.work_order_id = work_orders.id 
       AND c23_tms.job_id       = jobs.id

    LEFT JOIN C23_LaborHours c23_lh
        ON c23_lh.work_order_id = work_orders.id
       AND c23_lh.job_id        = jobs.id

    LEFT JOIN C23_CountWOs 
        ON C23_CountWOs.job_id = jobs.id

    LEFT JOIN C23_CountTasks 
        ON C23_CountTasks.job_id     = jobs.id
       AND C23_CountTasks.service_id = work_orders.service_id

    LEFT JOIN C23_Est_Material_Calculations c23_em
        ON c23_em.work_order_id = work_orders.id
       AND c23_em.service_id    = work_orders.service_id

    LEFT JOIN C23_Act_Material_Calculations c23_am
        ON c23_am.work_order_id = work_orders.id

    LEFT JOIN work_order_statuses 
        ON work_order_statuses.id = work_orders.work_order_status_id

    LEFT JOIN C23_EquipCount c23_ec
        ON c23_ec.job_id = jobs.id

    LEFT JOIN Next_WO_Date_Calculations nwd 
        ON nwd.work_order_id = work_orders.id

    LEFT JOIN Last_WO_Date_Calculations lwd 
        ON lwd.work_order_id = work_orders.id

    LEFT JOIN C23_ccj_crack c23ccj
        ON c23ccj.work_order_id = work_orders.id
       AND c23ccj.service_id    = work_orders.service_id

    WHERE
        jobs.deleted_at IS NULL
      AND work_orders.deleted_at IS NULL
      AND services.service_categories_id IN (2,3)
      AND jobs.project_stage_id <> 10
),

-- NEW: list of Repair (2&3) service IDs per job
C23_ServiceList AS (
    SELECT
        job_id,
        STRING_AGG(CAST(service_row_id AS VARCHAR(20)), ', ') AS ServiceIDs
    FROM (
        SELECT DISTINCT
            job_id,
            service_row_id
        FROM C23_Base
        WHERE service_row_id IS NOT NULL
    ) ds
    GROUP BY job_id
),

C23_ServiceNames AS (
    SELECT
        job_id,
        STRING_AGG(service_name, ', ') WITHIN GROUP (ORDER BY first_rn) AS ServiceNames
    FROM (
        SELECT
            job_id,
            service_name,
            MIN(rn) AS first_rn
        FROM C23_Base
        WHERE service_name IS NOT NULL
        GROUP BY job_id, service_name
    ) names
    GROUP BY job_id
),

C23_Agg AS (
    SELECT
        b.job_id,

        MAX(b.wo_count)                            AS [# in Category],
        sn.ServiceNames                            AS [Service],
        sl.ServiceIDs                              AS [Service ID],

        MAX(CASE WHEN b.rn = 1 THEN b.TCD END)      AS TCD,
        ''                                         AS [NS Revenue],
        CASE 
            WHEN SUM(CASE WHEN b.Next_WO_Date IS NOT NULL THEN 1 ELSE 0 END) = 0
             AND SUM(CASE WHEN ISNULL(b.ServiceStatusId, -1) NOT IN (3,4,7) THEN 1 ELSE 0 END) = 0
             AND MAX(b.Last_WO_Date) IS NOT NULL
            THEN CONVERT(VARCHAR(4), DATEPART(YEAR, MAX(b.Last_WO_Date)))
            ELSE ''
        END                                         AS [YR CMPLT],

        COALESCE(
            MAX(rt.total_rep_area),
            SUM(b.Sales_Crack_LnFt),
            0
        )                                          AS [RepArea - ST],
        SUM(b.Sales_CCJ_LnFt)                      AS [CCJ],
        SUM(b.TtlActQty)                           AS [ActQty],

        CAST(
            ROUND(
                COALESCE(
                    CASE 
                        WHEN SUM(b.Est_Material) > 0 THEN 
                            (SUM(b.Act_Material) * 1.0 / SUM(b.Est_Material)) * 100
                        ELSE NULL
                    END, 
                    0
                ),
                0
            )
        AS NVARCHAR(20)) + '%'                     AS [%CMPT],

        ''                                         AS [Total Mat Cost],

        SUM(b.task_count)                          AS [# of WO's],

        SUM(b.Total_Labor_Hours)                   AS [Ttl Labor Hrs],
        ''                                         AS [Ttl Labor Cost],

        MAX(b.Fleet_Count)                         AS [# of Equip],
        ''                                         AS [Ttl Equip Cost],

        COALESCE(
            MAX(CASE WHEN b.rn = 1 THEN b.Last_WO_Date END),
            'None'
        )                                          AS LastWO,

        COALESCE(
            MAX(CASE WHEN b.rn = 1 THEN b.Next_WO_Date END),
            ' '
        )                                          AS NextWO

    FROM C23_Base b
    LEFT JOIN C23_ServiceList sl
        ON sl.job_id = b.job_id
    LEFT JOIN C23_ServiceNames sn
        ON sn.job_id = b.job_id
    LEFT JOIN C23_RepAreaTotals rt
        ON rt.job_id = b.job_id
    GROUP BY b.job_id, sl.ServiceIDs, sn.ServiceNames
)

/* ---------- FINAL SELECT ---------- */

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

    -- ST: Surface Treatment (cat 1)
    sa.[# in Category]      AS [ST-# in Category],
    sa.Service              AS [ST-Service],
    sa.ServiceStatus        AS [ST-ServiceStatus],
    sa.TCD                  AS [ST-TCD],
    sa.[YR CMPLT]           AS [ST-YR CMPLT],
    sa.[Service ID]         AS [ST-Service ID],
    sa.[NS Revenue]         AS [ST-NS Revenue],
    sa.[RepArea - ST]       AS [ST-RepArea],
    sa.ActQty               AS [ST-ActQty],
    sa.[%CMPT]              AS [ST-%CMPT],
    sa.[Total Mat Cost]     AS [ST-Total Mat Cost],
    sa.[# of WO's]          AS [ST-# of WO's],
    sa.[Ttl Labor Hrs]      AS [ST-Ttl Labor Hrs],
    sa.[Ttl Labor Cost]     AS [ST-Ttl Labor Cost],
    sa.[# of Equip]         AS [ST-# of Equip],
    sa.[Ttl Equip Cost]     AS [ST-Ttl Equip Cost],
    sa.LastWO               AS [ST-LastWO],
    sa.NextWO               AS [ST-NextWO],

    -- SS: Support Services (cat 5)
    ss.wo_count             AS [SS-# in Category],
    ss.ServiceNames         AS [SS-Service],
    ss.[YR CMPLT]           AS [SS-YR CMPLT],
    ss.total_task_count     AS [SS-# of WO's],
    ss.total_labor_hrs      AS [SS-Ttl Labor Hrs],
    ''                      AS [SS-Ttl Labor Cost],
    ss.LastWO               AS [SS-LastWO],
    ss.NextWO               AS [SS-NextWO],

    -- R: Repair (cat 2 & 3)
    c23.[# in Category]     AS [R-# in Category],
    c23.[Service]           AS [R-Service],
    c23.TCD                 AS [R-TCD],
    c23.[YR CMPLT]          AS [R-YR CMPLT],
    c23.[Service ID]        AS [R-Service ID],
    c23.[NS Revenue]        AS [R-NS Revenue],
    c23.[RepArea - ST]      AS [R-RepArea],
    c23.[CCJ]               AS [R-CCJ],
    c23.[ActQty]            AS [R-ActQty],
    c23.[%CMPT]             AS [R-%CMPT],
    c23.[Total Mat Cost]    AS [R-Total Mat Cost],
    c23.[# of WO's]         AS [R-# of WO's],
    c23.[Ttl Labor Hrs]     AS [R-Ttl Labor Hrs],
    c23.[Ttl Labor Cost]    AS [R-Ttl Labor Cost],
    c23.[# of Equip]        AS [R-# of Equip],
    c23.[Ttl Equip Cost]    AS [R-Ttl Equip Cost],
    c23.LastWO              AS [R-LastWO],
    c23.NextWO              AS [R-NextWO]

FROM ProjectInfo pi
LEFT JOIN SurfaceAgg sa
    ON sa.job_id = pi.ProjectID
LEFT JOIN SS_Agg ss
    ON ss.job_id = pi.ProjectID
LEFT JOIN C23_Agg c23
    ON c23.job_id = pi.ProjectID
WHERE sa.job_id IS NOT NULL 
   OR c23.job_id IS NOT NULL
ORDER BY pi.ProjectID;
