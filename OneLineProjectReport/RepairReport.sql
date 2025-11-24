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
    LEFT JOIN customers               ON customers.id           = jobs.customer_id
    LEFT JOIN customers end_customer  ON end_customer.id        = jobs.end_customer_id
    LEFT JOIN regions                 ON regions.id             = jobs.region_id
    LEFT JOIN job_area                ON job_area.id            = jobs.sub_region_id
    LEFT JOIN locations               ON locations.id           = jobs.location_id
    LEFT JOIN users sales_rep         ON sales_rep.id           = jobs.estimator_id
    LEFT JOIN users sales_support     ON sales_support.id       = jobs.sales_support_id
    LEFT JOIN users PM                ON PM.id                  = jobs.manager_id
    LEFT JOIN project_populations     ON project_populations.id = jobs.project_population_id
    LEFT JOIN job_category            ON job_category.id        = jobs.category
    LEFT JOIN project_stages          ON project_stages.id      = jobs.project_stage_id
    WHERE jobs.deleted_at IS NULL
      AND jobs.project_status_id <> 10
      AND LOWER(jobs.name) NOT LIKE '%warranty%'
      AND LOWER(jobs.name) NOT LIKE '%test%'
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

FilteredWOSJ AS (
    SELECT
        wosj.*
    FROM (
        SELECT
            wosj.*,
            ROW_NUMBER() OVER (
                PARTITION BY wosj.work_order_id
                ORDER BY wosj.updated_at DESC, wosj.id DESC
            ) AS rn
        FROM work_order_services_json wosj
        WHERE wosj.deleted_at IS NULL
    ) wosj
    WHERE wosj.rn = 1
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
    FROM FilteredWOSJ wosj
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
        j.deleted_at IS NULL
        AND wo.deleted_at IS NULL
        AND j.project_stage_id <> 10
        AND s.service_categories_id IN (2,3)
        AND wosj.service_id IN (9, 7, 26, 30, 17, 10, 12, 27, 28)
    GROUP BY
        wosj.work_order_id,
        wosj.service_id
),

Act_Material_Calculations AS (
    SELECT
        wo.id AS work_order_id,
        ROUND(SUM(tm.actual), 0) AS Act_Material
    FROM tasks t
    INNER JOIN work_orders wo ON t.work_order_id = wo.id
    INNER JOIN jobs j         ON j.id = wo.job_id
    INNER JOIN services s     ON s.id = wo.service_id
    LEFT JOIN task_material tm 
        ON tm.task_id     = t.id
       AND tm.deleted_at  IS NOT NULL      -- as in your original
       AND tm.material_id = s.material_id
    WHERE
        t.material_complete IS NOT NULL
        AND j.deleted_at IS NULL
        AND wo.deleted_at IS NULL
        AND j.project_stage_id <> 10
        AND s.service_categories_id IN (2,3)
        AND t.task_status_id <> 8
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

EquipCount AS (
    SELECT
        job_id,
        SUM(distinct_fleet_count) AS fleet_count
    FROM (
        SELECT
            j.id  AS job_id,
            t.id  AS task_id,
            COUNT(DISTINCT tm.fleet_id) AS distinct_fleet_count
        FROM jobs j
        INNER JOIN work_orders wo   ON wo.job_id = j.id
        INNER JOIN services s       ON s.id = wo.service_id
        INNER JOIN tasks t          ON t.work_order_id = wo.id
        INNER JOIN task_material tm ON tm.task_id     = t.id
        WHERE
            j.deleted_at IS NULL
            AND wo.deleted_at IS NULL
            AND j.project_stage_id <> 10
            AND t.task_status_id <> 8
            AND tm.deleted_at IS NULL
            AND tm.actual IS NOT NULL
            AND s.service_categories_id IN (2,3)
        GROUP BY j.id, t.id
    ) tf
    GROUP BY job_id
),

LaborHours AS (
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
        AND services.service_categories_id IN (2,3)
        AND tasks.task_status_id <> 8
    GROUP BY jobs.id, work_orders.id
),

CountWOs AS (
    SELECT
        j.id AS job_id,
        wo.service_id,
        COUNT(wo.id) AS wo_count
    FROM work_orders wo
    INNER JOIN services s ON s.id = wo.service_id
    INNER JOIN jobs j     ON j.id = wo.job_id
    WHERE j.deleted_at IS NULL
      AND wo.deleted_at IS NULL
      AND j.project_stage_id <> 10
      AND s.service_categories_id IN (2,3)
    GROUP BY j.id, wo.service_id
),

CountTasks AS (
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

ccj_crack AS (
    SELECT 
        wosj.work_order_id,
        wosj.service_id,
        SUM(CASE WHEN area_details.area_type = 'crack' THEN ISNULL(TRY_CAST(area_details.area_length AS FLOAT), 0) ELSE 0 END) AS RepArea,
        SUM(CASE WHEN area_details.area_type = 'ccj'   THEN ISNULL(TRY_CAST(area_details.area_length AS FLOAT), 0) ELSE 0 END) AS CCJ
    FROM FilteredWOSJ wosj
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
        AND s.service_categories_id IN (2,3)
    GROUP BY wosj.work_order_id, wosj.service_id
),

Base AS (
    SELECT
        jobs.id                                   AS job_id,
        COALESCE(CountWOs.wo_count, 0)           AS wo_count,
        COALESCE(CountTasks.task_count, 0)       AS task_count,
        work_orders.service_id                   AS service_type_id,
        services.abbreviation                    AS service_name,
        work_order_statuses.name                 AS ServiceStatus,
        work_orders.work_order_status_id         AS ServiceStatusId,
        (
            ISNULL(JSON_VALUE(wosj.[fields], '$.serviceConditions.completion_season'), 'TBD') 
            + ' - ' + 
            ISNULL(JSON_VALUE(wosj.[fields], '$.serviceConditions.completion_year'), 'TBD')
        ) AS TCD,
        work_orders.id                           AS service_id,   -- this is what we'll list

        cc.RepArea                               AS Sales_Crack_LnFt,

        CASE 
            WHEN services.id = 7 THEN ISNULL(cc.CCJ, 0)
            ELSE NULL
        END                                      AS Sales_CCJ_LnFt,

        tms.Actual_Gal           AS TtlActQty,
        em.Est_Material          AS Est_Material,
        am.Act_Material          AS Act_Material,
        tsh.total_labor_hours    AS Total_Labor_Hours,
        EquipCount.fleet_count   AS Fleet_Count,
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
    LEFT JOIN FilteredWOSJ wosj
        ON wosj.work_order_id = work_orders.id

    LEFT JOIN TaskMaterialSums tms 
        ON tms.work_order_id = work_orders.id 
       AND tms.job_id       = jobs.id

    LEFT JOIN LaborHours tsh 
        ON tsh.work_order_id = work_orders.id 
       AND tsh.job_id       = jobs.id

    LEFT JOIN CountWOs 
        ON CountWOs.job_id     = jobs.id
       AND CountWOs.service_id = work_orders.service_id

    LEFT JOIN CountTasks 
        ON CountTasks.job_id     = jobs.id
       AND CountTasks.service_id = work_orders.service_id

    LEFT JOIN Est_Material_Calculations em
        ON em.work_order_id = work_orders.id
       AND em.service_id    = work_orders.service_id

    LEFT JOIN Act_Material_Calculations am
        ON am.work_order_id = work_orders.id

    LEFT JOIN work_order_statuses 
        ON work_order_statuses.id = work_orders.work_order_status_id

    LEFT JOIN EquipCount 
        ON EquipCount.job_id = jobs.id

    LEFT JOIN Next_WO_Date_Calculations nwd 
        ON nwd.work_order_id = work_orders.id

    LEFT JOIN Last_WO_Date_Calculations lwd 
        ON lwd.work_order_id = work_orders.id

    LEFT JOIN ccj_crack cc
        ON cc.work_order_id = work_orders.id
       AND cc.service_id    = work_orders.service_id

    WHERE
        jobs.deleted_at IS NULL
      AND work_orders.deleted_at IS NULL
      AND services.service_categories_id IN (2,3)
      AND jobs.project_stage_id <> 10
),

/* NEW: individual Repair service IDs per job */

Repair_ServiceList AS (
    SELECT
        job_id,
        MAX(CASE WHEN rn = 1 THEN CAST(service_id AS VARCHAR(20)) END) AS ServiceID1,
        MAX(CASE WHEN rn = 2 THEN CAST(service_id AS VARCHAR(20)) END) AS ServiceID2,
        MAX(CASE WHEN rn = 3 THEN CAST(service_id AS VARCHAR(20)) END) AS ServiceID3,
        MAX(CASE WHEN rn = 4 THEN CAST(service_id AS VARCHAR(20)) END) AS ServiceID4
    FROM (
        SELECT DISTINCT
            job_id,
            service_id,
            ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY service_id) AS rn
        FROM Base
        WHERE service_id IS NOT NULL
    ) ds
    GROUP BY job_id
),

Repair_ServiceNames AS (
    SELECT
        job_id,
        STRING_AGG(service_name, ', ') WITHIN GROUP (ORDER BY first_rn) AS ServiceNames
    FROM (
        SELECT
            job_id,
            service_name,
            MIN(rn) AS first_rn
        FROM Base
        WHERE service_name IS NOT NULL
        GROUP BY job_id, service_name
    ) names
    GROUP BY job_id
),

RepairAgg AS (
    SELECT
        b.job_id,

        MAX(b.wo_count)                            AS [R-# in Category],

        rsn.ServiceNames                           AS [R-Service],

        MAX(CASE WHEN b.rn = 1 THEN b.TCD END)     AS [R-TCD],

        CASE 
            WHEN SUM(CASE WHEN b.Next_WO_Date IS NOT NULL THEN 1 ELSE 0 END) = 0
             AND SUM(CASE WHEN ISNULL(b.ServiceStatusId, -1) NOT IN (3,4,7) THEN 1 ELSE 0 END) = 0
             AND MAX(b.Last_WO_Date) IS NOT NULL
            THEN CONVERT(VARCHAR(4), DATEPART(YEAR, MAX(b.Last_WO_Date)))
            ELSE ''
        END                                         AS [R-YR CMPLT],

        rsl.ServiceID1                              AS [R-Service ID],
        ''                                          AS [R-Rev1],
        rsl.ServiceID2                              AS [R-Service ID2],
        ''                                          AS [R-Rev2],
        rsl.ServiceID3                              AS [R-Service ID3],
        ''                                          AS [R-Rev3],
        rsl.ServiceID4                              AS [R-Service ID4],
        ''                                          AS [R-Rev4],

        ''                                         AS [R-NS Revenue], 

        SUM(b.Sales_Crack_LnFt)                    AS [R-RepArea],
        SUM(b.Sales_CCJ_LnFt)                      AS [R-CCJ],
        SUM(b.TtlActQty)                           AS [R-ActQty],

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
        AS NVARCHAR(20)) + '%'                     AS [R-%CMPT],

        ''                                         AS [R-Total Mat Cost],

        SUM(b.task_count)                          AS [R-# of WO's],

        SUM(b.Total_Labor_Hours)                   AS [R-Ttl Labor Hrs],
        ''                                         AS [R-Ttl Labor Cost],

        MAX(b.Fleet_Count)                         AS [R-# of Equip],
        ''                                         AS [R-Ttl Equip Cost],

        COALESCE(
            MAX(CASE WHEN b.rn = 1 THEN b.Last_WO_Date END),
            'None'
        )                                          AS [R-LastWO],

        COALESCE(
            MAX(CASE WHEN b.rn = 1 THEN b.Next_WO_Date END),
            ' '
        )                                          AS [R-NextWO]

    FROM Base b
    LEFT JOIN Repair_ServiceList  rsl ON rsl.job_id = b.job_id
    LEFT JOIN Repair_ServiceNames rsn ON rsn.job_id = b.job_id
    GROUP BY b.job_id, rsl.ServiceID1, rsl.ServiceID2, rsl.ServiceID3, rsl.ServiceID4, rsn.ServiceNames
),

/* --------- MINIMAL ST (Category 1) JUST FOR ST-Service & ST-YR CMPLT --------- */

ST_Base AS (
    SELECT
        j.id                             AS job_id,
        s.abbreviation                   AS service_name,
        wo.work_order_status_id          AS ServiceStatusId,
        (
            ISNULL(JSON_VALUE(wosj.[fields], '$.serviceConditions.completion_season'), 'TBD') 
            + ' - ' + 
            ISNULL(JSON_VALUE(wosj.[fields], '$.serviceConditions.completion_year'), 'TBD')
        ) AS TCD,
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
       AND s.service_categories_id = 1   -- Surface Treatment only
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

ST_Agg AS (
    SELECT
        b.job_id,
        sn.ServiceNames AS [ST-Service],
        CASE 
            WHEN SUM(CASE WHEN b.Next_WO_Date IS NOT NULL THEN 1 ELSE 0 END) = 0
             AND SUM(CASE WHEN ISNULL(b.ServiceStatusId, -1) NOT IN (3,4,7) THEN 1 ELSE 0 END) = 0
             AND MAX(b.Last_WO_Date) IS NOT NULL
            THEN CONVERT(VARCHAR(4), DATEPART(YEAR, MAX(b.Last_WO_Date)))
            ELSE ''
        END AS [ST-YR CMPLT]
    FROM ST_Base b
    LEFT JOIN ST_ServiceNames sn
        ON sn.job_id = b.job_id
    GROUP BY b.job_id, sn.ServiceNames
)

SELECT
    -- Base columns
    pi.Region,
    pi.SubRegion,
    pi.Yard,
    pi.ProjectID,
    pi.ProjectName,
    pi.[Project Status],
    pi.SalesRep,
    pi.PM,

    -- Repair columns (cat 2 & 3)
    ra.[R-# in Category],
    ra.[R-Service],
    ra.[R-TCD],
    ra.[R-YR CMPLT],
    ''                      AS [NS INV YR2],
    ra.[R-Service ID],
    ra.[R-Rev1],
    ra.[R-Service ID2],
    ra.[R-Rev2],
    ra.[R-Service ID3],
    ra.[R-Rev3],
    ra.[R-Service ID4],
    ra.[R-Rev4],
    ra.[R-NS Revenue],
    ra.[R-RepArea],
    ra.[R-CCJ],
    ra.[R-ActQty],
    ra.[R-%CMPT],
    ''                      AS [R-Mat Cost],
    ra.[R-Total Mat Cost],
    ra.[R-# of WO's],
    ra.[R-Ttl Labor Hrs],
    ''                      AS [R-Labor Cost],
    ra.[R-Ttl Labor Cost],
    ra.[R-# of Equip],
    ''                      AS [R-Equip Cost],
    ra.[R-Ttl Equip Cost],
    ra.[R-LastWO],
    ra.[R-NextWO],

    -- Surface Treatment summary at the end
    st.[ST-Service]         AS [S - Services],
    st.[ST-YR CMPLT]        AS [S-Yr-Cmplt],
    ''                      AS [ ],
    ''                      AS [R Revenue],
    ''                      AS [R Cost],
    ''                      AS [R GM],
    ''                      AS [R GM%]

FROM ProjectInfo pi
LEFT JOIN RepairAgg ra
    ON ra.job_id = pi.ProjectID
LEFT JOIN ST_Agg st
    ON st.job_id = pi.ProjectID
WHERE ra.job_id IS NOT NULL          -- only projects that have Repair (cat 2/3)
ORDER BY pi.ProjectID;
