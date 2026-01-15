WITH Est_Area_Calculations AS (
    SELECT
        wo.work_order_id,
        wo.service_id,
        area_details.area_app_rate,
        area_details.area_new_app_rate,
        CASE
            WHEN wo.service_id = 9 THEN ISNULL(ROUND(SUM(ISNULL(TRY_CAST(area_details.area_sq_ft_total AS FLOAT), 0)), 0), 0)
            WHEN wo.service_id = 7 THEN ISNULL(ROUND(SUM(ISNULL(TRY_CAST(area_details.area_length AS FLOAT), 0)), 0), 0)
            WHEN wo.service_id IN (10, 12, 27, 28) THEN ISNULL(ROUND(SUM(ISNULL(TRY_CAST(area_details.area_sq_ft_total AS FLOAT), 0)), 0), 0)
            WHEN wo.service_id IN (26, 30, 17) THEN ISNULL(ROUND(SUM(ISNULL(TRY_CAST(area_details.area_length AS FLOAT), 0)), 0), 0)
            ELSE NULL
        END AS Est_Area
    FROM work_order_services_json wo
    CROSS APPLY OPENJSON(wo.[fields], '$.areaDetails') AS j
    CROSS APPLY OPENJSON(j.value)
    WITH (
        area_length VARCHAR(255) '$.area_length',
        area_sq_ft_total VARCHAR(255) '$.area_sq_ft_total',
        area_app_rate VARCHAR(255) '$.area_app_rate',
        area_new_app_rate VARCHAR(255) '$.area_new_app_rate'
    ) area_details
    WHERE wo.deleted_at IS NULL
    GROUP BY wo.work_order_id, wo.service_id, area_details.area_app_rate, area_details.area_new_app_rate
),

Est_Material_Calculations AS (
    SELECT
        wosj.work_order_id,
        wosj.service_id,
        CASE
            WHEN wosj.service_id = 9 THEN ROUND(SUM(
                CASE
                    WHEN LTRIM(RTRIM(area_details.area_new_app_rate)) != '' AND area_details.area_new_app_rate IS NOT NULL
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
    CROSS APPLY OPENJSON(j.value)
    WITH (
        area_sq_ft_total VARCHAR(255) '$.area_sq_ft_total',
        area_app_rate VARCHAR(255) '$.area_app_rate',
        area_new_app_rate VARCHAR(255) '$.area_new_app_rate',
        area_est_box_count VARCHAR(255) '$.area_est_box_count'
    ) area_details
    WHERE wosj.deleted_at IS NULL
    GROUP BY wosj.work_order_id, wosj.service_id
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

Act_Material_Calculations AS (
    SELECT
        wo.id AS work_order_id,
        ROUND(SUM(tm.actual), 0) AS Act_Material
    FROM tasks t
    LEFT JOIN task_material tm ON tm.task_id = t.id
    LEFT JOIN work_orders wo ON t.work_order_id = wo.id
    WHERE
        t.material_complete IS NOT NULL
        AND tm.material_id = (
            SELECT s.material_id FROM services s WHERE s.id = wo.service_id
        )
        AND tm.deleted_at IS NULL
    GROUP BY wo.id
),

ccj_crack AS (
    SELECT 
        wosj.work_order_id,
        wosj.service_id,
        SUM(CASE WHEN area_details.area_type = 'crack' THEN ISNULL(TRY_CAST(area_details.area_length AS FLOAT), 0) ELSE 0 END) AS RepArea,
        SUM(CASE WHEN area_details.area_type = 'ccj' THEN ISNULL(TRY_CAST(area_details.area_length AS FLOAT), 0) ELSE 0 END) AS CCJ
    FROM work_order_services_json wosj
    CROSS APPLY OPENJSON(wosj.[fields], '$.areaDetails') j
    CROSS APPLY OPENJSON(j.value)
    WITH (
        area_type VARCHAR(255) '$.area_type',
        area_length VARCHAR(255) '$.area_length'
    ) area_details
    WHERE wosj.deleted_at IS NULL
    GROUP BY wosj.work_order_id, wosj.service_id
),

-- If you need a summary of actual sq ft:
task_summary AS (
    SELECT
        t.work_order_id,
        SUM(CASE 
            WHEN tl.pm_sq_ft IS NULL THEN tl.sq_ft 
            ELSE tl.pm_sq_ft 
        END) AS OpsSqFt
    FROM tasks t
    LEFT JOIN task_locations tl ON tl.task_id = t.id
    WHERE tl.deleted_at IS NULL
    GROUP BY t.work_order_id
)

SELECT
    -- Region Metadata
    CASE
        WHEN jobs.region_id = 1 THEN 'HAU'
        WHEN jobs.region_id = 5 THEN 'HAU - TX'
        WHEN jobs.region_id = 4 THEN 'HAU - SE'
        WHEN jobs.region_id = 2 THEN 'HAA'
        ELSE 'Unknown'
    END AS Region,
    (job_area.state + ' - ' + job_area.area) AS SubRegion,
    locations.short_code AS Yard,
    
    -- Job Information
    jobs.id AS ProjectID,
    jobs.name AS ProjectName,
    project_stages.display_name AS Status,
    (users.first_name + ' ' + LEFT(users.last_name, 1) + '.') AS SalesRep,
    (project_manager.first_name + ' ' + LEFT(project_manager.last_name, 1) + '.') AS PM,
    services.abbreviation AS Service,
    work_order_statuses.name AS ServiceStatus,
		
    -- Using JSON_VALUE for completion_season and completion_year directly from work_order_services_json.fields
    (ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.completion_season'), 'TBD') 
     + ' - ' 
     + ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.serviceConditions.completion_year'), 'TBD')) AS TCD,
		ea.area_app_rate AS RegAppRate,
		ea.area_new_app_rate AS 'PMAppRate',
		CASE
        WHEN services.id = 7 THEN ISNULL(cc.RepArea, 0)
        ELSE ISNULL(ea.Est_Area, 0)
    END AS RepArea,
		
		    CASE
        WHEN services.id IN (7, 9, 10, 12, 27, 28) THEN ISNULL(task_summary.OpsSqFt, 0)
        ELSE NULL
    END AS OpsArea,
		
				CASE
				WHEN services.id IN (7, 9, 10, 12, 27, 28) THEN 
						ISNULL(
								CAST(
										ROUND(
												CASE
														WHEN services.id = 7 THEN
																ISNULL(NULLIF(ISNULL(task_summary.OpsSqFt, 0) / NULLIF((cc.RepArea + cc.CCJ), 0), 0), 0)
														ELSE
																ISNULL(NULLIF(ISNULL(task_summary.OpsSqFt, 0) / NULLIF(ea.Est_Area, 0), 0), 0)
												END * 100, 0
										) AS NVARCHAR
								) + '%', '0%'
						)
				ELSE NULL
		END AS AreaVar,
		
		
		CASE
        WHEN services.id = 7 THEN ISNULL(cc.CCJ, 0)
        ELSE NULL
    END AS CCJ,

		
    CASE
        WHEN services.id IN (7, 26, 30) THEN 
            ISNULL(
                ROUND(
                    CASE
                        WHEN services.id = 7 THEN
                            (ISNULL(cc.RepArea, 0) + ISNULL(cc.CCJ, 0)) / NULLIF(ISNULL(em.Est_Material, 0), 0)
                        ELSE
                            ISNULL(ea.Est_Area, 0) / NULLIF(ISNULL(em.Est_Material, 0), 0)
                    END, 0
                ), NULL
            )
        ELSE NULL
    END AS LinearFtPerBox,
		
		task_summary.OpsSqFt AS ActArea,
		CAST(ROUND((ISNULL(task_summary.OpsSqFt, 0) / NULLIF(ISNULL(ea.Est_Area, 0), 0)) * 100, 0) AS NVARCHAR(50)) + '%' AS [PercentAreaCPLT],
		em.Est_Material AS EstQty,
		am.Act_Material AS ActQty,


		CAST(
    ROUND(
        COALESCE(
            CASE 
                WHEN em.Est_Material > 0 THEN 
                    (am.Act_Material * 1.0 / em.Est_Material) * 100
                ELSE NULL
            END, 
            0
        ), 
    0)
AS NVARCHAR) + '%' AS [%QtyCPLT],


    -- Next/Last Work Order Dates
    nwd.Next_WO_Date AS NextWO,
    lwd.Last_WO_Date AS LastWO,
		
		CASE
				WHEN work_orders.service_id IN (2, 3, 4, 5, 6, 11, 13, 18, 21, 45, 53, 54, 66, 14) THEN ISNULL(subcontractors.name, 'Yes')
				ELSE ''
		END AS Striping,
		
		CASE
				WHEN work_orders.service_id IN (2, 3, 4, 5, 6, 11, 13, 18, 21, 45, 53, 54, 66) THEN 'Yes'
				ELSE ''
		END AS SubServices,
		
		CAST(
				REPLACE(
						REPLACE(
								REPLACE(
										REPLACE(
												REPLACE(
														work_orders.notes, 
														'<p>', '' -- Remove opening paragraph tags
												), '</p>', '' -- Remove closing paragraph tags
										), '<br>', '' -- Remove line break tags
								), CHAR(13) + CHAR(10), ' ' -- Replace newlines with spaces
						), '<', '' -- Remove any remaining opening angle brackets
				) AS NVARCHAR(MAX)
		) AS Notes






FROM jobs
LEFT JOIN job_area ON job_area.id = jobs.sub_region_id
LEFT JOIN project_stages ON project_stages.id = jobs.project_stage_id
LEFT JOIN work_orders ON work_orders.job_id = jobs.id
LEFT JOIN services ON services.id = work_orders.service_id
LEFT JOIN locations ON locations.id = jobs.location_id
LEFT JOIN users ON jobs.estimator_id = users.id
LEFT JOIN work_order_statuses ON work_order_statuses.id = work_orders.work_order_status_id
LEFT JOIN users AS project_manager ON project_manager.id = jobs.manager_id
LEFT JOIN Est_Area_Calculations ea ON ea.work_order_id = work_orders.id AND ea.service_id = services.id
LEFT JOIN Est_Material_Calculations em ON em.work_order_id = work_orders.id AND em.service_id = services.id
LEFT JOIN Next_WO_Date_Calculations nwd ON nwd.work_order_id = work_orders.id
LEFT JOIN Last_WO_Date_Calculations lwd ON lwd.work_order_id = work_orders.id
LEFT JOIN Act_Material_Calculations am ON am.work_order_id = work_orders.id
LEFT JOIN ccj_crack cc ON cc.work_order_id = work_orders.id AND cc.service_id = services.id
LEFT JOIN task_summary ON task_summary.work_order_id = work_orders.id
LEFT JOIN work_order_services_json ON work_order_services_json.work_order_id = work_orders.id
LEFT JOIN work_order_subcontractors ON work_order_subcontractors.work_order_id = work_orders.id
LEFT JOIN subcontractors ON subcontractors.id = work_order_subcontractors.subcontractor_id

WHERE jobs.deleted_at IS NULL 
AND work_orders.deleted_at IS NULL
AND project_stages.id <> 10
AND jobs.id = 7820
ORDER BY jobs.id DESC;
