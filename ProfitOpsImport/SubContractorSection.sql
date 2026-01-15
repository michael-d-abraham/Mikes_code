WITH SupportServiceHours AS (
    SELECT 
        jobs.id AS job_id,
        work_orders.id AS work_order_id,

        SUM(CASE WHEN services.id = 1 THEN DATEDIFF(MINUTE, timeclock.[in], timeclock.[out]) / 60.0 ELSE 0 END) AS Noticing_Labor_Hrs,
        SUM(CASE WHEN services.id = 52 THEN DATEDIFF(MINUTE, timeclock.[in], timeclock.[out]) / 60.0 ELSE 0 END) AS Shuttle_Labor_Hrs,
        SUM(CASE WHEN services.id = 57 THEN DATEDIFF(MINUTE, timeclock.[in], timeclock.[out]) / 60.0 ELSE 0 END) AS TrafficControl_Labor_Hrs,
        SUM(CASE WHEN services.id = 15 THEN DATEDIFF(MINUTE, timeclock.[in], timeclock.[out]) / 60.0 ELSE 0 END) AS CleaningPJO_Labor_Hrs,
        SUM(CASE WHEN services.id = 74 THEN DATEDIFF(MINUTE, timeclock.[in], timeclock.[out]) / 60.0 ELSE 0 END) AS PreCleanTruck_Labor_Hrs

    FROM jobs
    INNER JOIN work_orders ON work_orders.job_id = jobs.id
    INNER JOIN tasks ON tasks.work_order_id = work_orders.id
    INNER JOIN services ON services.id = work_orders.service_id
    INNER JOIN timeclock ON timeclock.task_id = tasks.id AND timeclock.deleted_at IS NULL

    WHERE jobs.deleted_at IS NULL
      AND work_orders.deleted_at IS NULL
      AND work_orders.service_id IN (1, 52, 57, 15, 74)

    GROUP BY jobs.id, work_orders.id
)

SELECT
    jobs.id AS job_id,
    work_orders.service_id AS service_type_id,
    services.abbreviation AS service_name,
    work_orders.id AS service_id,
    work_order_statuses.name AS [Service Status],

		(
				ISNULL(work_order_subcontractors.tcd_qtr, 'TBD') 
				+ ' - ' + 
				ISNULL(work_order_subcontractors.tcd_year, 'TBD')
		) AS [TCD],


    -- Support Service Requirements
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.supportServices.requires_noticing'), '0') AS [Req Noticing],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.supportServices.requires_shuttle'), '0') AS [Req Shuttle],
    ISNULL(JSON_VALUE(work_order_services_json.[fields], '$.supportServices.requires_traffic_control'), '0') AS [Req Traffic Control],

    -- Subcontractor Info
    subcontractors.name AS [SubContractor Name],

    -- Work Order Quantities
    work_order_subcontractors.sqft AS [SqFt],
    work_order_subcontractors.qty AS [Qty],

    -- Financial Fields
    work_order_subcontractors.bid AS [Bid Amount],
    work_order_subcontractors.revenue AS [Revenue],
    NULL AS [% MarkUp - Sales], -- Needs to be added
    NULL AS [Sub Cost - Actual], -- Needs to be added
    NULL AS [%MarkUp - Final], -- Needs to be added

    -- Support Service Labor Hrs
    FORMAT(ssh.Noticing_Labor_Hrs, 'N2') AS [Noticing Labor Hrs],
    FORMAT(ssh.Shuttle_Labor_Hrs, 'N2') AS [Shuttle Labor Hrs],
    FORMAT(ssh.TrafficControl_Labor_Hrs, 'N2') AS [Traffic Control Labor Hrs],
    FORMAT(ssh.CleaningPJO_Labor_Hrs, 'N2') AS [Cleaning PJO Labor Hrs],
    FORMAT(ssh.PreCleanTruck_Labor_Hrs, 'N2') AS [PreClean Truck Labor Hrs],

    -- Invoicing Fields
    NULL AS [Proposal Amount], -- Needs to be added
    NULL AS [NS Invoice #], -- Needs to be added
    NULL AS [NS Invoice Amount] -- Needs to be added

FROM jobs
LEFT JOIN work_orders ON work_orders.job_id = jobs.id
LEFT JOIN services ON services.id = work_orders.service_id
LEFT JOIN work_order_services_json ON work_order_services_json.work_order_id = work_orders.id
LEFT JOIN work_order_statuses ON work_order_statuses.id = work_orders.work_order_status_id
LEFT JOIN SupportServiceHours ssh ON ssh.job_id = jobs.id AND ssh.work_order_id = work_orders.id
LEFT JOIN work_order_subcontractors ON work_order_subcontractors.work_order_id = work_orders.id
LEFT JOIN subcontractors ON subcontractors.id = work_order_subcontractors.subcontractor_id

WHERE jobs.deleted_at IS NULL
  AND work_orders.deleted_at IS NULL
  AND jobs.id = 8339
  AND services.service_categories_id = 4

ORDER BY jobs.id;
