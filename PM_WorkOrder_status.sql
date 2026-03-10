SELECT 
    jobs.id AS [Project ID],
    jobs.name AS [Project Name],
    project_stages.display_name AS [Project Status],
    work_orders.id AS [Work Order ID],
    services.abbreviation AS [Service],
    work_order_statuses.name AS [Service Status],
    tasks.id AS [Task ID],
    task_statuses.name AS [Task Status],
    tasks.completed_at AS [Completed At],
    tasks.completed_by AS [Completed By]
FROM jobs
LEFT JOIN work_orders 
    ON jobs.id = work_orders.job_id
LEFT JOIN tasks 
    ON tasks.work_order_id = work_orders.id
LEFT JOIN project_stages 
    ON project_stages.id = jobs.project_stage_id
LEFT JOIN services 
    ON services.id = work_orders.service_id
LEFT JOIN work_order_statuses 
    ON work_order_statuses.id = work_orders.work_order_status_id
LEFT JOIN task_statuses 
    ON task_statuses.id = tasks.task_status_id
WHERE tasks.completed_at IS NOT NULL
  AND tasks.task_status_id <> 6
  AND tasks.task_status_id <> 8
  AND jobs.deleted_at IS NULL
  AND work_orders.deleted_at IS NULL
  AND jobs.project_stage_id <> 10
  AND LOWER(jobs.name) NOT LIKE '%warranty%'
  AND LOWER(jobs.name) NOT LIKE '%test%';