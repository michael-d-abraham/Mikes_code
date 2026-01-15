SELECT
    jobs.id AS [Project ID],
    jobs.name AS [Project Name],
		project_stages.display_name AS Status,
    customers.name AS [Customer],
    end_customer.name AS [End Customer],
    '' AS [Parent Customer],
    jobs.deal_id AS [Deal ID],
    jobs.ns_transaction_id AS [Proposal ID],
		regions.name AS Region,
    (job_area.state + ' - ' + job_area.area) AS [SubRegion],
    CONCAT(sales_rep.first_name, ' ', sales_rep.last_name) AS [Sales Rep],
    CONCAT(sales_support.first_name, ' ', sales_support.last_name) AS [Sales Support],
    project_populations.name AS [Population],
    job_category.category AS [Category],
    CONCAT(PM.first_name, ' ', PM.last_name) AS [Project Manager],
		jobs.zipcode AS [Project Zip],
		locations.short_code AS [Yard],
		locations.zip AS [Yard Zip]

FROM jobs
LEFT JOIN customers ON customers.id = jobs.customer_id
LEFT JOIN customers end_customer ON end_customer.id = jobs.end_customer_id
-- LEFT JOIN customers Parent_customer ON customers.id = jobs.end_customer_id
LEFT JOIN regions ON regions.id = jobs.region_id
LEFT JOIN job_area ON job_area.id = jobs.sub_region_id
LEFT JOIN locations ON locations.id = jobs.location_id
LEFT JOIN users sales_rep ON sales_rep.id = jobs.estimator_id
LEFT JOIN users sales_support ON sales_support.id = jobs.sales_support_id
LEFT JOIN users PM ON PM.id = jobs.manager_id
LEFT JOIN project_populations ON project_populations.id = jobs.project_population_id
LEFT JOIN job_category ON job_category.id = jobs.category
LEFT JOIN project_stages ON project_stages.id = jobs.project_stage_id
WHERE jobs.deleted_at IS NULL
  AND jobs.project_status_id <> 10
  AND regions.active = 1
  AND customers.enabled = 1
  AND customers.deleted_at IS NULL
--   AND locations.status = 1
  AND job_area.active = 1
  AND job_category.active = 1
  AND jobs.id = 8339
	
