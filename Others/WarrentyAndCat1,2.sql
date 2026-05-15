WITH JobsWithWarranty AS (
    SELECT 
        j.id AS job_id
    FROM jobs j
    JOIN work_orders wo 
        ON wo.job_id = j.id
    JOIN services s 
        ON s.id = wo.service_id
    WHERE s.id IN (43,44,45,46,47)        -- warranty services
      AND j.project_stage_id <> 10
      AND j.deleted_at IS NULL
      AND wo.deleted_at IS NULL
    GROUP BY j.id
),
JobsWithRegular AS (
    SELECT 
        j.id AS job_id
    FROM jobs j
    JOIN work_orders wo 
        ON wo.job_id = j.id
    JOIN services s 
        ON s.id = wo.service_id
    WHERE s.service_categories_id IN (1,2)   -- regular categories
      AND s.id NOT IN (43,44,45,46,47)       -- must NOT be a warranty service
      AND j.project_stage_id <> 10
      AND j.deleted_at IS NULL
      AND wo.deleted_at IS NULL
    GROUP BY j.id
)
SELECT 
    j.id AS Project_ID,
    j.name AS Project_Name,
    s.abbreviation AS Warranty_Service_Name,
    s.id AS Warranty_Service_ID,
    wo.id AS WorkOrder_ID
FROM jobs j
JOIN JobsWithWarranty w 
    ON w.job_id = j.id
JOIN JobsWithRegular r 
    ON r.job_id = j.id
JOIN work_orders wo 
    ON wo.job_id = j.id
JOIN services s 
    ON s.id = wo.service_id
WHERE s.id IN (43,44,45,46,47)           -- only return warranty rows
  AND j.project_stage_id <> 10
  AND j.deleted_at IS NULL
  AND wo.deleted_at IS NULL
ORDER BY j.id, wo.id;
