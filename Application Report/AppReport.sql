WITH TaskMaterialSummary AS (
    SELECT
        regions.name AS region_name, 
        tasks.date AS WO_DATE,
        fleet.fleet_number,
        tasks.id AS task_id,
        SUM(task_material.actual) AS total_material
    FROM tasks
    LEFT JOIN task_fleet ON tasks.id = task_fleet.task_id
    LEFT JOIN fleet ON fleet.id = task_fleet.fleet_id AND fleet.deleted_at IS NULL
    LEFT JOIN fleet_categories ON fleet_categories.id = fleet.fleet_category_id
    LEFT JOIN task_material ON tasks.id = task_material.task_id AND fleet.id = task_material.fleet_id
    LEFT JOIN regions ON regions.id = tasks.region_id
    WHERE fleet_categories.id IN (1,2)
    AND task_material.deleted_at IS NULL
    AND fleet.deleted_at IS NULL
    AND tasks.date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY tasks.date, fleet.fleet_number, tasks.id, regions.name
)
SELECT
    region_name, 
    WO_DATE,
    fleet_number,
    STRING_AGG(
        CONCAT('Task ID: ', task_id, ' (Total Material: ', total_material, ')'), ' | '
    ) AS TaskSummary
FROM TaskMaterialSummary
GROUP BY region_name, WO_DATE, fleet_number  
HAVING COUNT(task_id) > 1
ORDER BY region_name, WO_DATE, fleet_number;

