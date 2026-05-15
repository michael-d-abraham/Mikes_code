SELECT
    work_orders.id,
    work_order_statuses.name AS StatusName,
    services.abbreviation AS ServiceAbbrev,
    work_orders.created_at,
    work_orders.updated_at,
    work_orders.deleted_at
FROM work_orders
LEFT JOIN work_order_statuses 
    ON work_orders.work_order_status_id = work_order_statuses.id
LEFT JOIN services 
    ON work_orders.service_id = services.id;
