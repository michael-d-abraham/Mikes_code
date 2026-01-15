;WITH TimeByUserTask AS (
    SELECT
        wo.job_id                                  AS project_id,
        wo.service_id                              AS service_id,
        tc.task_id                                 AS task_id,
        CAST(t.created_at AS date)                 AS task_date,
        tc.user_id,
        SUM(DATEDIFF(MINUTE, tc.[in], tc.[out])) / 60.0          AS total_hrs
    FROM timeclock tc
    JOIN tasks t
        ON t.id = tc.task_id
    JOIN work_orders wo
        ON wo.id = t.work_order_id
    JOIN jobs j
        ON j.id = wo.job_id
    WHERE
        tc.[in] IS NOT NULL
        AND tc.[out] IS NOT NULL
        AND tc.deleted_at IS NULL
        AND wo.deleted_at IS NULL
        AND j.deleted_at IS NULL
    GROUP BY
        wo.job_id,
        wo.service_id,
        tc.task_id,
        CAST(t.created_at AS date),
        tc.user_id
)

SELECT
    j.id                                           AS [Project #],
    j.name                                         AS [Project Name],
    s.name                                         AS [Service],
    t.id                                           AS [WO #],
    FORMAT(t.created_at, 'MM/dd/yyyy')             AS [WO Date],
    CONCAT(u.first_name, ' ', u.last_name)         AS [Employee],
    FORMAT(ROUND(tb.total_hrs, 2), 'N2')           AS [Total Hrs]
FROM TimeByUserTask tb
JOIN jobs j
    ON j.id = tb.project_id
JOIN tasks t
    ON t.id = tb.task_id
JOIN work_orders wo
    ON wo.id = t.work_order_id
LEFT JOIN users u
    ON u.id = tb.user_id
LEFT JOIN services s
    ON s.id = tb.service_id
WHERE
    tb.project_id = 7742
    AND tb.service_id = 19
ORDER BY
    [Project #], [WO #], [WO Date], [Employee];
