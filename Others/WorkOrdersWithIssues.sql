WITH FleetIssues AS (
    SELECT
        tf.task_id,
        MAX(CASE WHEN tf.has_issue = 1 THEN 1 ELSE 0 END) AS has_fleet_issue,
        STRING_AGG(NULLIF(tf.issue, ''), ' | ') WITHIN GROUP (ORDER BY tf.id) AS fleet_issue_notes
    FROM task_fleet tf
    GROUP BY tf.task_id
)
SELECT
    j.id AS [Project_ID],
    j.name AS [Name],
    CASE
        WHEN j.region_id = 1 THEN 'HAU'
        WHEN j.region_id = 5 THEN 'HAU - TX'
        WHEN j.region_id = 4 THEN 'HAU - SE'
        WHEN j.region_id = 2 THEN 'HAA'
        ELSE 'Unknown'
    END AS Region,
    t.id AS WO_ID,
    t.[date] AS [WO_Date],
    LTRIM(RTRIM(ISNULL(fm.first_name, '') + ' ' + ISNULL(fm.last_name, ''))) AS [Foreman],
    LTRIM(RTRIM(ISNULL(sup.first_name, '') + ' ' + ISNULL(sup.last_name, ''))) AS [Super],
    s.abbreviation AS [Service],
    t.weather_issue AS [Weather],
    t.job_incident AS [Job_Incident],
    COALESCE(fi.has_fleet_issue, 0) AS [Fleet_Issue],

    CAST(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            LTRIM(RTRIM(
                                ISNULL(NULLIF(t.incident_notes, ''), '')
                                + CASE WHEN NULLIF(t.incident_notes,'') IS NOT NULL 
                                         AND NULLIF(t.weather_notes,'')  IS NOT NULL THEN ' | ' ELSE '' END
                                + ISNULL(NULLIF(t.weather_notes, ''), '')
                                + CASE WHEN (NULLIF(t.incident_notes,'') IS NOT NULL OR NULLIF(t.weather_notes,'') IS NOT NULL)
                                         AND NULLIF(fi.fleet_issue_notes,'') IS NOT NULL THEN ' | ' ELSE '' END
                                + ISNULL(NULLIF(fi.fleet_issue_notes, ''), '')
                            )),
                        '<p>', ''),
                    '</p>', ''),
                '<br>', ''),
            CHAR(13) + CHAR(10), ' '),
        '<', ''
        ) AS NVARCHAR(MAX)
    ) AS [Notes],

    -- Clean “note -- name” from t.pm_comment, or NULL if pattern not found
    CASE 
        WHEN pm.idx_colon IS NOT NULL AND pm.idx_span IS NOT NULL AND pm.idx_span > pm.idx_colon + 3
             THEN LTRIM(RTRIM(SUBSTRING(pm.clean, pm.idx_colon + 3, pm.idx_span - (pm.idx_colon + 3))))
        ELSE NULL
    END AS [PM Notes]

FROM jobs j
LEFT JOIN work_orders wo ON wo.job_id = j.id
LEFT JOIN tasks t ON t.work_order_id = wo.id
LEFT JOIN users fm  ON fm.id  = t.foreman_id
LEFT JOIN users sup ON sup.id = t.superintendent_id
LEFT JOIN services s ON s.id = wo.service_id
LEFT JOIN FleetIssues fi ON fi.task_id = t.id

-- Minimal, safe parsing for a single pm_comment per task
OUTER APPLY (
    SELECT
        clean = REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(t.pm_comment, ''),
                            '<p id="weather">','<p>'),
                            '<p id="other">','<p>'),
                            '<p>',''),
                            '</p>',''),
        idx_colon_raw = CHARINDEX(' : ', REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(t.pm_comment, ''),
                            '<p id="weather">','<p>'),
                            '<p id="other">','<p>'),
                            '<p>',''),
                            '</p>','')),
        idx_colon = NULLIF(CHARINDEX(' : ', REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(t.pm_comment, ''),
                            '<p id="weather">','<p>'),
                            '<p id="other">','<p>'),
                            '<p>',''),
                            '</p>','')), 0),
        idx_span  = NULLIF(CHARINDEX('<span',
                        REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(t.pm_comment, ''),
                            '<p id="weather">','<p>'),
                            '<p id="other">','<p>'),
                            '<p>',''),
                            '</p>',''),
                        CASE WHEN CHARINDEX(' : ', REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(t.pm_comment, ''),
                            '<p id="weather">','<p>'),
                            '<p id="other">','<p>'),
                            '<p>',''),
                            '</p>','')) > 0
                             THEN CHARINDEX(' : ', REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(t.pm_comment, ''),
                            '<p id="weather">','<p>'),
                            '<p id="other">','<p>'),
                            '<p>',''),
                            '</p>','')) + 3
                             ELSE 1
                        END), 0)
) pm



WHERE
    t.[date] BETWEEN '2025-05-01' AND '2025-07-31'
    AND (
        t.weather_issue = 1
        OR t.job_incident = 1
        OR EXISTS (SELECT 1 FROM task_fleet tf WHERE tf.task_id = t.id AND tf.has_issue = 1)
    );
