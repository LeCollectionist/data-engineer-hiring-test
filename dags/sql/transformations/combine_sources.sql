SELECT
    houses.*,
    destinations.destination_name,
    -- TODO(candidate): uncomment below when destinations query is ready
    -- destinations.destination_parent_lvl_1,
    -- destinations.destination_parent_lvl_2,
    -- destinations.destination_parent_lvl_3,
    -- destinations.destination_parent_lvl_4,
    -- destinations.destination_parent_lvl_5
FROM
    staging.houses_conformed AS houses
LEFT JOIN
    staging.destinations_conformed AS destinations
    ON destinations.destination_id = houses.destination_id
