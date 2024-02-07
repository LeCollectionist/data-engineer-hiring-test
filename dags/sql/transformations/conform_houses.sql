SELECT
	id,
	created_at,
	name AS house_name, -- rename column
	capacity,
	bedrooms,
	destination_id
FROM
	staging.houses_cleaned
