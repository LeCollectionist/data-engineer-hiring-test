SELECT
	id,
	created_at,
	TRIM(name) AS name, -- Remove spaces before and after name
	TRIM(house_style) AS house_style, -- Remove spaces before and after house_style
	TRIM(house_architectural_style) AS house_architectural_style, -- Remove spaces before and after house_architectural_style
	capacity,
	bedrooms,
	destination_id
FROM
	landing.raw_houses
