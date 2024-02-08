-- TODO(candidate):
-- Write a query to transform staging.destination_cleaned in order to have all parents destinations denormalized for each rows in the result
--
-- Example: Hierarchy is defined as follows for Destination "Avignon"
--          Europe (id: 4, parent_id: NULL)
--          |__ France (id: 1, parent_id: 4)
--              |__ Provence (id: 101, parent_id: 1)
--                  |__ Alpilles (id: 105, parent_id: 101)
--                      |__ Avignon (id: 107, parent_id: 105)
--
-- The result should give for Avignon:
-- | destination_id | destination_name | parent_lvl_1 | parent_lvl_2 | parent_lvl_3 | parent_lvl_4 | parent_lvl_5 |
-- | 107            | Avignon          | Europe       | France       | Provence     | Alpilles     | NULL         |
--
-- Note: The depth of the hierarchy is not fixed and can be different for each destination, but with a maximum of 6 levels (so max 5 parents is possible).

SELECT
    id AS destination_id,
    name AS destination_name,
    -- TODO: Add the parents destinations, feel free to add more SQL script if you feel the need to do so
FROM
    staging.destinations_cleaned;
