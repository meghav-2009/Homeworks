--How many animals of each type have outcomes?
SELECT animal_type, COUNT(DISTINCT animal_id) as number_of_animals
FROM animaldimension
GROUP BY animal_type;

--How many animals are there with more than 1 outcome?
SELECT COUNT(animalkey) AS animals_with_more_than_one_outcome
FROM (
    SELECT animalkey
    FROM animalfact
    GROUP BY animalkey
    HAVING COUNT(DISTINCT outcomekey) > 1
) AS animals_with_multiple_outcomes;

--What are the top 5 months for outcomes? 
SELECT month, COUNT(*) as outcome_count
FROM timedimension
JOIN animalfact ON timedimension.timekey = animalfact.timekey
GROUP BY month
ORDER BY outcome_count DESC
LIMIT 5;

--
select
	cat_age_grp,
	COUNT(*) as count
from
	(
SELECT ad.animalkey,
    CASE WHEN age(dob) <  interval '1 year'  THEN 'Kitten'
        WHEN age(dob) >=  interval '1 year'  AND age(dob) <= interval '10 years' THEN 'Adult'
        WHEN age(dob) >  interval '10 years'  THEN 'Senior Cat'
    END AS cat_category
FROM animaldimension ad) as catgrp
JOIN animalfact af ON af.animalkey = catgrp.animalkey
JOIN outcomedimension od ON af.outcomekey = od.outcomekey
WHERE ad.animal_type = 'Cat'
    AND od.outcome_type = 'Adoption'
GROUP BY cat_category
ORDER BY cat_category;


--For each date, what is the cumulative total of outcomes up to and including this date?
SELECT
    ts AS date,
    SUM(COUNT(*)) OVER (ORDER BY ts) AS cumulative_total
FROM
    timedimension
LEFT JOIN
    animalfact ON timedimension.timekey = animalfact.timekey
GROUP BY
    ts
ORDER BY
    ts;








