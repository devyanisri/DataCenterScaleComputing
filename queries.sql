--How many animals of each type have outcomes?
SELECT
    a.animal_type,
    COUNT(DISTINCT a.animal_id) AS animal_count
FROM
    animaldim a
JOIN
    outcomesfact o ON a.animal_dim_key = o.animal_dim_key
JOIN
    outcomedim od ON o.outcome_dim_key = od.outcome_dim_key
WHERE
    od.outcome_type IS NOT NULL
GROUP BY
    a.animal_type;


--How many animals are there with more than 1 outcome?
SELECT
    COUNT(DISTINCT a.animal_id) AS animals_with_morethan_one_count
FROM
    animaldim a
JOIN
    outcomesfact o ON a.animal_dim_key = o.animal_dim_key
JOIN
    outcomedim od ON o.outcome_dim_key = od.outcome_dim_key
WHERE
    od.outcome_type IS NOT NULL
GROUP BY
    a.animal_id
HAVING
    COUNT(*) > 1;


--What are the top 5 months for outcomes? 
SELECT
    t.monthh,
    COUNT(*) AS counter
FROM
    outcomesfact o2
JOIN
    outcomedim o ON o2.outcome_dim_key = o.outcome_dim_key
JOIN
    timingdim t ON o2.time_dim_key = t.time_dim_key
WHERE
    o.outcome_type IS NOT NULL
GROUP BY
    t.monthh
ORDER BY
    counter DESC
LIMIT 5;


--What is the total number percentage of kittens, adults, and seniors, whose outcome is "Adopted"?

--Conversely, among all the cats who were "Adopted", what is the total number percentage of kittens, adults, and seniors?

WITH Cat_Ages AS (
  SELECT
    ad.animal_dim_key,
    CASE
      WHEN AGE(ad.dob) < INTERVAL '1 year' THEN 'Kittens'
      WHEN AGE(ad.dob) >= INTERVAL '1 year' AND AGE(ad.dob) <= INTERVAL '10 years' THEN 'Adults'
      WHEN AGE(ad.dob) > INTERVAL '10 years' THEN 'Seniors'
      ELSE 'Unknown'
    END AS cat_age_grp
  FROM animaldim ad where ad.animal_type = 'Cat'
)

SELECT
  cat_age_grp,
  COUNT(*) AS toalcount
FROM outcomesfact o
JOIN Cat_Ages cat ON o.animal_dim_key  = cat.animal_dim_key
JOIN outcomedim od ON o.outcome_dim_key  = od.outcome_dim_key 
WHERE od.outcome_type  = 'Adoption'
GROUP BY cat_age_grp;

--For each date, what is the cumulative total of outcomes up to and including this date?
WITH CumulativeOutcomes AS (
    SELECT
        date(a.timestmp) AS date_only,
        COUNT(a.animal_dim_key) AS outcomes
    FROM
        animaldim a
    LEFT JOIN
        outcomesfact o2 ON a.animal_dim_key = o2.animal_dim_key
    LEFT JOIN
        outcomedim o ON o2.outcome_dim_key = o.outcome_dim_key
    WHERE
        o.outcome_type IS NOT NULL
    GROUP BY
        date(a.timestmp)
)

SELECT
    date_only,
    outcomes,
    SUM(outcomes) OVER (ORDER BY date_only) AS cumulative_total
FROM
    CumulativeOutcomes
ORDER BY
    date_only;
