{{ config(database="side_project") }}

SELECT email,
SPLIT(email, "@")[OFFSET(0)] as username,
  SPLIT(SPLIT(email, "@")[OFFSET(1)], ".")[OFFSET(0)] as domain_name,
  CASE WHEN ARRAY_LENGTH(SPLIT(SPLIT(email, "@")[OFFSET(1)], "."))=2
    THEN SPLIT(SPLIT(email, "@")[OFFSET(1)], ".")[OFFSET(1)]
    ELSE CONCAT(SPLIT(SPLIT(email, "@")[OFFSET(1)], ".")[OFFSET(1)],
      ".",
      SPLIT(SPLIT(email, "@")[OFFSET(1)], ".")[OFFSET(2)]) 
  END AS domain_extension
FROM {{ ref('ms_people') }}