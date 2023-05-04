-- Databricks notebook source
CREATE WIDGET TEXT year DEFAULT "2023-01-31"


-- COMMAND ----------

SELECT "${year}"

-- COMMAND ----------

set Year = (SELECT CAST("${year}" AS DATE))

-- COMMAND ----------

SELECT "${Year}"

-- COMMAND ----------

SELECT CAST("${year}" AS DATE)


-- COMMAND ----------

-- DBTITLE 1,Warmup JVM
DESCRIBE DATABASE gwas_catalog_optimized;
SHOW TABLES FROM gwas_catalog_optimized;



-- COMMAND ----------

-- DBTITLE 1,GWAS discoveries
with T as (
  select
    *
  from
    gwas_catalog_optimized.full
  where
--     cast(DATE as Date) <= '{{ Year }}'
    cast(DATE as Date) <= '2023-01-31'
)
select
  DATE,
  sum(count(DATE)) over (
    order by
      DATE rows unbounded preceding
  ) as cumulative_associations
from
  T
group by
  DATE
order by
  DATE

-- COMMAND ----------

-- DBTITLE 1,Journals
with F as (
  with Z as (
    with G as (
      with T as (
        select
          JOURNAL,
          DATE,
          count(*) over () as tmp
        from
          gwas_catalog_optimized.full
        where
--           cast(DATE as Date) <= '{{ Year }}'
          cast(DATE as Date) <= '2023-01-31'
      )
      select
        count(JOURNAL) as count,
        JOURNAL
      from
        T
      group by
        JOURNAL
      order by
        count
    )
    select
      count,
      JOURNAL,
      RANK () OVER(
        ORDER BY
          count DESC
      ) AS journal_rank
    from
      G
  )
  select
    count,
    case
      when journal_rank > 16 then "other"
      else JOURNAL
    end as journal
  from
    Z
)
select
  sum(count) as total,
  journal
from
  F
group by
  journal
order by
  total

-- COMMAND ----------

-- DBTITLE 1,Study Counts
with T as (
  select
    count(PUBMEDID) as count,
    PUBMEDID
  from
    gwas_catalog_optimized.full
  where
--     cast(DATE as Date) <= '{{ Year }}'
    cast(DATE as Date) <= '2023-01-31'
  group by
    PUBMEDID
)
select
  count(*)
from
  T

-- COMMAND ----------

-- DBTITLE 1,GWAS geographical distribution
with T as (
  select
    explode(split(COUNTRY_OF_RECRUITMENT, ", ")) as country
  from
    gwas_catalog_optimized.ancestry
  where
--     cast(DATE as Date) <= '{{ Year }}'
    cast(DATE as Date) <= '2023-01-31'
)
select
  count(country) as count,
  replace(
    replace(country, "U.K.", "United Kingdom"),
    "U.S.",
    "United States"
  ) as country
from
  T
group by
  country
order by
  count

-- COMMAND ----------

-- DBTITLE 1,Diseases / traits
select
  count(`DISEASE/TRAIT`) as count,
  `DISEASE/TRAIT`
from
  gwas_catalog_optimized.full
where
--   cast(DATE as Date) <= '{{ Year }}'
  cast(DATE as Date) <= '2023-01-31'
group by
  `DISEASE/TRAIT`
order by
  count desc

-- COMMAND ----------

-- DBTITLE 1,Genes
with T1 as (
  with T as (
    select
      *
    from
      gwas_catalog_optimized.full
    where
--       cast(DATE as Date) <= '{{ Year }}'
      cast(DATE as Date) <= '2023-01-31'
  ) (
    select
      distinct explode(split(MAPPED_GENE, ", ")) as gene,
      CHR_ID,
      CHR_POS
    from
      T
  )
)
select
  count(gene) as count,
  gene
from
  T1
group by
  gene
order by
  count desc

-- COMMAND ----------

-- DBTITLE 1,Distribution of associations
with T2 as (
  with T1 as (
    with T as (
      select
        *
      from
        gwas_catalog_optimized.full
      where
--         cast(DATE as Date) <= '{{ Year }}'
        cast(DATE as Date) <= '2023-01-31'
    ) (
      select
        explode(split(CONTEXT, "[,;]")) as CONTEXT
      from
        T
    )
  )
  select
    count(CONTEXT) as count,
    CONTEXT
  from
    T1
  group by
    CONTEXT
  order by
    count
)
select
  *
from
  T2
where
  count > 100
