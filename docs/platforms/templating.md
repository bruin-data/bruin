# Templating

Bruin supports [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) as its templating language for SQL assets. This allows you to write dynamic SQL queries that can be parameterized with variables. This is useful when you want to write a query that is parameterized by a date, a user ID, or any other variable.

The following is an example SQL asset that uses Jinja templating for different `start_date` and `end_date` parameters:

```bruinsql
SELECT * FROM my_table WHERE dt BETWEEN '{{ start_date }}' AND '{{ end_date }}'
```

Since `start_date` and `end_date` parameters are automatically passed to your assets by Bruin, this allows the same SQL asset definition to be used both as your regular execution, e.g. daily or hourly, as well as backfilling a longer period of time.

You can do more complex stuff such as looping over a list of values, or using conditional logic. Here's an example of a SQL asset that loops over a list of user IDs:

```bruinsql
{% set days = [1, 3, 7, 15, 30, 90] %}

SELECT
    conversion_date,
    cohort_id,
    {% for day_n in days %}
    SUM(IFF(days_since_install < {{ day_n }}, revenue, 0)) AS revenue_{{ day_n }}_days
    {% if not loop.last %},{% endif %}
    {% endfor %}
FROM user_cohorts
GROUP BY 1,2
```

This will render into the following SQL query:

```bruinsql
SELECT
    conversion_date,
    cohort_id,
    SUM(IFF(days_since_install < 1, revenue, 0)) AS revenue_1_days,
    SUM(IFF(days_since_install < 3, revenue, 0)) AS revenue_3_days,
    SUM(IFF(days_since_install < 7, revenue, 0)) AS revenue_7_days,
    SUM(IFF(days_since_install < 15, revenue, 0)) AS revenue_15_days,
    SUM(IFF(days_since_install < 30, revenue, 0)) AS revenue_30_days,
    SUM(IFF(days_since_install < 90, revenue, 0)) AS revenue_90_days
FROM user_cohorts
GROUP BY 1,2
```
You can read more about [Jinja here](https://jinja.palletsprojects.com/en/3.1.x/).