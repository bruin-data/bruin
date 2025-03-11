# Filters

Bruin supports Jinja filters to modify any given variable before they are converted to string. Filters are separated from the variable by a pipe symbol (`|`) and may have optional arguments in parentheses. Multiple filters can be chained. The output of one filter is applied to the next.

```sql
SELECT * 
FROM my_table 
WHERE dt BETWEEN '{{ start_date |  date_format('%Y-%m-%d') }}' 
             AND '{{ end_date | date_format('%Y-%m-%d') }}'
```

## Default Filters
Bruin CLI supports various default filters supported by Jinja, you can see the [list here](https://github.com/NikolaLohinski/gonja/blob/master/docs/filters.md).

## Date Filters
### `add_years`

Adds the given number of years to the date.

```jinja
{{ end_datetime | add_years(2) }}
```

Bruin supports various date-related filters to format dates that can be chained together.

### `add_months`

Adds the given number of months to the date.

```jinja
{{ end_datetime | add_months(3) }}
```


### `add_days`

Adds the given number of days to the date.

```jinja
{{ end_datetime | add_days(3) }}
```

### `add_hours`

Adds the given number of hours to the date.

```jinja
{{ end_datetime | add_hours(3) }}
```

### `add_minutes`

Adds the given number of minutes to the date.

```jinja
{{ end_datetime | add_minutes(3) }}
```

### `add_seconds`

Adds the given number of seconds to the date.

```jinja
{{ end_datetime | add_seconds(3) }}
```

### `add_milliseconds`

Adds the given number of milliseconds to the date.

```jinja
{{ end_datetime | add_milliseconds(3) }}
```

### `truncate_year`

Truncates the given date to the first day of the year at midnight (00:00:00).

```jinja
{{ "2024-03-20 15:30:00" | truncate_year }}  
# Returns: "2024-01-01 00:00:00"
```

### `truncate_month`

Truncates the given date to the first day of the month at midnight (00:00:00).

```jinja
{{ "2024-03-20 15:30:00" | truncate_month }}  
# Returns: "2024-03-01 00:00:00"
```

### `truncate_day`

Truncates the given date to the start of the day at midnight (00:00:00).

```jinja
{{ "2024-03-20 15:30:00" | truncate_day }}  
# Returns: "2024-03-20 00:00:00"
```

### `truncate_hour`

Truncates the given date to the start of the hour (XX:00:00).

```jinja
{{ "2024-03-20 15:30:45" | truncate_hour }}  
# Returns: "2024-03-20 15:00:00"
```

### `date_add`

> [!CAUTION]
> The `date_add` filter is deprecated. Please use the `add_days` filter instead for better compatibility and consistency.


Adds given number of days to the date.

```jinja
{{ end_datetime | date_add(3) }}
```

For a given datetime `2022-02-07T04:00:00.948740Z`, this results in the following string:

```
2022-02-10T04:00:00.948740Z
```

You can also give negative numbers to subtract days.

### `date_format`

Formats the date according to the given format string.

```jinja
{{ end_datetime | date_format('%Y-%m-%d') }}
```

For a given datetime `2022-02-07T04:00:00.948740Z`, this results in the following string:

```
2022-02-07
```

The format given here follows Python date formatting rules, you can see the [list here](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes).

