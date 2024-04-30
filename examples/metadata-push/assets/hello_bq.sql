/* @bruin

name: dashboard.hello_bq
type: bq.sql

description: |
  This asset contains one line per booking, along with the booking's details. It is meant to serve as the single-source-of-truth for the booking entity.

  ## Primary Terms
  - Booking: the representation of the real-life booking the user had with their coach
  - Booking Status: an enum representing the state of the booking at the time
  - Cancellation Reason: The reason used to cancel the booking, available only for cancelled bookings.

  ## Sample Query
  ```sql
  SELECT Organization, count(*)
  FROM `dashboard.bookings`
  GROUP BY 1
  ORDER BY 2 DESC
  ```

  If you are interested in changing/managing individual bookings, please visit [Pace platform](https://pace.neooptima.com/).

materialization:
    type: table

depends:
   - basic

columns:
  - name: one
    description: the column description given by burak
    type: STRING
    primary_key: true
    checks:
        - name: not_null
  - name: two
    description: second col here
    type: STRING
    checks:
        - name: not_null
  - name: three
    description: third one honey, updated
    type: STRING
    checks:
        - name: not_null


@bruin */

select 1 as one, 'secondval' as two, 'whataboutthis' as three
union all
select 2 as one, 'secondval' as two, 'whataboutthis' as three
--     and {{ start_date }}
--     and {{ end_timestamp }}
--     and {{ end_timestamp | add_days(2) }}
--     and {{ end_timestamp | add_days(2) | date_format('%Y-%m-%d') }}
