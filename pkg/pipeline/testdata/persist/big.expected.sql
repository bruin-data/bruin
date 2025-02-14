/* @bruin

name: dashboard.bookings
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

materialization:
  type: table

depends:
  - raw.Bookings
  - raw.Sessions
  - raw.Languages
  - raw.Programmes
  - dashboard.organizations
  - raw.Teams
  - dashboard.users
  - dashboard.session_type_mapping
owner: sabri.karagonen@getbruin.com
extends: []

columns:
  - name: BookingId
    type: STRING
    description: Unique identifier for the booking
    primary_key: true
    checks:
      - name: not_null
      - name: unique
      - name: positive
  - name: UserId
    type: STRING
    description: Unique identifier for the user
  - name: StartDateDt
    type: TIMESTAMP
    description: Date the booking starts
    checks:
      - name: not_null
  - name: SessionType
    type: STRING
    description: Type of session
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - "1:1"
          - Group
  - name: Status
    type: STRING
    description: Status of the booking
    checks:
      - name: not_null
  - name: SessionLanguage
    type: STRING
    description: Language of the session
    checks:
      - name: not_null
  - name: ProgramName
    type: STRING
    description: Name of the program
    checks:
      - name: not_null
  - name: Organization
    type: STRING
    checks:
      - name: not_null
  - name: SessionName
    type: STRING
    checks:
      - name: not_null

custom_checks:
  - name: Mike Blackburn has 16 credits in June
    value: 16
    query: |
      SELECT
        count(*)
      FROM `dashboard.bookings`
      where CoachName='Mike Blackburn'
        and date_trunc(StartDateDt, month) = "2022-06-01"
        and credits_spent = 1
  - name: Mike Blackburn has 1 cancelled booking in June
    value: 1
    query: |
      SELECT
        count(*)
      FROM `dashboard.bookings`
      where CoachName='Mike Blackburn'
        and date_trunc(StartDateDt, month) = "2022-06-01"
        and credits_spent = 1
        and Status = "Cancelled"
  - name: Mike Blackburn has 15 finished bookings in June
    value: 15
    query: |
      SELECT
        count(*)
      FROM `dashboard.bookings`
      where CoachName='Mike Blackburn'
        and date_trunc(StartDateDt, month) = "2022-06-01"
        and credits_spent =1
        and Status = "Finished"
  - name: Mike Blackburn has 4 rebookings in June
    value: 4
    query: |
      WITH
      user_coach_bookings as
      (
          SELECT
              UserId,
              CoachName,
              greatest(count(*) - 1, 0) as rebookings
          FROM `dashboard.bookings`
          where credits_spent = 1
            and CoachName = "Mike Blackburn"
            and date_trunc(StartDateDt, month) = "2022-06-01"
          group by 1,2
      )
          select
              sum(rebookings) as rebookings,
          from user_coach_bookings
  - name: Laura Roberts has 8 credits in June
    value: 8
    query: |
      SELECT
        count(*)
      FROM `dashboard.bookings`
      where CoachName='Laura Roberts'
        and date_trunc(StartDateDt, month) = "2022-06-01"
        and credits_spent =1
  - name: Laura Roberts has none cancelled booking in June
    query: |
      SELECT
        count(*)
      FROM `dashboard.bookings`
      where CoachName='Laura Roberts'
        and date_trunc(StartDateDt, month) = "2022-06-01"
        and credits_spent =1
        and Status = "Cancelled"
  - name: Laura Roberts has 8 finished bookings in June
    value: 8
    query: |
      SELECT
        count(*)
      FROM `dashboard.bookings`
      where CoachName='Laura Roberts'
        and date_trunc(StartDateDt, month) = "2022-06-01"
        and credits_spent =1
        and Status = "Finished"
  - name: Laura Roberts has 2 rebookings in June
    value: 2
    query: |
      WITH
      user_coach_bookings as
      (
          SELECT
              UserId,
              CoachName,
              greatest(count(*) - 1, 0) as rebookings
          FROM `dashboard.bookings`
          where credits_spent = 1
            and CoachName = "Laura Roberts"
            and date_trunc(StartDateDt, month) = "2022-06-01"
          group by 1,2
      )
          select
              sum(rebookings) as rebookings,
          from user_coach_bookings
  - name: Mark Pringle has 5 credits in June
    value: 5
    query: |
      SELECT
        count(*)
      FROM `dashboard.bookings`
      where CoachName='Mark Pringle'
        and date_trunc(StartDateDt, month) = "2022-06-01"
        and credits_spent =1
  - name: Mark Pringle has 5 finished bookings in June
    value: 5
    query: |
      SELECT
        count(*)
      FROM `dashboard.bookings`
      where CoachName='Mark Pringle'
        and date_trunc(StartDateDt, month) = "2022-06-01"
        and credits_spent =1
        and Status = "Finished"
  - name: Mark Pringle has none rebooking in June
    query: |
      WITH
      user_coach_bookings as
      (
          SELECT
              UserId,
              CoachName,
              greatest(count(*) - 1, 0) as rebookings
          FROM `dashboard.bookings`
          where credits_spent = 1
            and CoachName = "Mark Pringle"
            and date_trunc(StartDateDt, month) = "2022-06-01"
            and SessionType= "1:1"
          group by 1,2
      )
          select
              sum(rebookings) as rebookings,
          from user_coach_bookings
  - name: row counts must be equal
    query: |-
      select
      (
        select
          count(*)
        from `dashboard.bookings`
      ) -
      (
        select
          count(*)
        from `raw.Bookings`
      )

@bruin */

SELECT
    bookings.Id as BookingId,
    bookings.UserId,
    bookings.StartDate,
    date(bookings.StartDate) as StartDateDt,
    dashboard.session_type_mapping(bookings.SessionType) as SessionType,
    teams.Name as TeamName,
    bookings.TeamId,
    dashboard.booking_status(
        bookings.CancelledAt,
        bookings.Accepted,
        bookings.StartDate,
        bookings.EndDate,
        bookings.CoachRespondedAt,
        bookings.RequestCancelledAt,
        bookings.ConfirmedAt,
        bookings.Expired
    ) as Status,
    users.MemberName,
    coaches.MemberName as CoachName,
    bookings.CoachId,
    sessions.Id as SessionId,
    sessions.Name as SessionName,
    organizations.Name as Organization,
    bookings.OrganizationId,
    languages.Name as SessionLanguage,
    bookings.LanguageId,
    programmes.Name as ProgramName,
    bookings.ProgrammeId,
    organizations.Country,
    organizations.State,
    case
        when bookings.CancelledAt is not null
        then coalesce(bookings.CancellationReason, 'Empty Reason')
    end as CancellationReason,
    case
        when
            bookings.Id is not null and
            bookingCreditRefundedAt is null and
            bookings.Accepted
        then 1
        else 0
    end as credits_spent,

from `raw.Bookings` as bookings
inner join `raw.Sessions`as sessions
    on bookings.SessionId = sessions.Id
inner join `dashboard.users` as coaches
    on Coaches.Id = bookings.CoachId
inner join `raw.Languages` as languages
    on bookings.LanguageId = languages.Id
inner join  `raw.Programmes`  as programmes
    on Bookings.ProgrammeId = Programmes.Id
inner join  `dashboard.organizations`  as organizations
    on Programmes.OrganizationId = Organizations.Id
left join `dashboard.users` as users
    on Users.Id = bookings.UserId
left join `raw.Teams` teams
    on teams.Id = bookings.TeamId
