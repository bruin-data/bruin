# Data Dictionary

Every table you can query, every column, and what it holds. Row counts are from a
default install; they are identical on every install.

Coordinates are a synthetic local grid for Yorkville. Only distances between points are
meaningful — they do not correspond to anywhere real. All timestamps are naive local
time in a single fixed zone with no daylight saving, so no clock arithmetic anywhere can
produce a gap or a repeat.

## Summary

| Table | Rows |
|---|---|
| `town.citizens` | 12,400 |
| `town.addresses` | 4,900 |
| `town.cell_towers` | 58 |
| `town.cameras` | 24 |
| `town.building_readers` | 42 |
| `town.vehicles` | 7,600 |
| `town.vehicle_insurance` | 7,600 |
| `town.plate_reads` | 299,010 |
| `town.parking_citations` | 17,203 |
| `town.devices` | 14,200 |
| `town.device_pings` | 4,902,081 |
| `town.call_records` | 602,625 |
| `town.bank_accounts` | 13,594 |
| `town.merchants` | 740 |
| `town.card_transactions` | 713,698 |
| `town.businesses` | 620 |
| `town.employment` | 13,992 |
| `town.badges` | 2,900 |
| `town.building_access_events` | 85,497 |
| `town.property_records` | 4,900 |
| `town.council_decisions` | 340 |
| `town.firearm_licences` | 1,180 |
| `town.range_visits` | 24,680 |
| `town.travel_records` | 10,843 |
| `town.hotel_stays` | 6,035 |
| `town.clinic_visits` | 27,113 |
| `town.library_loans` | 27,026 |
| `town.gym_checkins` | 163,850 |
| `casefile.witness_statements` | 38 |
| `casefile.forensic_findings` | 14 |
| `casefile.interview_notes` | 26 |

## Residents and places

### `town.citizens` — 12,400 rows

The residents of Yorkville as the civic register holds them.

| Column | Type | Description |
|---|---|---|
| `citizen_id` | varchar | Resident identifier, format C##### |
| `first_name` | varchar | Given name |
| `last_name` | varchar | Family name |
| `date_of_birth` | date | Date of birth |
| `sex` | varchar | M or F as recorded on the register |
| `address_id` | integer | Registered address |
| `height_cm` | integer | Height in centimetres as last recorded |
| `handedness` | varchar | right, left or ambidextrous |
| `eye_colour` | varchar | Eye colour |
| `phone_number` | varchar | Registered contact number, format 55-###-#### |
| `marital_status` | varchar | single, married, cohabiting, divorced, widowed, or minor |
| `moved_in_date` | date | Date the resident registered at the current address |
| `birth_town` | varchar | Town of birth |
| `prior_service` | varchar | Service branch, null for residents who never served |
| `service_qualification` | varchar | Recorded service qualification, null for most who served |
| `shoe_size_eu` | integer | Shoe size, continental sizing |
| `licence_number` | varchar | Driving licence number, null for residents who hold none |

### `town.addresses` — 4,900 rows

Every postal address in Yorkville. Each street has an anchor point on the town grid and its addresses run along it, so neighbours really are neighbours and share a cell site.

| Column | Type | Description |
|---|---|---|
| `address_id` | integer | Address identifier |
| `street` | varchar | Street name |
| `number` | integer | House or building number |
| `unit` | varchar | Flat or unit designation, null for whole-building addresses |
| `district` | varchar | District, taken from the covering cell site |
| `lat` | double | Latitude on the town grid |
| `lon` | double | Longitude on the town grid |
| `building_type` | varchar | house, terrace, flat_block, maisonette, commercial or mixed_use |
| `nearest_cell_id` | varchar | Cell site whose coverage this address falls in |

### `town.cell_towers` — 58 rows

The 58 mobile cell sites covering Yorkville, laid out on the town's local coordinate grid. Coordinates are a synthetic local grid, not a real-world location; only distances between points are meaningful.

| Column | Type | Description |
|---|---|---|
| `cell_id` | varchar | Site identifier, format CELL-### |
| `lat` | double | Latitude on the town grid |
| `lon` | double | Longitude on the town grid |
| `district` | varchar | District the site sits in |
| `coverage_note` | varchar | What the operator records this site as serving |

### `town.cameras` — 24 rows

Yorkville's automatic number plate cameras. Six of them cover the approaches to Wychwood Square; the rest sit on through routes and the town boundary.

| Column | Type | Description |
|---|---|---|
| `camera_id` | varchar | Camera identifier, format CAM-## |
| `location` | varchar | Where the camera is mounted and which way it faces |
| `lat` | double | Latitude on the town grid |
| `lon` | double | Longitude on the town grid |
| `road_class` | varchar | square approach, through route or boundary |

### `town.building_readers` — 42 rows

Badge readers in Yorkville's twelve managed commercial buildings. Every door with a reader on it appears here, including the ones tenants rarely use.

| Column | Type | Description |
|---|---|---|
| `reader_id` | varchar | Reader identifier |
| `building` | varchar | Building the reader is fitted in |
| `zone` | varchar | Which door or route the reader controls |

## Vehicles

### `town.vehicles` — 7,600 rows

Vehicles taxed to an Yorkville address. Registration marks are three letters, a dash and three digits. Yorkville's letter series were issued in blocks over the years, so the leading letter clusters rather than spreading evenly.

| Column | Type | Description |
|---|---|---|
| `plate` | varchar | Registration mark, format AAA-### |
| `make` | varchar | Manufacturer |
| `model` | varchar | Model |
| `body_type` | varchar | hatchback, saloon, estate, suv, van, coupe or pickup |
| `colour` | varchar | Recorded colour |
| `year` | integer | Model year |
| `owner_citizen_id` | varchar | Registered keeper, null where a company holds the vehicle |
| `owner_business_id` | varchar | Registered company keeper, null for privately held vehicles |
| `registered_date` | date | Date the current keeper was recorded |

### `town.vehicle_insurance` — 7,600 rows

Live motor policies. A policy may name an additional driver, which is how a vehicle comes to be driven by someone other than its registered keeper.

| Column | Type | Description |
|---|---|---|
| `policy_id` | varchar | Policy identifier |
| `plate` | varchar | Vehicle the policy covers |
| `policyholder_citizen_id` | varchar | Person who holds the policy, null for company policies |
| `named_driver_citizen_id` | varchar | Additional driver named on the policy, null where none is named |
| `cover` | varchar | Level of cover |
| `started` | date | Date cover began |

### `town.plate_reads` — 299,010 rows

Automatic number plate reads from Yorkville's 24 cameras. A vehicle is read only when it passes a camera, so an absence means either that the vehicle was parked or that it was on roads no camera covers.

| Column | Type | Description |
|---|---|---|
| `read_id` | varchar | Read identifier |
| `camera_id` | varchar | Camera that made the read |
| `plate` | varchar | Registration mark read |
| `ts` | timestamp | When the read was made |
| `direction` | varchar | Direction of travel past the camera |

### `town.parking_citations` — 17,203 rows

Parking notices issued by the town wardens. The warden records the plate, not the driver.

| Column | Type | Description |
|---|---|---|
| `citation_id` | varchar | Notice identifier |
| `plate` | varchar | Registration mark on the notice |
| `issued_at` | timestamp | When the notice was issued |
| `street` | varchar | Where the vehicle was standing |
| `contravention` | varchar | What the notice was issued for |
| `paid` | boolean | Whether the notice has been paid |

## Telephony

### `town.devices` — 14,200 rows

Handsets active on the Yorkville networks. Prepaid handsets are sold over the counter and carry no subscriber record, so their citizen_id is null; around fourteen hundred residents use one as their only phone.

| Column | Type | Description |
|---|---|---|
| `device_id` | varchar | Handset identifier |
| `msisdn` | varchar | Subscriber number, format 55-###-#### |
| `citizen_id` | varchar | Registered subscriber, null for prepaid handsets |
| `activated_date` | date | Date the handset first appeared on the network |
| `handset_model` | varchar | Handset make and model |

### `town.device_pings` — 4,902,081 rows

Tower registrations for every handset on the Yorkville networks. A handset re-registers roughly every two hours while it is being carried; the six hours around the rally are kept at fifteen-minute resolution for every handset on the network, as operators do for any public event.

| Column | Type | Description |
|---|---|---|
| `device_id` | varchar | Handset that registered |
| `ts` | timestamp | Fifteen-minute bucket the registration falls in |
| `cell_id` | varchar | Cell site the handset registered on |

### `town.call_records` — 602,625 rows

Ninety days of connected and attempted calls on the Yorkville networks. A duration of zero means the call was not answered. cell_id is the site the calling handset was registered on when the call began.

| Column | Type | Description |
|---|---|---|
| `call_id` | varchar | Call identifier |
| `caller_msisdn` | varchar | Number that placed the call |
| `callee_msisdn` | varchar | Number that was called |
| `started_at` | timestamp | When the call began |
| `duration_sec` | integer | Seconds connected, zero when the call was not answered |
| `cell_id` | varchar | Site the calling handset was on |
| `direction` | varchar | outgoing or incoming, from the calling handset's record |

## Money

### `town.bank_accounts` — 13,594 rows

Current accounts held at Yorkville's banks, whether by a resident or a company.

| Column | Type | Description |
|---|---|---|
| `account_id` | varchar | Account identifier |
| `citizen_id` | varchar | Resident holder, null for company accounts |
| `business_id` | varchar | Company holder, null for personal accounts |
| `account_type` | varchar | current, savings or business |
| `opened` | date | Date the account was opened |

### `town.merchants` — 740 rows

Card terminals registered in Yorkville, one per trading premises.

| Column | Type | Description |
|---|---|---|
| `merchant_id` | varchar | Terminal identifier |
| `name` | varchar | Trading name |
| `category` | varchar | Category the terminal reports under |
| `address_id` | integer | Trading address |

### `town.card_transactions` — 713,698 rows

Ninety days of account entries: card payments, cash machine use, standing payments and transfers. Amounts are negative when money leaves the account.

| Column | Type | Description |
|---|---|---|
| `txn_id` | varchar | Entry identifier |
| `account_id` | varchar | Account the entry posted to |
| `ts` | timestamp | When the entry posted |
| `amount` | decimal | Signed amount, negative when money left the account |
| `channel` | varchar | card, atm_withdrawal, atm_deposit, transfer_in, transfer_out or direct_debit |
| `merchant_id` | varchar | Terminal the card payment was taken on, null for other channels |
| `counterparty_name` | varchar | Name on the other side of a transfer, null for card and cash entries |

## Work, property and the council

### `town.businesses` — 620 rows

Companies registered in Yorkville: employers, fleet operators, trade contractors and a handful of holding companies that own property rather than trade.

| Column | Type | Description |
|---|---|---|
| `business_id` | varchar | Company identifier |
| `name` | varchar | Registered company name |
| `sector` | varchar | Trade the company is registered under |
| `founded` | date | Date of incorporation |
| `principal_citizen_id` | varchar | Registered principal, null where the company files no named principal |
| `address_id` | integer | Registered office |
| `fleet_size` | integer | Vehicles registered to the company |

### `town.employment` — 13,992 rows

Job spells recorded against Yorkville companies. A spell with a null end date is still running.

| Column | Type | Description |
|---|---|---|
| `spell_id` | varchar | Job spell identifier |
| `citizen_id` | varchar | Employee |
| `business_id` | varchar | Employer |
| `role_title` | varchar | Job title as filed |
| `started` | date | Date the spell began |
| `ended` | date | Date the spell ended, null while it is still running |

### `town.badges` — 2,900 rows

Access badges issued for the managed buildings. A badge is issued by whichever company sponsors the holder, which for contractors is not the company that occupies the building.

| Column | Type | Description |
|---|---|---|
| `badge_id` | varchar | Badge identifier |
| `citizen_id` | varchar | Badge holder |
| `issued_by_business_id` | varchar | Company that sponsored the badge |
| `building` | varchar | Building the badge opens |
| `status` | varchar | active, expired or withdrawn |

### `town.building_access_events` — 85,497 rows

Badge presentations at the managed buildings. A refused presentation is still recorded, so an expired or withdrawn badge leaves a trail too.

| Column | Type | Description |
|---|---|---|
| `event_id` | varchar | Event identifier |
| `reader_id` | varchar | Reader the badge was presented to |
| `badge_id` | varchar | Badge presented |
| `ts` | timestamp | When the badge was presented |
| `result` | varchar | granted or refused |

### `town.property_records` — 4,900 rows

The land register: every parcel in Yorkville, who holds it and how it is zoned. The Nordheimer Vale parcels are the town's last large undeveloped tract, a shelf of ravine land above the old brickyards.

| Column | Type | Description |
|---|---|---|
| `parcel_id` | varchar | Parcel identifier |
| `address_id` | integer | Address the parcel sits at |
| `district` | varchar | District the parcel sits in |
| `owner_citizen_id` | varchar | Resident holder, null where a company holds the parcel |
| `owner_business_id` | varchar | Company holder, null for privately held parcels |
| `zoning_class` | varchar | residential, commercial, industrial, mixed or undeveloped |
| `last_transfer` | date | Date the parcel last changed hands |

### `town.council_decisions` — 340 rows

Motions put to Yorkville town council and how they went. Where a vote is tied the chair casts the deciding vote, and the minutes record who that was.

| Column | Type | Description |
|---|---|---|
| `motion_id` | varchar | Motion identifier |
| `motion` | varchar | Motion as minuted |
| `decided_on` | date | Date the motion was decided |
| `outcome` | varchar | carried, rejected or deferred |
| `votes_for` | integer | Votes in favour |
| `votes_against` | integer | Votes against |
| `casting_vote_citizen_id` | varchar | Councillor who cast the deciding vote, null unless the vote was tied |
| `affected_district` | varchar | District the motion concerned, null for town-wide business |

## Firearms and the ranges

### `town.firearm_licences` — 1,180 rows

Firearm certificates on the Yorkville register. Yorkville is a hunting town, so rifle certificates outnumber everything else and holding one says very little about a person.

| Column | Type | Description |
|---|---|---|
| `licence_id` | varchar | Certificate identifier |
| `citizen_id` | varchar | Certificate holder |
| `weapon_class` | varchar | Class of firearm the certificate covers |
| `calibre` | varchar | Chambering the certificate covers |
| `issued_date` | date | Date the certificate was issued |
| `expires_date` | date | Date the certificate lapses, five years after issue |
| `status` | varchar | active, expired, revoked or suspended |

### `town.range_visits` — 24,680 rows

Lane bookings at Yorkville's three shooting ranges over the last three years. Only the Bracondale ranges have butts beyond 300 metres.

| Column | Type | Description |
|---|---|---|
| `visit_id` | varchar | Booking identifier |
| `citizen_id` | varchar | Club member who booked the lane |
| `range_id` | varchar | Which range the lane belongs to |
| `check_in` | timestamp | When the member signed in |
| `lane_distance_m` | integer | Lane length in metres |
| `rounds_fired` | integer | Rounds logged against the booking |
| `score` | integer | Scored result out of 100, null when the session was not scored |

## Coming and going

### `town.travel_records` — 10,843 rows

Departures and arrivals recorded against Yorkville residents at the regional terminals. Journeys by road within the region are not recorded here.

| Column | Type | Description |
|---|---|---|
| `record_id` | varchar | Record identifier |
| `citizen_id` | varchar | Traveller |
| `direction` | varchar | departure or arrival |
| `ts` | timestamp | When the traveller passed the desk |
| `carrier` | varchar | Carrier the traveller booked with |
| `destination_code` | varchar | Three-letter code for the other end of the journey |
| `destination_type` | varchar | domestic or international |

### `town.hotel_stays` — 6,035 rows

Hotel bookings made by Yorkville residents, in the town and in the towns around it. The booking is recorded against whoever paid for the room.

| Column | Type | Description |
|---|---|---|
| `booking_id` | varchar | Booking identifier |
| `hotel_id` | varchar | Hotel identifier |
| `booker_citizen_id` | varchar | Resident who made the booking |
| `check_in` | date | First night of the stay |
| `check_out` | date | Morning the room was given up |
| `guests` | integer | Guests on the booking |
| `town_name` | varchar | Town the hotel is in |

## Everything else

### `town.clinic_visits` — 27,113 rows

Attendances at Yorkville General and the two branch surgeries over the last year. An admission with no discharge date was still open when the extract was taken.

| Column | Type | Description |
|---|---|---|
| `visit_id` | varchar | Attendance identifier |
| `citizen_id` | varchar | Patient |
| `clinic_id` | varchar | Site attended |
| `admitted` | timestamp | When the patient was seen |
| `discharged` | timestamp | When the patient left, null if still on the ward |
| `presenting_note` | varchar | Reason for attendance as recorded at the desk |
| `outcome` | varchar | treated and discharged, admitted, referred or did not attend |

### `town.library_loans` — 27,026 rows

Loans from Yorkville's public library over the last year, including the ones still out.

| Column | Type | Description |
|---|---|---|
| `loan_id` | varchar | Loan identifier |
| `citizen_id` | varchar | Borrower |
| `title` | varchar | Title borrowed |
| `subject` | varchar | Shelf the title sits on |
| `borrowed_on` | date | Date the loan was taken out |
| `returned_on` | date | Date the title came back, null if still out |

### `town.gym_checkins` — 163,850 rows

Turnstile records from Yorkville's three fitness centres. High volume and, so far as the case goes, entirely beside the point.

| Column | Type | Description |
|---|---|---|
| `checkin_id` | varchar | Turnstile record identifier |
| `citizen_id` | varchar | Member |
| `site_id` | varchar | Which centre |
| `ts` | timestamp | When the member came through the turnstile |
| `minutes_on_site` | integer | Minutes between entry and exit |

## The case papers

### `casefile.witness_statements` — 38 rows

Statements taken by responding officers in the twelve hours after the shooting. Witnesses describe what they saw; none of them knew who they were looking at.

| Column | Type | Description |
|---|---|---|
| `statement_id` | varchar | Statement identifier |
| `taken_at` | timestamp | When the statement was taken |
| `witness_ref` | varchar | Reference the officer logged the witness under |
| `location` | varchar | Where the witness was standing |
| `statement` | varchar | What the witness said, as written down |

### `casefile.forensic_findings` — 14 rows

Findings filed by the scene examiners and the regional laboratory. Each finding narrows what kind of person and what kind of weapon, and nothing further.

| Column | Type | Description |
|---|---|---|
| `finding_id` | varchar | Finding identifier |
| `filed_on` | date | Date the finding was filed |
| `discipline` | varchar | Which examiner filed it |
| `finding` | varchar | The finding as written |

### `casefile.interview_notes` — 26 rows

Notes from follow-up interviews in the fortnight after the shooting. These are the officers' summaries, not verbatim records, and no interviewee is named.

| Column | Type | Description |
|---|---|---|
| `note_id` | varchar | Note identifier |
| `interviewed_on` | date | Date of the interview |
| `interviewee_role` | varchar | How the interviewee came to be relevant |
| `note` | varchar | The officer's summary |

