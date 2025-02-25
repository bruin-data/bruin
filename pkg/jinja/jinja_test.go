package jinja

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJinjaRenderer_RenderQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		args    Context
		want    string
		wantErr bool
	}{
		{
			name:  "simple render for ds",
			query: "set analysis_end_date = '{{ ds }}'::date; select * from {{ ref('abc') }}",
			args: Context{
				"ds": "2022-02-03",
				"ref": func(str string) string {
					return "some-ref-here"
				},
			},
			want: "set analysis_end_date = '2022-02-03'::date; select * from some-ref-here",
		},
		{
			name:  "add_days",
			query: "{{ start_date | add_days(3) | add_days(1) | add_days(-5) | date_format('%Y/%m/%d') }}",
			args: Context{
				"start_date": "2022-02-03",
			},
			want: "2022/02/02",
		},
		{
			name:  "add_hours",
			query: "{{ start_datetime | add_hours(1) | add_minutes(12) | add_seconds(5) | date_format('%Y/%m/%d %H:%M:%S') }}",
			args: Context{
				"start_datetime": "2022-02-03T04:00:00",
			},
			want: "2022/02/03 05:12:05",
		},
		{
			name:  "multiple variables",
			query: "set analysis_end_date = '{{ ds }}'::date and '{{testVar}}' == 'testvar' and another date {{    ds }}",
			args: Context{
				"ds":      "2022-02-03",
				"testVar": "testvar",
			},
			want: "set analysis_end_date = '2022-02-03'::date and 'testvar' == 'testvar' and another date 2022-02-03",
		},
		{
			name: "jinja variables work as well",
			query: `
{% set payment_method = "bank_transfer" %}

select
    order_id,
    sum(case when payment_method = '{{payment_method}}' then amount end) as {{payment_method}}_amount,
    sum(amount) as total_amount
from app_data.payments
group by 1`,
			args: Context{},
			want: `


select
    order_id,
    sum(case when payment_method = 'bank_transfer' then amount end) as bank_transfer_amount,
    sum(amount) as total_amount
from app_data.payments
group by 1`,
		},
		{
			name: "array variables work",
			query: `
{% set payment_methods = ["bank_transfer", "credit_card", "gift_card"] %}

select
    order_id,
    {% for payment_method in payment_methods %}
    sum(case when payment_method = '{{payment_method}}' then amount end) as {{payment_method}}_amount,
    {% endfor %}
    sum(amount) as total_amount
from app_data.payments
group by 1`,
			args: Context{},
			want: `


select
    order_id,
    
    sum(case when payment_method = 'bank_transfer' then amount end) as bank_transfer_amount,
    
    sum(case when payment_method = 'credit_card' then amount end) as credit_card_amount,
    
    sum(case when payment_method = 'gift_card' then amount end) as gift_card_amount,
    
    sum(amount) as total_amount
from app_data.payments
group by 1`,
		},
		{
			name: "given array from outside is rendered",
			query: `
select
    order_id,
    {% for payment_method in payment_methods %}
    sum(case when payment_method = '{{payment_method}}' then amount end) as {{payment_method}}_amount,
    {% endfor %}
    sum(amount) as total_amount
from app_data.payments
group by 1`,
			args: Context{
				"payment_methods": []string{"bank_transfer", "credit_card", "gift_card"},
			},
			want: `
select
    order_id,
    
    sum(case when payment_method = 'bank_transfer' then amount end) as bank_transfer_amount,
    
    sum(case when payment_method = 'credit_card' then amount end) as credit_card_amount,
    
    sum(case when payment_method = 'gift_card' then amount end) as gift_card_amount,
    
    sum(amount) as total_amount
from app_data.payments
group by 1`,
		},
		{
			name: "for loop set",
			query: `
{%- for num in range(8, 11) %}
{{ ("0" ~ num|string)[-2:] -}}
{% endfor %}`,
			args: Context{},
			want: `
08
09
10`,
		},
		{
			name:  "truncate_year",
			query: "{{ date | truncate_year }}",
			args: Context{
				"date": "2024-03-15 14:30:45",
			},
			want: "2024-01-01 00:00:00",
		},
		{
			name:  "truncate_month",
			query: "{{ date | truncate_month }}",
			args: Context{
				"date": "2024-03-15 14:30:45",
			},
			want: "2024-03-01 00:00:00",
		},
		{
			name:  "truncate_day",
			query: "{{ date | truncate_day }}",
			args: Context{
				"date": "2024-03-15 14:30:45",
			},
			want: "2024-03-15 00:00:00",
		},
		{
			name:  "truncate_hour",
			query: "{{ date | truncate_hour }}",
			args: Context{
				"date": "2024-03-15 14:30:45",
			},
			want: "2024-03-15 14:00:00",
		},
		{
			name:  "truncate with different format",
			query: "{{ date | truncate_month | date_format('%Y-%m-%d') }}",
			args: Context{
				"date": "2024-03-15",
			},
			want: "2024-03-01",
		},
		{
			name:  "chained truncate operations",
			query: "{{ date | add_days(5) | truncate_month }}",
			args: Context{
				"date": "2024-03-15 14:30:45",
			},
			want: "2024-03-01 00:00:00",
		},
		{
			name:  "truncate with timestamp",
			query: "{{ date | truncate_day }}",
			args: Context{
				"date": "2024-03-15T14:30:45.123456Z",
			},
			want: "2024-03-15T00:00:00.000000Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			receiver := NewRenderer(tt.args)
			got, err := receiver.Render(tt.query)
			require.NoError(t, err)

			require.Equal(t, tt.want, got)
		})
	}
}

func TestJinjaRendererWithStartEndDate(t *testing.T) {
	t.Parallel()

	startDate, err := time.Parse("2006-01-02 15:04:05", "2022-02-03 04:00:00")
	require.NoError(t, err)

	endDate := time.Date(2022, 2, 4, 4, 0, 0, 948740170, time.UTC)

	tests := []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		{
			name:  "simple render for ds",
			query: "{{ end_date }}, {{ end_datetime | add_days(3) }}, {{ end_timestamp | add_days(3) }}, {{ end_timestamp | add_days(2) | date_format('%Y-%m-%d') }}",
			want:  "2022-02-04, 2022-02-07T04:00:00, 2022-02-07T04:00:00.948740Z, 2022-02-06",
		},
		{
			name:  "simple render for ds",
			query: "{{ end_date }}, {{ end_datetime | date_add(3) }}, {{ end_timestamp | date_add(3) }}, {{ end_timestamp | date_add(2) | date_format('%Y-%m-%d') }}",
			want:  "2022-02-04, 2022-02-07T04:00:00, 2022-02-07T04:00:00.948740Z, 2022-02-06",
		},
		{
			name:    "things that are not in the template should be remove",
			query:   "set analysis_end_date = '{{ whatever }}'::date;",
			wantErr: true,
		},
		{
			name: "array variables work",
			query: `
{%- set payment_methods = ["bank_transfer", "credit_card", "gift_card"] -%}

select
    order_id,
    {% for payment_method in payment_methods %}
    sum(case when payment_method = '{{payment_method}}' then amount end) as {{payment_method}}_amount,
    {%- endfor %}
    sum(amount) as total_amount
from app_data.payments
	where created_at >= '{{ start_datetime }}' and created_at < '{{ end_datetime }}'
group by 1`,
			want: `select
    order_id,
    
    sum(case when payment_method = 'bank_transfer' then amount end) as bank_transfer_amount,
    sum(case when payment_method = 'credit_card' then amount end) as credit_card_amount,
    sum(case when payment_method = 'gift_card' then amount end) as gift_card_amount,
    sum(amount) as total_amount
from app_data.payments
	where created_at >= '2022-02-03T04:00:00' and created_at < '2022-02-04T04:00:00'
group by 1`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			receiver := NewRendererWithStartEndDates(&startDate, &endDate, "your-pipeline-name", "your-run-id")
			got, err := receiver.Render(tt.query)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}

func TestJinjaRendererErrorHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		wantErr string
	}{
		{
			name:    "missing filter",
			query:   "{{ 'value' | random_filter_that_doesnt_exist('abc') }}",
			wantErr: "filter 'random_filter_that_doesnt_exist' not found",
		},
		{
			name:    "missing variable",
			query:   "{{ some_random_variable }}",
			wantErr: "missing variable 'some_random_variable'",
		},
		{
			name:    "missing endfor",
			query:   "{% for i in range(1, 10) %}{{ i }}",
			wantErr: "missing 'endfor' at (Line: 1 Col: 35, near \"\")",
		},
		{
			name:    "missing endif",
			query:   "{% if true %}{{ i }}",
			wantErr: "missing end of the 'if' condition at (Line: 1 Col: 21, near \"\"), did you forget to add 'endif'?",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			receiver := NewRenderer(Context{})
			_, err := receiver.Render(tt.query)
			require.Error(t, err)

			require.Equal(t, tt.wantErr, err.Error())
		})
	}
}

func TestAddMonths(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		date      string
		months    string
		want      string
		wantError bool
	}{
		{
			name:   "add positive months",
			date:   "2024-01-15",
			months: "3",
			want:   "2024-04-15",
		},
		{
			name:   "add negative months",
			date:   "2024-01-15",
			months: "-2",
			want:   "2023-11-15",
		},
		{
			name:   "add months across year boundary",
			date:   "2023-12-15",
			months: "2",
			want:   "2024-02-15",
		},
		{
			name:   "add zero months",
			date:   "2024-01-15",
			months: "0",
			want:   "2024-01-15",
		},
		{
			name:      "invalid months parameter",
			date:      "2024-01-15",
			months:    "invalid",
			wantError: true,
		},
		{
			name:      "invalid date format",
			date:      "not-a-date",
			months:    "1",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			query := fmt.Sprintf("{{ '%s' | add_months('%s') }}", tt.date, tt.months)
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(query)

			if tt.wantError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, result)
		})
	}
}

func TestAddYears(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		date      string
		years     string
		want      string
		wantError bool
	}{
		{
			name:  "add positive years",
			date:  "2024-01-15",
			years: "3",
			want:  "2027-01-15",
		},
		{
			name:  "add negative years",
			date:  "2024-01-15",
			years: "-2",
			want:  "2022-01-15",
		},
		{
			name:  "add zero years",
			date:  "2024-01-15",
			years: "0",
			want:  "2024-01-15",
		},
		{
			name:  "handle leap year",
			date:  "2024-02-29",
			years: "1",
			want:  "2025-03-01",
		},
		{
			name:      "invalid years parameter",
			date:      "2024-01-15",
			years:     "invalid",
			wantError: true,
		},
		{
			name:      "invalid date format",
			date:      "not-a-date",
			years:     "1",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			query := fmt.Sprintf("{{ '%s' | add_years('%s') }}", tt.date, tt.years)
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(query)

			if tt.wantError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, result)
		})
	}
}

func TestAddDays(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		date      string
		days      string
		want      string
		wantError bool
	}{
		{
			name: "add positive days",
			date: "2024-01-15",
			days: "5",
			want: "2024-01-20",
		},
		{
			name: "add negative days",
			date: "2024-01-15",
			days: "-3",
			want: "2024-01-12",
		},
		{
			name: "add zero days",
			date: "2024-01-15",
			days: "0",
			want: "2024-01-15",
		},
		{
			name: "cross month boundary",
			date: "2024-01-30",
			days: "3",
			want: "2024-02-02",
		},
		{
			name: "cross year boundary",
			date: "2023-12-30",
			days: "3",
			want: "2024-01-02",
		},
		{
			name: "handle leap year",
			date: "2024-02-28",
			days: "1",
			want: "2024-02-29",
		},
		{
			name:      "invalid days parameter",
			date:      "2024-01-15",
			days:      "invalid",
			wantError: true,
		},
		{
			name:      "invalid date format",
			date:      "not-a-date",
			days:      "1",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			query := fmt.Sprintf("{{ '%s' | add_days('%s') }}", tt.date, tt.days)
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(query)

			if tt.wantError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, result)
		})
	}
}

func TestAddHours(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		date      string
		hours     string
		want      string
		wantError bool
	}{
		{
			name:  "add positive hours",
			date:  "2024-01-15 10:30:00",
			hours: "5",
			want:  "2024-01-15 15:30:00",
		},
		{
			name:  "add negative hours",
			date:  "2024-01-15 10:30:00",
			hours: "-3",
			want:  "2024-01-15 07:30:00",
		},
		{
			name:  "add zero hours",
			date:  "2024-01-15 10:30:00",
			hours: "0",
			want:  "2024-01-15 10:30:00",
		},
		{
			name:  "cross day boundary forward",
			date:  "2024-01-15 23:30:00",
			hours: "2",
			want:  "2024-01-16 01:30:00",
		},
		{
			name:  "cross day boundary backward",
			date:  "2024-01-15 01:30:00",
			hours: "-2",
			want:  "2024-01-14 23:30:00",
		},
		{
			name:  "cross month boundary",
			date:  "2024-01-31 23:30:00",
			hours: "2",
			want:  "2024-02-01 01:30:00",
		},
		{
			name:  "cross year boundary",
			date:  "2023-12-31 23:30:00",
			hours: "2",
			want:  "2024-01-01 01:30:00",
		},
		{
			name:      "invalid hours parameter",
			date:      "2024-01-15 10:30:00",
			hours:     "invalid",
			wantError: true,
		},
		{
			name:      "invalid date format",
			date:      "not-a-date",
			hours:     "1",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			query := fmt.Sprintf("{{ '%s' | add_hours('%s') }}", tt.date, tt.hours)
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(query)

			if tt.wantError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, result)
		})
	}
}

func TestAddMinutes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		date      string
		minutes   string
		want      string
		wantError bool
	}{
		{
			name:    "add positive minutes",
			date:    "2024-01-15 10:30:00",
			minutes: "15",
			want:    "2024-01-15 10:45:00",
		},
		{
			name:    "add negative minutes",
			date:    "2024-01-15 10:30:00",
			minutes: "-20",
			want:    "2024-01-15 10:10:00",
		},
		{
			name:    "add zero minutes",
			date:    "2024-01-15 10:30:00",
			minutes: "0",
			want:    "2024-01-15 10:30:00",
		},
		{
			name:    "cross hour boundary forward",
			date:    "2024-01-15 10:50:00",
			minutes: "15",
			want:    "2024-01-15 11:05:00",
		},
		{
			name:    "cross hour boundary backward",
			date:    "2024-01-15 10:05:00",
			minutes: "-10",
			want:    "2024-01-15 09:55:00",
		},
		{
			name:    "cross day boundary",
			date:    "2024-01-15 23:55:00",
			minutes: "10",
			want:    "2024-01-16 00:05:00",
		},
		{
			name:    "cross month boundary",
			date:    "2024-01-31 23:55:00",
			minutes: "10",
			want:    "2024-02-01 00:05:00",
		},
		{
			name:    "cross year boundary",
			date:    "2023-12-31 23:55:00",
			minutes: "10",
			want:    "2024-01-01 00:05:00",
		},
		{
			name:      "invalid minutes parameter",
			date:      "2024-01-15 10:30:00",
			minutes:   "invalid",
			wantError: true,
		},
		{
			name:      "invalid date format",
			date:      "not-a-date",
			minutes:   "1",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			query := fmt.Sprintf("{{ '%s' | add_minutes('%s') }}", tt.date, tt.minutes)
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(query)

			if tt.wantError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, result)
		})
	}
}

func TestAddSeconds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		date      string
		seconds   string
		want      string
		wantError bool
	}{
		{
			name:    "add positive seconds",
			date:    "2024-01-15 10:30:15",
			seconds: "30",
			want:    "2024-01-15 10:30:45",
		},
		{
			name:    "add negative seconds",
			date:    "2024-01-15 10:30:45",
			seconds: "-30",
			want:    "2024-01-15 10:30:15",
		},
		{
			name:    "add zero seconds",
			date:    "2024-01-15 10:30:15",
			seconds: "0",
			want:    "2024-01-15 10:30:15",
		},
		{
			name:    "cross minute boundary forward",
			date:    "2024-01-15 10:30:45",
			seconds: "30",
			want:    "2024-01-15 10:31:15",
		},
		{
			name:    "cross minute boundary backward",
			date:    "2024-01-15 10:30:15",
			seconds: "-30",
			want:    "2024-01-15 10:29:45",
		},
		{
			name:    "cross hour boundary",
			date:    "2024-01-15 10:59:45",
			seconds: "30",
			want:    "2024-01-15 11:00:15",
		},
		{
			name:    "cross day boundary",
			date:    "2024-01-15 23:59:45",
			seconds: "30",
			want:    "2024-01-16 00:00:15",
		},
		{
			name:    "cross month boundary",
			date:    "2024-01-31 23:59:45",
			seconds: "30",
			want:    "2024-02-01 00:00:15",
		},
		{
			name:    "cross year boundary",
			date:    "2023-12-31 23:59:45",
			seconds: "30",
			want:    "2024-01-01 00:00:15",
		},
		{
			name:      "invalid seconds parameter",
			date:      "2024-01-15 10:30:15",
			seconds:   "invalid",
			wantError: true,
		},
		{
			name:      "invalid date format",
			date:      "not-a-date",
			seconds:   "1",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			query := fmt.Sprintf("{{ '%s' | add_seconds('%s') }}", tt.date, tt.seconds)
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(query)

			if tt.wantError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, result)
		})
	}
}

func TestAddMilliseconds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		date         string
		milliseconds string
		want         string
		wantError    bool
	}{
		{
			name:         "add positive milliseconds",
			date:         "2024-01-15 10:30:15.500",
			milliseconds: "300",
			want:         "2024-01-15 10:30:15.800",
		},
		{
			name:         "add negative milliseconds",
			date:         "2024-01-15 10:30:15.800",
			milliseconds: "-300",
			want:         "2024-01-15 10:30:15.500",
		},
		{
			name:         "add zero milliseconds",
			date:         "2024-01-15 10:30:15.500",
			milliseconds: "0",
			want:         "2024-01-15 10:30:15.500",
		},
		{
			name:         "cross second boundary forward",
			date:         "2024-01-15 10:30:15.800",
			milliseconds: "300",
			want:         "2024-01-15 10:30:16.100",
		},
		{
			name:         "cross second boundary backward",
			date:         "2024-01-15 10:30:15.200",
			milliseconds: "-300",
			want:         "2024-01-15 10:30:14.900",
		},
		{
			name:         "cross minute boundary",
			date:         "2024-01-15 10:30:59.800",
			milliseconds: "300",
			want:         "2024-01-15 10:31:00.100",
		},
		{
			name:         "cross hour boundary",
			date:         "2024-01-15 10:59:59.800",
			milliseconds: "300",
			want:         "2024-01-15 11:00:00.100",
		},
		{
			name:         "cross day boundary",
			date:         "2024-01-15 23:59:59.800",
			milliseconds: "300",
			want:         "2024-01-16 00:00:00.100",
		},
		{
			name:         "cross month boundary",
			date:         "2024-01-31 23:59:59.800",
			milliseconds: "300",
			want:         "2024-02-01 00:00:00.100",
		},
		{
			name:         "cross year boundary",
			date:         "2023-12-31 23:59:59.800",
			milliseconds: "300",
			want:         "2024-01-01 00:00:00.100",
		},
		{
			name:         "invalid milliseconds parameter",
			date:         "2024-01-15 10:30:15.500",
			milliseconds: "invalid",
			wantError:    true,
		},
		{
			name:         "invalid date format",
			date:         "not-a-date",
			milliseconds: "1",
			wantError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			query := fmt.Sprintf("{{ '%s' | add_milliseconds('%s') }}", tt.date, tt.milliseconds)
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(query)

			if tt.wantError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, result)
		})
	}
}
