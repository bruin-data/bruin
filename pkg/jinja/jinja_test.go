package jinja

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
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
		{
			name:  "reverse range",
			query: "{% for i in range(10, 1, -2) %}{{i}}-{%endfor%}",
			args:  Context{},
			want:  "10-8-6-4-2-",
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

			receiver := NewRendererWithStartEndDates(&startDate, &endDate, "your-pipeline-name", "your-run-id", nil)
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

func TestRenderer_CloneForAsset_IntervalModifierTemplates(t *testing.T) {
	t.Parallel()

	basePipeline := &pipeline.Pipeline{
		Name: "test-pipeline",
		Variables: pipeline.Variables{
			"env": map[string]any{
				"test_var": "test_value",
			},
		},
	}

	tests := []struct {
		name              string
		asset             *pipeline.Asset
		endDate           time.Time
		startDate         time.Time
		wantErr           bool
		wantErrMsg        string
		description       string
		expectedStartDate string
		expectedEndDate   string
	}{
		{
			name: "thirty_days_back_template",
			asset: &pipeline.Asset{
				Name: "thirty-days-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{{ '-30d' }}",
					},
					End: pipeline.TimeModifier{
						Template: "{{ '-1d' }}",
					},
				},
			},
			endDate:           time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			startDate:         time.Date(2023, 12, 2, 0, 0, 0, 0, time.UTC),
			description:       "30 days back template should resolve correctly",
			expectedStartDate: "2023-11-02", // startDate + 30 days back = 2023-11-02
			expectedEndDate:   "2024-01-01", // endDate - 1 day = 2024-01-01
		},
		{
			name: "two_hours_back_template",
			asset: &pipeline.Asset{
				Name: "two-hours-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{{ '-2h' }}",
					},
					End: pipeline.TimeModifier{
						Template: "{{ '-1d' }}",
					},
				},
			},
			endDate:           time.Date(2024, 1, 2, 14, 30, 0, 0, time.UTC),
			startDate:         time.Date(2023, 12, 2, 14, 30, 0, 0, time.UTC),
			description:       "2 hours back template should resolve correctly",
			expectedStartDate: "2023-12-02", // startDate + 2 hours back = 2023-12-02 12:30:00
			expectedEndDate:   "2024-01-01", // endDate - 1 day = 2024-01-01
		},
		{
			name: "conditional_truncate_day_template",
			asset: &pipeline.Asset{
				Name: "conditional-truncate-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{% if end_datetime == (end_datetime | truncate_day) %}-30d{% else %}-2h{% endif %}",
					},
					End: pipeline.TimeModifier{
						Template: "{{ '-1d' }}",
					},
				},
			},
			endDate:           time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), // Midnight
			startDate:         time.Date(2023, 12, 2, 0, 0, 0, 0, time.UTC),
			description:       "conditional template with truncate_day should use 30d for midnight",
			expectedStartDate: "2023-11-02", // startDate + 30 days back (midnight case)
			expectedEndDate:   "2024-01-01", // endDate - 1 day = 2024-01-01
		},
		{
			name: "conditional_truncate_day_template_afternoon",
			asset: &pipeline.Asset{
				Name: "conditional-truncate-afternoon-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{% if end_datetime == (end_datetime | truncate_day) %}-30d{% else %}-2h{% endif %}",
					},
					End: pipeline.TimeModifier{
						Template: "{{ '-1d' }}",
					},
				},
			},
			endDate:           time.Date(2024, 1, 2, 14, 30, 0, 0, time.UTC), // Afternoon
			startDate:         time.Date(2023, 12, 2, 14, 30, 0, 0, time.UTC),
			description:       "conditional template with truncate_day should use 2h for afternoon",
			expectedStartDate: "2023-12-02", // startDate + 2 hours back (afternoon case)
			expectedEndDate:   "2024-01-01", // endDate - 1 day = 2024-01-01
		},
		{
			name: "simple_static_templates",
			asset: &pipeline.Asset{
				Name: "simple-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{{ '-6h' }}",
					},
					End: pipeline.TimeModifier{
						Template: "{{ '+1h' }}",
					},
				},
			},
			endDate:           time.Date(2024, 1, 2, 14, 30, 0, 0, time.UTC),
			startDate:         time.Date(2024, 1, 1, 14, 30, 0, 0, time.UTC),
			description:       "simple static templates should resolve correctly",
			expectedStartDate: "2024-01-01", // startDate + 6 hours back = 2024-01-01 08:30:00
			expectedEndDate:   "2024-01-02", // endDate + 1 hour = 2024-01-02 15:30:00
		},
		{
			name: "variable_based_templates",
			asset: &pipeline.Asset{
				Name: "variable-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{{ '-12h' }}",
					},
					End: pipeline.TimeModifier{
						Template: "{{ '30m' }}",
					},
				},
			},
			endDate:           time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC),
			startDate:         time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			description:       "templates using simple time values should work",
			expectedStartDate: "2024-01-01", // startDate + 12 hours back = 2024-01-01 00:00:00
			expectedEndDate:   "2024-01-02", // endDate + 30 minutes = 2024-01-02 12:30:00
		},
		{
			name: "complex_conditional_templates",
			asset: &pipeline.Asset{
				Name: "complex-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{{ '-12h' }}",
					},
					End: pipeline.TimeModifier{
						Template: "{{ '+1h' }}",
					},
				},
			},
			endDate:           time.Date(2024, 1, 2, 15, 0, 0, 0, time.UTC),
			startDate:         time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC),
			description:       "complex conditional templates with multiple conditions",
			expectedStartDate: "2024-01-01", // startDate + 12 hours back = 2024-01-01 03:00:00
			expectedEndDate:   "2024-01-02", // endDate + 1 hour = 2024-01-02 16:00:00
		},
		{
			name: "invalid_template_syntax",
			asset: &pipeline.Asset{
				Name: "invalid-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{% invalid syntax %}",
					},
				},
			},
			endDate:     time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC),
			startDate:   time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			wantErr:     true,
			wantErrMsg:  "failed to resolve start interval modifier template",
			description: "invalid template syntax should return error",
		},
		{
			name: "template_renders_invalid_format",
			asset: &pipeline.Asset{
				Name: "invalid-format-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{{ 'invalid-format' }}",
					},
				},
			},
			endDate:     time.Date(2024, 1, 2, 16, 0, 0, 0, time.UTC),
			startDate:   time.Date(2024, 1, 1, 16, 0, 0, 0, time.UTC),
			wantErr:     true,
			wantErrMsg:  "failed to resolve start interval modifier template",
			description: "template rendering invalid time format should return error",
		},
		{
			name: "no_interval_modifiers",
			asset: &pipeline.Asset{
				Name: "no-modifiers-asset",
			},
			endDate:           time.Date(2024, 1, 2, 18, 0, 0, 0, time.UTC),
			startDate:         time.Date(2024, 1, 1, 18, 0, 0, 0, time.UTC),
			description:       "asset without interval modifiers should work normally",
			expectedStartDate: "2024-01-01", // No modifiers applied, original start date
			expectedEndDate:   "2024-01-02", // No modifiers applied, original end date
		},
		{
			name: "business_hours_logic",
			asset: &pipeline.Asset{
				Name: "business-hours-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{% if end_datetime >= '2024-01-02T09:00:00' and end_datetime <= '2024-01-02T17:00:00' %}-6h{% else %}-12h{% endif %}",
					},
					End: pipeline.TimeModifier{
						Template: "{% if end_datetime >= '2024-01-02T09:00:00' and end_datetime <= '2024-01-02T17:00:00' %}-1h{% else %}-3h{% endif %}",
					},
				},
			},
			endDate:           time.Date(2024, 1, 2, 14, 30, 0, 0, time.UTC), // Business hours (2:30 PM)
			startDate:         time.Date(2024, 1, 1, 14, 30, 0, 0, time.UTC),
			description:       "business hours should use shorter intervals",
			expectedStartDate: "2024-01-01", // startDate + 6h back = 2024-01-01 08:30:00
			expectedEndDate:   "2024-01-02", // endDate + 1h back = 2024-01-02 13:30:00
		},
		{
			name: "month_end_logic",
			asset: &pipeline.Asset{
				Name: "month-end-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{% if end_date >= '2024-01-25' %}-7d{% else %}-1d{% endif %}",
					},
					End: pipeline.TimeModifier{
						Template: "{% if end_date >= '2024-01-25' %}-1d{% else %}-2h{% endif %}",
					},
				},
			},
			endDate:           time.Date(2024, 1, 31, 10, 0, 0, 0, time.UTC), // Month end (day 31)
			startDate:         time.Date(2024, 1, 30, 10, 0, 0, 0, time.UTC),
			description:       "month end should use longer lookback",
			expectedStartDate: "2024-01-23", // startDate + 7d back = 2024-01-23 10:00:00
			expectedEndDate:   "2024-01-30", // endDate + 1d back = 2024-01-30 10:00:00
		},
		{
			name: "variable_based_conditional",
			asset: &pipeline.Asset{
				Name: "variable-conditional-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{% if end_datetime >= '2024-01-02T12:00:00' %}-24h{% else %}-6h{% endif %}",
					},
					End: pipeline.TimeModifier{
						Template: "{% if end_datetime >= '2024-01-02T12:00:00' %}-2h{% else %}-1h{% endif %}",
					},
				},
			},
			endDate:           time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC),
			startDate:         time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			description:       "afternoon time should use longer intervals",
			expectedStartDate: "2023-12-31", // startDate + 24h back (afternoon) = 2023-12-31 12:00:00
			expectedEndDate:   "2024-01-02", // endDate + 2h back (afternoon) = 2024-01-02 10:00:00
		},
		{
			name: "complex_time_based_logic",
			asset: &pipeline.Asset{
				Name: "complex-time-asset",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{
						Template: "{% if end_datetime < '2024-01-02T06:00:00' %}-36h{% elif end_datetime < '2024-01-02T12:00:00' %}-24h{% elif end_datetime < '2024-01-02T18:00:00' %}-12h{% else %}-6h{% endif %}",
					},
					End: pipeline.TimeModifier{
						Template: "{% if end_datetime < '2024-01-02T06:00:00' %}-6h{% elif end_datetime < '2024-01-02T12:00:00' %}-3h{% elif end_datetime < '2024-01-02T18:00:00' %}-2h{% else %}-1h{% endif %}",
					},
				},
			},
			endDate:           time.Date(2024, 1, 2, 3, 0, 0, 0, time.UTC), // Early morning (3 AM)
			startDate:         time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC),
			description:       "early morning should use longest intervals",
			expectedStartDate: "2023-12-30", // startDate + 36h back = 2023-12-30 15:00:00
			expectedEndDate:   "2024-01-01", // endDate + 6h back = 2024-01-01 21:00:00
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, tt.startDate)
			ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, tt.endDate)
			ctx = context.WithValue(ctx, pipeline.RunConfigApplyIntervalModifiers, true)
			ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run-id")

			baseRenderer := NewRenderer(Context{})
			clonedRenderer, err := baseRenderer.CloneForAsset(ctx, basePipeline, tt.asset)

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, clonedRenderer)

			// Test that the cloned renderer brings the correct asset name
			testQuery := "SELECT '{{ this }}' as asset_name"
			result, err := clonedRenderer.Render(testQuery)
			require.NoError(t, err)
			require.Equal(t, "SELECT '"+tt.asset.Name+"' as asset_name", result)

			// Test that interval modifiers were applied correctly by checking the dates
			dateQuery := "SELECT '{{ start_date }}' as start_date, '{{ end_date }}' as end_date"
			dateResult, err := clonedRenderer.Render(dateQuery)
			require.NoError(t, err)
			require.Contains(t, dateResult, tt.expectedStartDate)
			require.Contains(t, dateResult, tt.expectedEndDate)
		})
	}
}

func TestRenderer_IsFullRefresh(t *testing.T) {
	t.Parallel()

	startDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	basePipeline := &pipeline.Pipeline{
		Name: "test-pipeline",
		Variables: pipeline.Variables{
			"env": map[string]any{
				"test_var": "test_value",
			},
		},
	}

	asset := &pipeline.Asset{
		Name: "test-asset",
	}

	tests := []struct {
		name                 string
		fullRefresh          bool
		expectedRenderedText string
	}{
		{
			name:                 "is_full_refresh is false when full refresh is disabled",
			fullRefresh:          false,
			expectedRenderedText: "False",
		},
		{
			name:                 "is_full_refresh is true when full refresh is enabled",
			fullRefresh:          true,
			expectedRenderedText: "True",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, startDate)
			ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, endDate)
			ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run-id")
			ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, tt.fullRefresh)

			baseRenderer := NewRendererWithStartEndDates(&startDate, &endDate, basePipeline.Name, "test-run-id", basePipeline.Variables.Value())
			clonedRenderer, err := baseRenderer.CloneForAsset(ctx, basePipeline, asset)
			require.NoError(t, err)
			require.NotNil(t, clonedRenderer)

			// Test that is_full_refresh variable is set correctly
			result, err := clonedRenderer.Render("{{ is_full_refresh }}")
			require.NoError(t, err)
			require.Equal(t, tt.expectedRenderedText, result)

			// Test using is_full_refresh in a conditional
			conditionalQuery := `{% if is_full_refresh %}full{% else %}incremental{% endif %}`
			conditionalResult, err := clonedRenderer.Render(conditionalQuery)
			require.NoError(t, err)
			if tt.fullRefresh {
				require.Equal(t, "full", conditionalResult)
			} else {
				require.Equal(t, "incremental", conditionalResult)
			}
		})
	}
}

func TestPythonEnvVariables_FullRefresh(t *testing.T) {
	t.Parallel()

	startDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name                      string
		fullRefresh               bool
		expectedFullRefreshEnvVar string
	}{
		{
			name:                      "BRUIN_FULL_REFRESH is empty string when full refresh is disabled",
			fullRefresh:               false,
			expectedFullRefreshEnvVar: "",
		},
		{
			name:                      "BRUIN_FULL_REFRESH is '1' when full refresh is enabled",
			fullRefresh:               true,
			expectedFullRefreshEnvVar: "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			envVars := PythonEnvVariables(&startDate, &endDate, "test-pipeline", "test-run-id", tt.fullRefresh)

			// Verify BRUIN_FULL_REFRESH is set correctly
			require.Equal(t, tt.expectedFullRefreshEnvVar, envVars["BRUIN_FULL_REFRESH"])

			// Verify other required environment variables are still present
			require.Equal(t, "2024-01-01", envVars["BRUIN_START_DATE"])
			require.Equal(t, "2024-01-02", envVars["BRUIN_END_DATE"])
			require.Equal(t, "test-run-id", envVars["BRUIN_RUN_ID"])
			require.Equal(t, "test-pipeline", envVars["BRUIN_PIPELINE"])
			require.Equal(t, "1", envVars["PYTHONUNBUFFERED"])
		})
	}
}
