package cmd

import (
	"testing"
	"time"
)

func TestDefaultStartEndDateUseUTCYesterday(t *testing.T) {
	now := time.Now().UTC()
	want := now.AddDate(0, 0, -1)

	if defaultStartDate.Year() != want.Year() ||
		defaultStartDate.Month() != want.Month() ||
		defaultStartDate.Day() != want.Day() {
		t.Fatalf("defaultStartDate = %v, want UTC yesterday %v", defaultStartDate, want)
	}
	if defaultStartDate.Hour() != 0 || defaultStartDate.Minute() != 0 || defaultStartDate.Second() != 0 {
		t.Fatalf("defaultStartDate time-of-day = %v, want 00:00:00 UTC", defaultStartDate)
	}
	if defaultEndDate.Year() != want.Year() ||
		defaultEndDate.Month() != want.Month() ||
		defaultEndDate.Day() != want.Day() {
		t.Fatalf("defaultEndDate = %v, want UTC yesterday %v", defaultEndDate, want)
	}
	if defaultEndDate.Hour() != 23 || defaultEndDate.Minute() != 59 || defaultEndDate.Second() != 59 {
		t.Fatalf("defaultEndDate time-of-day = %v, want 23:59:59 UTC", defaultEndDate)
	}
	if defaultStartDate.Location() != time.UTC || defaultEndDate.Location() != time.UTC {
		t.Fatalf("defaults must be UTC: start=%v end=%v", defaultStartDate.Location(), defaultEndDate.Location())
	}
}
