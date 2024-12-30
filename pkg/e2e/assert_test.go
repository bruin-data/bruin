package e2e

import (
	"testing"
)

func TestAssertByExitCode(t *testing.T) {
	tests := []struct {
		name    string
		input   *Task
		wantErr bool
	}{
		{"Matching Exit Codes", &Task{Actual: Output{0, "", "", []string{}}, Expected: Output{0, "", "", []string{}}}, false},
		{"Mismatched Exit Codes", &Task{Actual: Output{1, "", "", []string{}}, Expected: Output{0, "", "", []string{}}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AssertByExitCode(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("AssertByExitCode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAssertByOutputString(t *testing.T) {
	tests := []struct {
		name    string
		input   *Task
		wantErr bool
	}{
		{"Matching Output Strings", &Task{
			Actual:   Output{0, "output", "", []string{}},
			Expected: Output{0, "output", "", []string{}},
		}, false},
		{"Mismatched Output Strings", &Task{
			Actual:   Output{0, "output1", "", []string{}},
			Expected: Output{0, "output2", "", []string{}},
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AssertByOutputString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("AssertByOutputString() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAssertByOutputJson(t *testing.T) {
	// Assuming parseJSONOutputs and compareJSON are implemented correctly
	tests := []struct {
		name    string
		input   *Task
		wantErr bool
	}{
		{"Matching JSON Outputs", &Task{
			Actual:   Output{0, `{"key":"value"}`, "", []string{}},
			Expected: Output{0, `{"key":"value"}`, "", []string{}},
		}, false},
		{"Mismatched JSON Outputs", &Task{
			Actual:   Output{0, `{"key":"value1"}`, "", []string{}},
			Expected: Output{0, `{"key":"value2"}`, "", []string{}},
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AssertByOutputJson(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("AssertByOutputJson() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAssertByError(t *testing.T) {
	tests := []struct {
		name    string
		input   *Task
		wantErr bool
	}{
		{"Matching Errors", &Task{
			Actual:   Output{0, "", "error", []string{}},
			Expected: Output{0, "", "error", []string{}},
		}, false},
		{"Mismatched Errors", &Task{
			Actual:   Output{0, "", "error1", []string{}},
			Expected: Output{0, "", "error2", []string{}},
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AssertByError(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("AssertByError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
