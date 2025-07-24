package influxdb

import "testing"

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()
	c := Config{
		Host:   "localhost",
		Port:   8086,
		Token:  "my-token",
		Org:    "my-org",
		Bucket: "metrics",
		Secure: "false",
	}
	want := "influxdb://localhost:8086?bucket=metrics&org=my-org&secure=false&token=my-token"
	if got := c.GetIngestrURI(); got != want {
		t.Errorf("expected %s, got %s", want, got)
	}
}
