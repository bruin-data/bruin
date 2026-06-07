package manifold

import "testing"

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		QueryParams: map[string]string{
			"term": "bitcoin",
		},
	})
	if err != nil {
		t.Fatalf("NewClient() returned error: %v", err)
	}

	got, err := client.GetIngestrURI()
	if err != nil {
		t.Fatalf("GetIngestrURI() returned error: %v", err)
	}

	want := "manifold://?term=bitcoin"
	if got != want {
		t.Errorf("GetIngestrURI() = %v, want %v", got, want)
	}
}
