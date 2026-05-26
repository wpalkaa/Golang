package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAPIClient_SendData_Success(t *testing.T) {
	expectedUserAgent := "Klient/1.0"

	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		// czy dobry klient
		userAgent := req.Header.Get("User-Agent")
		if userAgent != expectedUserAgent {
			t.Errorf("Oczekiwano User-Agent %q, otrzymano %q", expectedUserAgent, userAgent)
		}

		// czy dobry header
		if ct := req.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Oczekiwano Content-Type 'application/json', otrzymano %q", ct)
		}

		res.Header().Set("Content-Type", "application/json")
		res.WriteHeader(http.StatusOK)
		json.NewEncoder(res).Encode(ResponsePayload{
			Message: "success",
			Status:  "ok",
		})
	}))
	defer server.Close()

	// ============ Dzialania na serwerze przez klienta ============
	client := NewAPIClient(server.URL, expectedUserAgent)
	payload := RequestBody{Message: "wysyłam requeścika"}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.SendData(ctx, payload)
	if err != nil {
		t.Fatalf("Powinno zwrócić ok, zwrociło błąd: %v", err)
	}

	if resp.Status != "ok" || resp.Message != "success" {
		t.Errorf("Nie 'ok' lub 'success': %+v", resp)
	}
}

func TestAPIClient_SendData_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewAPIClient(server.URL, "Klient/1.0")

	ctx := context.Background()
	_, err := client.SendData(ctx, RequestBody{Message: "Błąd"})

	if err == nil {
		t.Fatal("Miał być błąd a ni mo")
	}

	expectedErr := "server error, 500"
	if err.Error() != expectedErr {
		t.Errorf("Oczekiwano błędu %q, otrzymano %q", expectedErr, err.Error())
	}
}

func TestAPIClient_SendData_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewAPIClient(server.URL, "Klient/1.0")

	ctx := context.Background()
	_, err := client.SendData(ctx, RequestBody{Message: "Błąd"})

	if err == nil {
		t.Fatal("Miał być błąd a ni mo")
	}

	expectedErr := "not found, 404"
	if err.Error() != expectedErr {
		t.Errorf("Oczekiwano błędu %q, otrzymano %q", expectedErr, err.Error())
	}
}

func TestSendData_Invalid_JSON_in_response(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.Write([]byte("djaiwd"))
	}))
	defer server.Close()

	client := &APIClient{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	}

	_, err := client.SendData(context.Background(), RequestBody{})

	if err == nil {
		t.Fatal("Miał być błąd a ni mo")
	}

	expectedErr := "błąd dekodowania odpowiedzi JSON"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Oczekiwano błędu %q, otrzymano %q", expectedErr, err.Error())
	}
}
