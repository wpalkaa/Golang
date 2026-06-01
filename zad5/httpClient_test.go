package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClient_GetItems(t *testing.T) {
	expectedUserAgent := "Klient"

	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		// czy dobry klient
		userAgent := req.Header.Get("User-Agent")
		if userAgent != expectedUserAgent {
			t.Errorf("expected User-Agent %s, got %s", expectedUserAgent, userAgent)
		}
		// Sprawdzenie metody i endpointu
		if req.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", req.Method)
		}
		if req.URL.Path != "/items" {
			t.Errorf("expected /items, got %s", req.URL.Path)
		}

		res.Header().Set("Content-Type", "application/json")
		res.WriteHeader(http.StatusOK)
		json.NewEncoder(res).Encode([]Item{
			{
				ID:          1,
				Name:        "item",
				Description: "opis itemu",
			},
			{
				ID:          2,
				Name:        "item 2",
				Description: "opis itemu 2",
			},
		})
	}))
	defer server.Close()

	// ============ działania na serwerze przez klienta ============

	client := NewAPIClient(server.URL, expectedUserAgent)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	items, err := client.GetItems(ctx)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].Name != "item" {
		t.Errorf("expected name 'item', got %s", items[0].Name)
	}
}

func TestClient_CreateItem(t *testing.T) {
	expectedUserAgent := "Klient"
	expectedReqBody := `{"name":"New item","description":"new item description"}`

	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", req.Method)
		}
		if req.URL.Path != "/items" {
			t.Errorf("expected /items, got %s", req.URL.Path)
		}
		if req.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", req.Header.Get("Content-Type"))
		}

		bodyBytes, _ := io.ReadAll(req.Body)
		if string(bodyBytes) != expectedReqBody {
			t.Errorf("expected body %s, got %s", expectedReqBody, string(bodyBytes))
		}

		res.Header().Set("Content-Type", "application/json")
		res.WriteHeader(http.StatusCreated)
		json.NewEncoder(res).Encode(Item{
			ID:          3,
			Name:        "New item",
			Description: "new item description",
		})
	}))
	defer server.Close()

	// ============ Dzialania na serwerze przez klienta ============

	client := NewAPIClient(server.URL, expectedUserAgent)
	input := CreateItemRequest{
		Name:        "New item",
		Description: "new item description",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	item, err := client.CreateItem(ctx, input)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if item.ID != 3 {
		t.Errorf("expected ID 3, got %d", item.ID)
	}
}

func TestClient_UnexpectedStatusCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		// Błąd serwera
		res.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewAPIClient(server.URL, "Klient")

	_, err := client.GetItems(context.Background())
	if err == nil {
		t.Fatal("expected error on 500 status code, got nil")
	}
}

func TestClient_Invalid_JSON_in_response(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.Write([]byte("djaiwd"))
	}))
	defer server.Close()

	client := NewAPIClient(server.URL, "Klient")

	_, err := client.GetItems(context.Background())

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	expectedErr := "failed to decode response"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("expected error containing %q, got %q", expectedErr, err.Error())
	}
}
