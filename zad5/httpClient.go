package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type Item struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type CreateItemRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type UserAgentTransport struct {
	UserAgent string
	Transport http.RoundTripper // silnik HTTP, posiada RoundTrip(*http.Request) (*http.Response, error), przyjmuje żądanie i zwraca odpowiedx
}

// wykonuje się przed wysłaniem requesta przez klienta do serwera
func (t *UserAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {

	transport := t.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	clonedReq := req.Clone(req.Context())
	clonedReq.Header.Set("User-Agent", t.UserAgent)

	return transport.RoundTrip(clonedReq)
}

type APIClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewAPIClient(baseURL, userAgent string) *APIClient {
	return &APIClient{
		baseURL: baseURL, // adres API
		httpClient: &http.Client{
			Transport: &UserAgentTransport{
				UserAgent: userAgent,
				Transport: http.DefaultTransport,
			},
		},
	}
}

// GET Items
func (c *APIClient) GetItems(ctx context.Context) ([]Item, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/items", nil)

	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var items []Item
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil { // dekoder strumieniowy, io.ReadAll czyta wszystko
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return items, nil
}

// POST Item
func (c *APIClient) CreateItem(ctx context.Context, data CreateItemRequest) (*Item, error) {
	bodyData, err := json.Marshal(data) // zamienia na json

	if err != nil {
		return nil, fmt.Errorf("failed to convert data to json")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/items", bytes.NewReader(bodyData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var createdItem Item
	if err := json.NewDecoder(resp.Body).Decode(&createdItem); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &createdItem, nil
}
