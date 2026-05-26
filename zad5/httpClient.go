package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type RequestBody struct {
	Message string `json:"message"`
}

type ResponsePayload struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

type customTransport struct {
	UserAgent string
	Transport http.RoundTripper // silnik HTTP, posiada RoundTrip(*http.Request) (*http.Response, error), przyjmuje żądanie i zwraca odpowiedx
}

// wykonuje się przed wysłaniem requesta przez klienta do serwera
func (c *customTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", c.UserAgent)

	transport := c.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	return transport.RoundTrip(req)
}

type APIClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewAPIClient(baseURL, userAgent string) *APIClient {
	return &APIClient{
		BaseURL: baseURL, // adres API
		HTTPClient: &http.Client{
			Transport: &customTransport{
				UserAgent: userAgent,
				Transport: http.DefaultTransport,
			},
		},
	}
}

func (c *APIClient) SendData(ctx context.Context, data RequestBody) (*ResponsePayload, error) {

	// -  Kodowaniem i dekodowaniem JSON. (tutaj kodowanie)
	body, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("nie udało się zmienić danych na JSON: %w", err)
	}

	// - Żądaniami świadomymi kontekstu.
	reqURL := fmt.Sprintf("%s/api/resource", c.BaseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewBuffer(body)) // bufor strumieniowy
	if err != nil {
		return nil, fmt.Errorf("nie udało się stworzyć requesta: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// wykonanie requesta
	res, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("nie udało się wysłać requesta: %w", err)
	}
	defer res.Body.Close()

	// - Jawną obsługą statusów.
	if res.StatusCode == 500 {
		return nil, fmt.Errorf("server error, 500")
	}
	if res.StatusCode == 404 {
		return nil, fmt.Errorf("not found, 404")
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("inne kody, %d", res.StatusCode)
	}

	//  - Kodowaniem i dekodowaniem JSON (tutaj dekodowanie).
	var result ResponsePayload
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil { // dekoder strumieniowy, io.ReadAll czyta wszystko
		return nil, fmt.Errorf("błąd dekodowania odpowiedzi JSON: %w", err)
	}

	return &result, nil
}
