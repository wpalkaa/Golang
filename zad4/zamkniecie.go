package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const WeatherStep = 5 * time.Millisecond      // ~5 minut czasu
const GridStep = 100 * time.Millisecond       // 1 godzina czasu
const WeatherPerGrid = GridStep / WeatherStep // = 12 kroków po
const ForecastHorizon = 5                     // prognoza na 5 kroków GridStep
const PredictorBufferSize = WeatherPerGrid    // bufor = 1 godzin

type WeatherData struct {
	Wind float64
	Sun  float64
}

type ForecastData struct {
	n               int
	predictedPower  float64
	predictedChange float64
}

type DemandReport struct {
	Id       string
	Demand   float64
	Priority int
}

type SupplyStatus struct {
	Allocatedpower float64
}

type DataLogger interface {
	Run(ctx context.Context)
	Log(mess string)
	Flush()
}

type ConsumerInt interface {
	Run(ctx context.Context, wg *sync.WaitGroup, demandChan chan<- DemandReport)
}

type EnergyStorage interface {
	Charge(value float64) float64
	Discharge(value float64) float64
	GetSoC() float64
}

type EnergySource interface {
	Run(ctx context.Context, wg *sync.WaitGroup)
	SetLimit(limit float64)
	GetPower() float64
}

type Predictor interface {
	Run(ctx context.Context, wg *sync.WaitGroup, weatherDataChan <-chan WeatherData, forecastChan chan<- ForecastData)
}

type WeatherProvider interface {
	Run(ctx context.Context, wg *sync.WaitGroup)
}

// ============ Logger ============

type Logger struct {
	logChan chan string
}

// implementacja loggera
func CreateLogger() {}

// Run(ctx context.Context)
// Log(mess string)
// Flush()

// ============ WeatherStation ============

type WeatherStation struct {
	broadcaster chan<- WeatherData
}

// funkcja generująca dane pogodowe, wysyłane one są do broadcastera
func (w *WeatherStation) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(WeatherStep)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			// Wylicz to i tamto, wyślij na kanał
			w.broadcaster <- WeatherData{ /*...*/ }
		}
	}
}

// ============ Broadcaster ============

type Broadcaster struct {
	subcribers []chan<- WeatherData
	m          sync.Mutex
}

func (b *Broadcaster) fun(ctx context.Context, wg *sync.WaitGroup, ch <-chan WeatherData) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return

		case data := <-ch:
			//przyjmuje dane, rozgłoś z użyciem instrukcji select z klauzulą default,
			// porzuca paczkę dla danego subskrybenta, jeśli ten jest zajęty i nie odbiera —
			// dzięki temu wolny subskrybent nie blokuje całej sieci
		}
	}
	/*
		// Szkielet logiki Broadcastera
		for _, ch := range subscribers {
		 select {
		 case ch <- weatherData:
		 default:
		 // subskrybent zajęty — porzucamy paczkę dla niego
		 }
		}
	*/
}

// ============ Predictor ============

type WeatherPredictor struct {
	buffer []WeatherData // ma byc slice 12 odczytów - 60min danych
}

func (p *WeatherPredictor) Run(ctx context.Context, wg *sync.WaitGroup, weatherDataChan <-chan WeatherData, forecastChan chan<- ForecastData) {
	defer wg.Done()
	gridTicker := time.NewTicker(GridStep)
	defer gridTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case data := <-weatherDataChan:
			p.buffer = append(p.buffer, data)
			if len(p.buffer) > 12 {
				p.buffer = p.buffer[1:]
			}

		case <-gridTicker.C:
			// Licz ForecastReport i wyślij do gridHub
		}
	}
}

// ============ Konsumenci ============

type ConsumerType string

const (
	Residental ConsumerType = "residental"
	Industrial ConsumerType = "industrial"
	Critical   ConsumerType = "critical"
)

type Consumer struct {
	Id       int
	Type     ConsumerType
	Priority int
}

func (c *Consumer) fun(ctx context.Context, wg *sync.WaitGroup, demandChan chan<- DemandReport, respChan <-chan SupplyStatus, c Consumer) {
	defer wg.Done()
	ticker := ticker.NewTimer(GridStep)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			// 1. Oblicz aktualne zapotrzebowanie P_demand (na podstawie czasu symu
			// 2. Wyślij DemandReport{ID, P_demand, Priority} do GridHub przez wspó
			// 3. Czekaj na SupplyStatus z kanału zwrotnego
			// 4. Jeśli SupplyStatus.AllocatedMW < P_demand → przejdź w stan Partia
			// 5. Zgłoś zdarzenie do modułu statystyk
		}
	}
}

// ============ ESS ============
type ESS struct {
	capacity float64
	current  float64
}

func (ess *ESS) Charge(value float64) float64 {
	// ładuj
}

func (ess *ESS) Discharge(value float64) float64 {
	// odładuj
}

func (ess *ESS) GetSoC() float64 {
	return ess.current / ess.capacity
}

// ============ GridHub ============

type GridHub struct {
	forecastChan <-chan ForecastData
	demandChan   <-chan DemandReport
	consumers    []Consumer
	logger       Datalogger
}

func (gh *GridHub) fun(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(GridStep)
	defer ticker.Stop()

	demandsMap := map[string]DemandReport{}

	// bilans = łączna_produkcja + ESS.Discharge() < łączny_popyt,

	for {
		select {
		case <-ctx.Done():
			return

		case <-gh.forecastChan:
			// porównaj prognozę z planowanym zapotrzebowaniem
			// if < OZE, sprawdź status elektrowni węglowej
			//     if eWęglowa OFF, eWęglowa.Start()

		case d := <-gh.demandChan:
			demandsMap[d.Id] = d

		case <-ticker.C:
			// Oblicz różnicę między produkcją a zgłoszonym popytem

			// Sprawdź magazyn ESS:
			// Nadwyżka produkcji, SoC < 100% => Wysyła energię do ESS (Charge)
			// Nadwyżka produkcji, SoC = 100% => Wysyła sygnał do OZE o ograniczeniu mocy (Curtailment)
			// Niedobór produkcji, SoC > 0% => Rozładowuje ESS (Discharge)
			// Niedobór produkcji, SoC = 0% => Uruchamia procedurę Load Shedding

			// If bilans < 0, baterie puste, eWęglowa wciąż startuje:

			// 1. Pobierz listę wszystkich aktywnych konsumentów posortowaną według
			// priorytetu (od najniższego: Residential → Industrial → Critical).
			// 2. Odłączaj kolejnych konsumentów (od najniższego priorytetu), aż bilans wróci do
			// zera lub lista się wyczerpie.
			// 3. Wyślij odłączonemu konsumentowi SupplyStatus{AllocatedMW: 0,
			// Reason: "LoadShed"}.
			// 4. Stan odłączenia utrzymuje się do następnej iteracji tickera bilansującego —
			// konsument może wrócić do sieci automatycznie, gdy bilans się poprawi.
			// 5. Każde zdarzenie odłączenia jest natychmiast rejestrowane w module statystyk.
		}
	}
}

func main() {
	// 1. Tworzymy kontekst, który można anulować
	ctx, cancel := context.WithCancel(context.Background())

	// 2. Kanał do nasłuchiwania sygnałów OS
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// 3. Gorutyna czekająca na Ctrl+C
	go func() {
		<-sigChan
		fmt.Println("\n[SYSTEM] Otrzymano sygnał przerwania. Zamykanie...")
		cancel() // Wysyła sygnał ctx.Done() do wszystkich gorutyn
	}()

	var wg sync.WaitGroup

	// Uruchamianie komponentów

	// ... reszta komponentów ...

	// 4. Czekamy na zakończenie wszystkich gorutyn
	wg.Wait()
	fmt.Println("[SYSTEM] Wszystkie komponenty zamknięte. Koniec.")
}
