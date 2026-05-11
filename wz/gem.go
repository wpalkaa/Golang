package main

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

// ==========================================
// 1. STAŁE SYMULACJI
// ==========================================
const WeatherStep = 5 * time.Millisecond // 1 krok pogodowy = ~5 minut symulacji
const GridStep = 100 * time.Millisecond  // 1 krok sieciowy = 1 godzina symulacji
const WeatherPerGrid = int(GridStep / WeatherStep)
const ForecastHorizon = 5

// ==========================================
// 2. STRUKTURY KOMUNIKACYJNE
// ==========================================
type WeatherData struct {
	WindSpeed float64
	Sun       float64
}

type ForecastReport struct {
	PredictedRenewablePower float64
	TrendPercentage         float64
	HorizonSteps            int
}

type SupplyStatus struct {
	AllocatedMW float64
	Reason      string // "OK", "LoadShed", "Partial"
}

type DemandReport struct {
	ID           string
	PDemand      float64
	Priority     int // 1 - Critical, 2 - Industrial, 3 - Residential
	ResponseChan chan SupplyStatus
}

// Globalne statystyki zabezpieczone muteksem
type GlobalStats struct {
	mu               sync.Mutex
	TotalLoadSheds   int
	TotalEnergyGiven float64
}

var stats GlobalStats

// ==========================================
// 3. INTERFEJSY
// ==========================================
type EnergySource interface {
	Start(ctx context.Context, wg *sync.WaitGroup)
	GetPower() float64
	SetCurtailment(limit float64)
}

type Predictor interface {
	Start(ctx context.Context, wg *sync.WaitGroup, weatherSubChan <-chan WeatherData, forecastChan chan<- ForecastReport)
}

type Consumer interface {
	Start(ctx context.Context, wg *sync.WaitGroup, demandChan chan<- DemandReport)
}

type EnergyStorage interface {
	Charge(power float64) float64
	Discharge(power float64) float64
	GetSoC() float64
}

type DataLogger interface {
	Start(ctx context.Context, wg *sync.WaitGroup)
	LogEvent(event string)
	Flush() error
}

// ==========================================
// 4. IMPLEMENTACJE KOMPONENTÓW
// ==========================================

// --- STACJA POGODOWA ---
type WeatherStation struct {
	outChan chan<- WeatherData
	wind    float64
	sun     float64
}

func (w *WeatherStation) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(WeatherStep)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Płynna zmiana warunków pogodowych: Vt+1 = Vt + random(-1, 1)
			w.wind += (rand.Float64()*2 - 1) * 2.0
			w.sun += (rand.Float64()*2 - 1) * 5.0

			// Ograniczenia fizyczne
			if w.wind < 0 {
				w.wind = 0
			}
			if w.sun < 0 {
				w.sun = 0
			}
			if w.sun > 100 {
				w.sun = 100
			}

			w.outChan <- WeatherData{WindSpeed: w.wind, Sun: w.sun}
		}
	}
}

// --- BROADCASTER (Pub/Sub) ---
type Broadcaster struct {
	inChan      <-chan WeatherData
	subscribers []chan WeatherData
}

func (b *Broadcaster) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-b.inChan:
			for _, ch := range b.subscribers {
				select {
				case ch <- data:
				default:
					// Subskrybent zajęty - porzucamy paczkę
				}
			}
		}
	}
}

// --- PREDICTOR ---
type SystemPredictor struct {
	buffer []WeatherData
}

func (p *SystemPredictor) Start(ctx context.Context, wg *sync.WaitGroup, weatherSubChan <-chan WeatherData, forecastChan chan<- ForecastReport) {
	defer wg.Done()
	gridTicker := time.NewTicker(GridStep)
	defer gridTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case data := <-weatherSubChan:
			p.buffer = append(p.buffer, data)
			if len(p.buffer) > WeatherPerGrid {
				p.buffer = p.buffer[1:] // Utrzymujemy historię tylko z 1 godziny (12 kroków)
			}
		case <-gridTicker.C:
			if len(p.buffer) >= 2 {
				oldest := p.buffer[0]
				newest := p.buffer[len(p.buffer)-1]

				// Uproszczony trend słońca
				trend := newest.Sun - oldest.Sun
				predictedOZE := newest.Sun*1.5 + newest.WindSpeed*2.0 // Prosty model OZE

				report := ForecastReport{
					PredictedRenewablePower: predictedOZE + trend,
					TrendPercentage:         trend,
					HorizonSteps:            ForecastHorizon,
				}

				// Wysyłamy do GridHub (nie blokując Predictora)
				select {
				case forecastChan <- report:
				default:
				}
			}
		}
	}
}

// --- MAGAZYN ENERGII (ESS) ---
type Battery struct {
	capacity float64
	current  float64
}

func (b *Battery) Charge(power float64) float64 {
	freeSpace := b.capacity - b.current
	if power <= freeSpace {
		b.current += power
		return 0 // Cała nadwyżka przyjęta
	}
	b.current = b.capacity
	return power - freeSpace // Zwraca nieprzyjętą resztę (wymaga Curtailment)
}

func (b *Battery) Discharge(power float64) float64 {
	if b.current >= power {
		b.current -= power
		return power
	}
	available := b.current
	b.current = 0
	return available // Zwraca ile realnie udało się oddać
}

func (b *Battery) GetSoC() float64 {
	return b.current / b.capacity
}

// --- KONSUMENCI ---
type BaseConsumer struct {
	ID       string
	Priority int
	BasePow  float64
}

func (c *BaseConsumer) Start(ctx context.Context, wg *sync.WaitGroup, demandChan chan<- DemandReport, calcDemand func(int) float64) {
	defer wg.Done()
	ticker := time.NewTicker(GridStep)
	defer ticker.Stop()
	stepCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hour := stepCount % 24
			pDemand := calcDemand(hour)
			respChan := make(chan SupplyStatus, 1)

			// Wyślij żądanie
			demandChan <- DemandReport{
				ID:           c.ID,
				PDemand:      pDemand,
				Priority:     c.Priority,
				ResponseChan: respChan,
			}

			// Czekaj na przydział
			status := <-respChan
			if status.Reason == "LoadShed" {
				stats.mu.Lock()
				stats.TotalLoadSheds++
				stats.mu.Unlock()
			}
			stepCount++
		}
	}
}

// --- DATALOGGER ---
type CSVLogger struct {
	logChan chan string
	writer  *bufio.Writer
	file    *os.File
}

func (l *CSVLogger) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-l.logChan:
			l.writer.WriteString(msg + "\n")
		}
	}
}

func (l *CSVLogger) LogEvent(event string) {
	select {
	case l.logChan <- event:
	default:
	}
}

func (l *CSVLogger) Flush() error {
	err := l.writer.Flush()
	l.file.Close()
	return err
}

// --- GRID HUB ---
type GridHub struct {
	demandChan   <-chan DemandReport
	forecastChan <-chan ForecastReport
	logger       DataLogger
	battery      EnergyStorage
}

func (h *GridHub) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(GridStep)
	defer ticker.Stop()

	// Bufor na bieżące żądania od konsumentów
	currentDemands := make(map[string]DemandReport)

	// Stan sieci
	var ozePower float64 = 50.0 // Początkowa produkcja OZE
	var coalPower float64 = 0.0
	coalWarming := false

	step := 0

	for {
		select {
		case <-ctx.Done():
			return

		case req := <-h.demandChan:
			currentDemands[req.ID] = req

		case forecast := <-h.forecastChan:
			// Zdarzenie 2: Reakcja na prognozę
			if forecast.TrendPercentage < -5.0 && coalPower == 0 && !coalWarming {
				coalWarming = true
				h.logger.LogEvent("PREDICTOR: Ostrzeżenie! Uruchamianie elektrowni węglowej.")
				// Prosta symulacja rozgrzewania - włączamy po 2 godzinach
				go func() {
					time.Sleep(GridStep * 2)
					coalPower = 100.0
					coalWarming = false
				}()
			}
			ozePower = forecast.PredictedRenewablePower // Aktualizujemy produkcję wg "rzeczywistości" Predictora

		case <-ticker.C:
			// Zdarzenie 1 & 3 & 4: Bilansowanie sieci co 1h
			totalDemand := 0.0
			var activeReqs []DemandReport

			for _, req := range currentDemands {
				totalDemand += req.PDemand
				activeReqs = append(activeReqs, req)
			}

			totalProd := ozePower + coalPower
			balance := totalProd - totalDemand
			gridStatus := "STABLE"

			// --- ZARZĄDZANIE ESS i LOAD SHEDDING ---
			if balance > 0 {
				// Nadwyżka -> ładujemy baterie (Zdarzenie 3)
				unallocated := h.battery.Charge(balance)
				if unallocated > 0 {
					h.logger.LogEvent(fmt.Sprintf("CURTAILMENT: Ograniczono OZE o %.2f MW", unallocated))
				}
				// Zaspokajamy wszystkich
				for _, req := range activeReqs {
					req.ResponseChan <- SupplyStatus{AllocatedMW: req.PDemand, Reason: "OK"}
				}

			} else if balance < 0 {
				// Niedobór -> próbujemy rozładować baterię
				needed := -balance
				fromBattery := h.battery.Discharge(needed)
				balance += fromBattery

				if balance < 0 {
					gridStatus = "CRITICAL"
					// Zdarzenie 4: Load Shedding
					// 1. Sortuj malejąco wg Priorytetu (3, 2, 1) -> odłączamy najwyższe liczby (najniższy priorytet)
					sort.Slice(activeReqs, func(i, j int) bool {
						return activeReqs[i].Priority > activeReqs[j].Priority
					})

					deficit := -balance
					for _, req := range activeReqs {
						if deficit > 0 {
							// Odcinamy całkowicie
							req.ResponseChan <- SupplyStatus{AllocatedMW: 0, Reason: "LoadShed"}
							deficit -= req.PDemand
							h.logger.LogEvent(fmt.Sprintf("LOAD SHED: Odłączono %s (Prio: %d, MW: %.2f)", req.ID, req.Priority, req.PDemand))
						} else {
							req.ResponseChan <- SupplyStatus{AllocatedMW: req.PDemand, Reason: "OK"}
						}
					}
				} else {
					// Bateria uratowała sytuację
					for _, req := range activeReqs {
						req.ResponseChan <- SupplyStatus{AllocatedMW: req.PDemand, Reason: "OK"}
					}
				}
			} else {
				// Idealny bilans
				for _, req := range activeReqs {
					req.ResponseChan <- SupplyStatus{AllocatedMW: req.PDemand, Reason: "OK"}
				}
			}

			// Raportowanie co 4 kroki (co 4 symulowane godziny)
			if step%4 == 0 {
				fmt.Printf("[KROK %d] Sieć: OZE=%.0f MW | Konw.=%.0f MW | Bateria SoC=%.0f%% | Popyt=%.0f MW | Status=[%s]\n",
					step, ozePower, coalPower, h.battery.GetSoC()*100, totalDemand, gridStatus)
			}

			// Wyczyść mapę żądań na kolejny krok
			currentDemands = make(map[string]DemandReport)
			step++
		}
	}
}

// ==========================================
// 5. MAIN (ZAMKNIĘCIE I INICJALIZACJA)
// ==========================================
func main() {
	fmt.Println("[SYSTEM] Uruchamianie symulatora sieci energetycznej...")

	// 1. Kontekst graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n[SYSTEM] Otrzymano sygnał przerwania (Ctrl+C). Rozpoczynam zamykanie (graceful shutdown)...")
		cancel()
	}()

	var wg sync.WaitGroup

	// 2. Inicjalizacja kanałów i infrastruktury
	weatherChan := make(chan WeatherData)
	predictorSubChan := make(chan WeatherData, 10) // Subskrybent pogody
	forecastChan := make(chan ForecastReport, 1)   // Kanał predyktora (bufor 1)
	demandChan := make(chan DemandReport, 50)      // Kanał agregacji żądań konsumentów

	// Plik logów
	os.MkdirAll("logs", 0755)
	logFile, _ := os.Create("logs/grid_history.csv")
	logger := &CSVLogger{
		logChan: make(chan string, 100),
		writer:  bufio.NewWriter(logFile),
		file:    logFile,
	}

	// Inicjalizacja ESS
	battery := &Battery{capacity: 200.0, current: 50.0}

	// 3. Uruchamianie Gorutyn (Systemów)

	// Datalogger
	wg.Add(1)
	go logger.Start(ctx, &wg)

	// Stacja Pogodowa
	weatherStation := &WeatherStation{outChan: weatherChan, wind: 10, sun: 50}
	wg.Add(1)
	go weatherStation.Start(ctx, &wg)

	// Broadcaster
	broadcaster := &Broadcaster{
		inChan:      weatherChan,
		subscribers: []chan WeatherData{predictorSubChan},
	}
	wg.Add(1)
	go broadcaster.Start(ctx, &wg)

	// Predictor
	predictor := &SystemPredictor{}
	wg.Add(1)
	go predictor.Start(ctx, &wg, predictorSubChan, forecastChan)

	// GridHub
	hub := &GridHub{
		demandChan:   demandChan,
		forecastChan: forecastChan,
		logger:       logger,
		battery:      battery,
	}
	wg.Add(1)
	go hub.Start(ctx, &wg)

	// Odbiorcy
	// Residential (Priorytet 3) - Szczyty 7-9 i 18-22
	resConsumer := &BaseConsumer{ID: "Res-Osiedle-1", Priority: 3}
	wg.Add(1)
	go resConsumer.Start(ctx, &wg, demandChan, func(h int) float64 {
		if (h >= 7 && h <= 9) || (h >= 18 && h <= 22) {
			return 40.0
		}
		return 15.0
	})

	// Industrial (Priorytet 2) - Praca 6-18
	indConsumer := &BaseConsumer{ID: "Ind-Fabryka-1", Priority: 2}
	wg.Add(1)
	go indConsumer.Start(ctx, &wg, demandChan, func(h int) float64 {
		if h >= 6 && h <= 18 {
			return 80.0 + rand.Float64()*10.0
		}
		return 20.0
	})

	// Critical (Priorytet 1) - Stały profil
	critConsumer := &BaseConsumer{ID: "Crit-Szpital", Priority: 1}
	wg.Add(1)
	go critConsumer.Start(ctx, &wg, demandChan, func(h int) float64 {
		return 10.0
	})

	// 4. Czekamy na sygnał z systemu (wg.Wait() odblokuje się po wciśnięciu Ctrl+C)
	wg.Wait()

	// 5. Gwarancja zapisu plików na koniec
	logger.Flush()

	fmt.Printf("[SYSTEM] Statystyki: Odłączenia (Load Shedding): %d\n", stats.TotalLoadSheds)
	fmt.Println("[SYSTEM] Zapisano logi. Wszystkie komponenty zamknięte. Koniec.")
}
