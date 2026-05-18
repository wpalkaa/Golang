package main

import (
	"context"
	"fmt"
	"math/rand/v2"
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
const WindMax = 40.0
const SunMax = 100
const WINDPOWER = 3.0
const SUNPOWER = 2.0

type WeatherData struct {
	WindSpeed    float64
	SunIntensity float64
}

type ForecastData struct {
	// n               int
	predictedPower float64
	// predictedChange float64
}

type DemandReport struct {
	Id       string
	Demand   float64
	Priority int
	RespChan chan SupplyStatus
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
	logChan <-chan string
}

// implementacja loggera
func CreateLogger() {}

// Run(ctx context.Context)
// Log(mess string)
// Flush()

// ============ Broadcaster ============

type Broadcaster struct {
	subcribers []chan<- WeatherData
	mutex      sync.Mutex
}

func (b *Broadcaster) Subscribe() chan WeatherData {
	ch := make(chan WeatherData)

	b.mutex.Lock()
	b.subcribers = append(b.subcribers, ch)
	b.mutex.Unlock()

	return ch
}

func (b *Broadcaster) Send(data WeatherData) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for _, ch := range b.subcribers {
		select {
		case ch <- data:
		default:
		}
	}
}

// ============ WeatherStation ============

type WeatherStation struct {
	windSpeed float64
	sun       float64
}

func NewWeatherStation() *WeatherStation {
	return &WeatherStation{}
}

func (ws *WeatherStation) Run(ctx context.Context, wg *sync.WaitGroup, broadcast *Broadcaster) {
	defer wg.Done()
	ticker := time.NewTicker(WeatherStep)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			ws.windSpeed += rand.Float64()*2 - 1
			ws.sun += rand.Float64()*10 - 10

			if ws.windSpeed < 0.0 {
				ws.windSpeed = 0.0
			}
			if ws.windSpeed > WindMax {
				ws.windSpeed = 40.0
			}
			if ws.sun < 0 {
				ws.sun = 0
			}
			if ws.sun > SunMax {
				ws.sun = 100
			}

			broadcast.Send(WeatherData{WindSpeed: ws.windSpeed, SunIntensity: ws.sun})
		}
	}
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
			first := p.buffer[0]
			last := p.buffer[len(p.buffer)-1]

			trendWind := last.WindSpeed - first.WindSpeed
			trendSun := last.SunIntensity - first.SunIntensity

			predictedPower := (trendWind / WindMax * WINDPOWER) + (float64(trendSun)/SunMax)*SUNPOWER

			select {
			case forecastChan <- ForecastData{predictedPower: predictedPower}:
			default:
			}
		}
	}
}

// ============ OZE ============
type OZE struct {
	output   float64
	mu       sync.Mutex
	capacity float64
	limit    float64
}

func (o *OZE) SetLimit(limit float64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if limit <= 0.0 {
		o.limit = 1.0
	}
	if limit > 100.0 {
		o.limit = 100.0
	}
}

func (o *OZE) GetPower() float64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.output
}

func (o *OZE) Run(ctx context.Context, wg *sync.WaitGroup, weatherChan <-chan WeatherData) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-weatherChan:
			o.mu.Lock()
			o.output = (o.capacity + (data.WindSpeed/WindMax)*WINDPOWER + (data.SunIntensity/SunMax)*SUNPOWER) * o.limit
			o.mu.Unlock()
		}
	}
}

// ============ CoalPlant ============
type PlantState int

const (
	PlantOff PlantState = iota
	PlantWarmingUp
	PlantOn
)

type CoalPlant struct {
	mu          sync.Mutex
	output      float64
	state       PlantState
	warmingTime int
}

func (cp *CoalPlant) Run() {
	cp.mu.Lock()
	if cp.state != PlantOff {
		cp.mu.Unlock()
		return
	}

	cp.state = PlantWarmingUp
	cp.mu.Unlock()

	go func() {
		time.Sleep(GridStep * time.Duration(cp.warmingTime))

		cp.mu.Lock()
		cp.state = PlantOn
		cp.output = 200.0
		cp.mu.Unlock()
	}()
}

func (cp *CoalPlant) GetPower() float64 {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.state == PlantOn {
		return cp.output
	}
	return 0
}

// ============ Konsumenci ============

// ---- residental
type ResidentalConsumer struct {
	id         string
	baseDemand float64
}

func (r *ResidentalConsumer) GetPriority() int { return 3 }
func (r *ResidentalConsumer) GetID() string    { return r.id }

func (r *ResidentalConsumer) calcDemand(hour int) float64 {
	if (hour >= 7 && hour <= 9) || (hour >= 18 && hour <= 22) {
		return r.baseDemand * 2
	}
	return r.baseDemand
}

func (r *ResidentalConsumer) Run(ctx context.Context, wg *sync.WaitGroup, demandChan chan<- DemandReport) {
	defer wg.Done()
	ticker := time.NewTicker(GridStep)
	defer ticker.Stop()
	respChan := make(chan SupplyStatus, 1)

	step := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			step++
			hour := step % 24
			demand := r.calcDemand(hour)

			report := DemandReport{
				Id:       r.id,
				Demand:   demand,
				Priority: r.GetPriority(),
				RespChan: respChan,
			}

			demandChan <- report

			select {
			case <-respChan:
			case <-ctx.Done():
				return
			}
		}
	}
}

// ------Industrial
type IndustrialConsumer struct {
	id         string
	baseDemand float64
}

func (r *IndustrialConsumer) GetPriority() int { return 2 }
func (r *IndustrialConsumer) GetID() string    { return r.id }

func (r *IndustrialConsumer) calcDemand(hour int) float64 {
	if hour >= 6 && hour <= 18 {
		demand := r.baseDemand * 3
		if rand.Float64() < 0.1 {
			demand += 50
		}
		return demand
	}
	return r.baseDemand
}

func (r *IndustrialConsumer) Run(ctx context.Context, wg *sync.WaitGroup, demandChan chan<- DemandReport) {
	defer wg.Done()
	ticker := time.NewTicker(GridStep)
	defer ticker.Stop()
	respChan := make(chan SupplyStatus, 1)

	step := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			step++
			hour := step % 24
			demand := r.calcDemand(hour)

			report := DemandReport{
				Id:       r.id,
				Demand:   demand,
				Priority: r.GetPriority(),
				RespChan: respChan,
			}

			demandChan <- report

			select {
			case <-respChan:
			case <-ctx.Done():
				return
			}
		}
	}
}

// ------Critical
type CriticalConsumer struct {
	id         string
	baseDemand float64
}

func (r *CriticalConsumer) GetPriority() int { return 1 }
func (r *CriticalConsumer) GetID() string    { return r.id }

func (r *CriticalConsumer) Run(ctx context.Context, wg *sync.WaitGroup, demandChan chan<- DemandReport) {
	defer wg.Done()
	ticker := time.NewTicker(GridStep)
	defer ticker.Stop()
	respChan := make(chan SupplyStatus, 1)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:

			report := DemandReport{
				Id:       r.id,
				Demand:   r.baseDemand,
				Priority: r.GetPriority(),
				RespChan: respChan,
			}

			demandChan <- report

			select {
			case <-respChan:
			case <-ctx.Done():
				return
			}
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
	oze          *OZE
	coalPlant    *CoalPlant
	ess          *ESS
	forecastChan <-chan ForecastData
	demandChan   <-chan DemandReport
	demands      map[string]DemandReport
	// logChan chan<- Logger
}

func (gh *GridHub) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(GridStep)
	defer ticker.Stop()

	demandsMap := map[string]DemandReport{}
	step := 0
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

	demandChat := make(chan DemandReport, 50)
	forecastChan := make(chan ForecastData, 1)
	loggerChan := make(chan DataLogger, 100)

	// 3. Gorutyna czekająca na Ctrl+C
	go func() {
		<-sigChan
		fmt.Println("\n[SYSTEM] Otrzymano sygnał przerwania. Zamykanie...")
		cancel() // Wysyła sygnał ctx.Done() do wszystkich gorutyn
	}()

	var wg sync.WaitGroup

	// Uruchamianie komponentów
	// go weatherStation.Run(ctx, &wg, broadcaser)

	// ... reszta komponentów ...

	// 4. Czekamy na zakończenie wszystkich gorutyn
	wg.Wait()
	fmt.Println("[SYSTEM] Wszystkie komponenty zamknięte. Koniec.")
}

// Każdy konsumer ma swoją strukturę
// GridHub - synchronizacja => 3 odbiorców wysyła demand i dopiero przelicza gridhub wszystko
