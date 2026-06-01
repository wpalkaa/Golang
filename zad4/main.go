package main

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"sort"
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
const WINDPOWER = 1350.0
const SUNPOWER = 1300.0
const RaportEveryN = 3

type WeatherData struct {
	WindSpeed    float64
	SunIntensity float64
}

type ForecastData struct {
	predictedChange float64
}

type DemandReport struct {
	Id       string
	Demand   float64
	Priority int
	RespChan chan SupplyStatus
}

type SupplyStatus struct {
	AvailablePower float64
	message        string
}

type CoalPlantStatus struct {
	State  PlantState
	Output float64
}

type CoalPlantComm struct {
	Type  string
	Reply chan CoalPlantStatus
}

// ============ Interfejsy ============

type DataLogger interface {
	Run(ctx context.Context, wg *sync.WaitGroup)
	Log(msg string)
	Flush()
}

type Consumer interface {
	Run(ctx context.Context, wg *sync.WaitGroup, demandChan chan<- DemandReport)
}

type EnergyStorage interface {
	Charge(value float64) float64
	Discharge(value float64) float64
	GetSoC() float64
}

type EnergySource interface {
	Run(ctx context.Context, wg *sync.WaitGroup, weatherChan <-chan WeatherData)
	SetLimit(limit float64)
	GetPower() float64
}

type Predictor interface {
	Run(ctx context.Context, wg *sync.WaitGroup, weatherDataChan <-chan WeatherData, forecastChan chan<- ForecastData)
}

type WeatherProvider interface {
	Run(ctx context.Context, wg *sync.WaitGroup, broadcast *Broadcaster)
}

// ============ Logger ============

type Logger struct {
	logChan chan string
	file    *os.File
	writer  *bufio.Writer
	mu      sync.Mutex // Dodajemy Mutex do ochrony bufio.Writer
}

func (l *Logger) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer l.file.Close()
	defer l.Flush()

	flushTicker := time.NewTicker(GridStep * 24)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			l.mu.Lock()
			for {
				select {
				case msg := <-l.logChan:
					l.writer.WriteString(msg + "\n")
				default:
					l.mu.Unlock()
					return
				}
			}
		case msg := <-l.logChan:
			l.mu.Lock()
			l.writer.WriteString(msg + "\n")
			l.mu.Unlock()
		case <-flushTicker.C:
			l.Flush()
		}
	}
}

func (l *Logger) Log(msg string) {
	l.logChan <- msg
}

func (l *Logger) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writer.Flush()
}

// ============ Broadcaster ============

type Broadcaster struct {
	subcribers []chan<- WeatherData
	mutex      sync.Mutex
}

func (b *Broadcaster) Subscribe() chan WeatherData {
	ch := make(chan WeatherData, 8)

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

func (ws *WeatherStation) Run(ctx context.Context, wg *sync.WaitGroup, broadcast *Broadcaster) {
	defer wg.Done()
	ticker := time.NewTicker(WeatherStep)
	ticker2 := time.NewTicker(GridStep)
	defer ticker.Stop()
	defer ticker2.Stop()

	step := 0
	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			ws.windSpeed += rand.Float64()*2 - 1
			ws.sun += rand.Float64()*20 - 10

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
		case <-ticker2.C:
			step++
			if step%RaportEveryN == 0 {
				fmt.Printf("[Pogoda]: Wiatr: %.2f km/h | Słońce: %.2f%%.\n", ws.windSpeed, ws.sun)
			}
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
			if len(p.buffer) < 2 {
				continue
			}
			first := p.buffer[0]
			last := p.buffer[len(p.buffer)-1]

			trendWind := last.WindSpeed - first.WindSpeed
			trendSun := last.SunIntensity - first.SunIntensity

			predictedChange := (trendWind / WindMax * WINDPOWER) + (float64(trendSun)/SunMax)*SUNPOWER

			select {
			case forecastChan <- ForecastData{predictedChange: predictedChange}:
				fmt.Printf("[Predictor]: Zmiana OZE: %+.2f MW (wiatr %+.2f, słońce %+.2f)\n", predictedChange, trendWind, trendSun)
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
	if limit < 0 {
		limit = 0
	}
	o.limit = limit
}

func (o *OZE) GetPower() float64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.output
}

func (o *OZE) GetLimit() float64 {
	return o.limit
}

func (o *OZE) Run(ctx context.Context, wg *sync.WaitGroup, weatherChan <-chan WeatherData) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case data := <-weatherChan:
			basePower := ((data.WindSpeed / WindMax) * WINDPOWER) + ((data.SunIntensity / SunMax) * SUNPOWER)

			o.mu.Lock()
			if basePower > o.limit {
				o.output = o.limit
			} else {
				o.output = basePower
			}
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
	output      float64
	state       PlantState
	warmingTime int
	wg          *sync.WaitGroup
	commChan    <-chan CoalPlantComm
}

func (cp *CoalPlant) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	var warmupTimer <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-cp.commChan:
			if cmd.Type == "START" {
				if cp.state == PlantOff {
					fmt.Println("[CoalPlant]: Włączanie elektrowni węglowej...")
					cp.state = PlantWarmingUp
					warmupTimer = time.After(GridStep * time.Duration(cp.warmingTime))
				}
				if cmd.Reply != nil {
					cmd.Reply <- CoalPlantStatus{State: cp.state, Output: cp.output}
				}
			} else if cmd.Type == "STATUS" {
				if cmd.Reply != nil {
					cmd.Reply <- CoalPlantStatus{State: cp.state, Output: cp.output}
				}
			}
		case <-warmupTimer:
			if cp.state == PlantWarmingUp {
				cp.state = PlantOn
				cp.output = 200.0
				fmt.Println("[CoalPlant]: Elektrownia węglowa włączona.")
			}
			warmupTimer = nil
		}
	}
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

			select {
			case demandChan <- DemandReport{
				Id:       r.id,
				Demand:   r.calcDemand(step % 24),
				Priority: r.GetPriority(),
				RespChan: respChan,
			}:
			case <-ctx.Done():
				return
			}
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
			select {
			case demandChan <- DemandReport{
				Id:       r.id,
				Demand:   r.calcDemand(step % 24),
				Priority: r.GetPriority(),
				RespChan: respChan,
			}:
			case <-ctx.Done():
				return
			}
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
			select {
			case demandChan <- DemandReport{
				Id:       r.id,
				Demand:   r.baseDemand,
				Priority: r.GetPriority(),
				RespChan: respChan,
			}:
			case <-ctx.Done():
				return
			}
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
	mu       sync.Mutex
}

func (ess *ESS) Charge(value float64) float64 {
	ess.mu.Lock()
	defer ess.mu.Unlock()
	space := ess.capacity - ess.current
	if value <= space {
		ess.current += value
		return 0
	}
	ess.current = ess.capacity
	return value - space
}

func (ess *ESS) Discharge(value float64) float64 {
	ess.mu.Lock()
	defer ess.mu.Unlock()
	if ess.current >= value {
		ess.current -= value
		return value
	}
	available := ess.current
	ess.current = 0
	return available
}

func (ess *ESS) GetSoC() float64 {
	ess.mu.Lock()
	defer ess.mu.Unlock()
	return ess.current / ess.capacity
}

func (ess *ESS) GetAvailableEnergy() float64 {
	ess.mu.Lock()
	defer ess.mu.Unlock()
	return ess.current
}

// ============ GridHub ============

type GridHub struct {
	oze          *OZE
	ess          *ESS
	forecastChan <-chan ForecastData
	demandChan   <-chan DemandReport
	demands      map[string]DemandReport
	logger       *Logger
	coalCommChan chan<- CoalPlantComm

	statsMu sync.Mutex
}

func (gh *GridHub) getCoalStatus() CoalPlantStatus {
	replyChan := make(chan CoalPlantStatus, 1)
	gh.coalCommChan <- CoalPlantComm{Type: "STATUS", Reply: replyChan}
	return <-replyChan
}

func (gh *GridHub) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(GridStep)
	defer ticker.Stop()

	gh.demands = make(map[string]DemandReport)
	step := 0
	ozeOutput := 0.0
	coalPlantOutput := 0.0

	gh.logger.Log("hour, ozeOutput, coalPlantOutput, SoC, currentDemand, balance, status")

	for {
		select {
		case <-ctx.Done():
			return

		case forecast := <-gh.forecastChan:
			currentDemand := 0.0
			for _, dem := range gh.demands {
				currentDemand += dem.Demand
			}
			currentOze := gh.oze.GetPower()
			coalStatus := gh.getCoalStatus()
			currentCoal := coalStatus.Output

			predictedOutput := forecast.predictedChange + currentOze + currentCoal
			availableBattery := gh.ess.GetAvailableEnergy()

			if predictedOutput+availableBattery < currentDemand && coalStatus.State == PlantOff {
				fmt.Printf("[GridHub]: Przewidywana produkcja (%.2f MW) nie wystarczy by zaspokoić potrzeby (%.2f MW). Włączanie elektrowni węglowej.\n", predictedOutput, currentDemand)
				gh.coalCommChan <- CoalPlantComm{Type: "START"}
			}

		case d := <-gh.demandChan:
			gh.demands[d.Id] = d

		case <-ticker.C:
			step++
			status := "STABLE"

			ozeOutput = gh.oze.GetPower()
			coalStatus := gh.getCoalStatus()
			coalPlantOutput = coalStatus.Output
			currentLimit := gh.oze.GetLimit()

			currentDemand := 0.0
			for _, dem := range gh.demands {
				currentDemand += dem.Demand
			}

			balance := ozeOutput + coalPlantOutput - currentDemand

			ozeLimit := 99999.9

			if balance >= 0 {
				unallocated := gh.ess.Charge(balance) // ile zostało
				if unallocated > 0 {
					ozeLimit = ozeOutput - unallocated
					if ozeLimit < 0 {
						ozeLimit = 0
					}

					gh.oze.SetLimit(ozeLimit)
					fmt.Printf("[GridHub]: Limit OZE: %.1f MW\n", ozeLimit)
				} else {
					// gh.oze.SetLimit(math.MaxFloat64)
					gh.oze.SetLimit(99999.0)
				}

				for _, dem := range gh.demands {
					dem.RespChan <- SupplyStatus{AvailablePower: dem.Demand, message: "ok"}
				}
			} else {
				status = "CRITICAL"
				// gh.oze.SetLimit(math.MaxFloat64)
				gh.oze.SetLimit(ozeLimit)

				fromESS := gh.ess.Discharge(-balance)
				balance += fromESS

				// Bateria starczy
				if balance > 0 {
					for _, dem := range gh.demands {
						dem.RespChan <- SupplyStatus{AvailablePower: dem.Demand}
					}
					// Bateria nie starczy - jakiś Load Shedding
				} else {
					activeReq := make([]DemandReport, 0, len(gh.demands))

					for _, v := range gh.demands {
						activeReq = append(activeReq, v)
					}

					sort.Slice(activeReq, func(i, j int) bool {
						return activeReq[i].Priority > activeReq[j].Priority
					})

					deficit := -balance
					for _, req := range activeReq {
						if deficit > 0 {
							fmt.Printf("[GridHub - LoadShed]: Odłączam %s (priorytet - %d, %1.f MW)\n", req.Id, req.Priority, req.Demand)
							req.RespChan <- SupplyStatus{AvailablePower: 0, message: "LoadShedding"}
							deficit -= req.Demand
						} else {
							req.RespChan <- SupplyStatus{AvailablePower: req.Demand, message: "ok"}
						}
					}
				}
			}

			gh.statsMu.Lock()
			if step%RaportEveryN == 0 {
				fmt.Printf("[Produkcja] OZE: %.1f MW | Konwencjonalna: %.1f MW | Baterie: %.0f%% (SoC)\n", ozeOutput, coalPlantOutput, gh.ess.GetSoC()*100)
				fmt.Printf("[Sieć]      Popyt: %.1f MW | Bilans: %+.1f MW | Stan: [%s]\n", currentDemand, balance, status)
			}
			gh.statsMu.Unlock()

			gh.logger.Log(fmt.Sprintf("%d, %.1f, %.1f, %.1f,%.0f, %.2f, %.2f, %s", step%24, ozeOutput, coalPlantOutput, currentLimit, gh.ess.GetSoC()*100, currentDemand, balance, status))

			gh.demands = make(map[string]DemandReport)
		}
	}
}

func main() {
	fmt.Println("[SYSTEM] Uruchamianie sieci energetycznej...")
	// 1. Tworzymy kontekst, który można anulować
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Kanał do nasłuchiwania sygnałów OS
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// 3. Gorutyna czekająca na Ctrl+C
	go func() {
		<-sigChan
		fmt.Println("\n[SYSTEM] Otrzymano sygnał przerwania. Zamykanie...")
		cancel() // Wysyła sygnał ctx.Done() do wszystkich gorutyn
	}()

	demandChan := make(chan DemandReport, 50)
	forecastChan := make(chan ForecastData, 1)
	coalCommChan := make(chan CoalPlantComm, 10)

	var wg sync.WaitGroup

	os.Mkdir("logs", 0755)
	logFile, _ := os.Create("logs/grid_history.csv")
	logger := &Logger{
		logChan: make(chan string, 100),
		writer:  bufio.NewWriter(logFile),
		file:    logFile,
	}

	broadcaster := &Broadcaster{subcribers: make([]chan<- WeatherData, 0)}
	weatherStation := &WeatherStation{windSpeed: 20.0, sun: 80.0}
	predictor := &WeatherPredictor{buffer: make([]WeatherData, 0)}
	// oze := &OZE{output: 0.0, limit: math.MaxFloat64}
	oze := &OZE{output: 0.0, limit: 99999.0}

	coalPlant := &CoalPlant{state: PlantOff, warmingTime: 3, wg: &wg, commChan: coalCommChan}
	ess := &ESS{capacity: 500.0, current: 250.0}

	hub := &GridHub{
		oze:          oze,
		ess:          ess,
		forecastChan: forecastChan,
		demandChan:   demandChan,
		logger:       logger,
		coalCommChan: coalCommChan,
	}

	r1 := &ResidentalConsumer{id: "r1", baseDemand: 5.0}
	r2 := &ResidentalConsumer{id: "r2", baseDemand: 8.0}
	r3 := &ResidentalConsumer{id: "r3", baseDemand: 4.0}
	r4 := &ResidentalConsumer{id: "r4", baseDemand: 13.0}

	i1 := &IndustrialConsumer{id: "i1", baseDemand: 50.0}
	i2 := &IndustrialConsumer{id: "i2", baseDemand: 83.0}

	c1 := &CriticalConsumer{id: "c1", baseDemand: 30.0}

	consumers := []Consumer{
		r1, r2, r3, r4,
		i1, i2,
		c1,
	}

	wg.Add(1)
	go logger.Run(ctx, &wg)

	wg.Add(1)
	go oze.Run(ctx, &wg, broadcaster.Subscribe())

	wg.Add(1)
	go weatherStation.Run(ctx, &wg, broadcaster)

	wg.Add(1)
	go predictor.Run(ctx, &wg, broadcaster.Subscribe(), forecastChan)

	wg.Add(1)
	go coalPlant.Run(ctx, &wg)

	wg.Add(1)
	go hub.Run(ctx, &wg)

	for _, c := range consumers {
		wg.Add(1)
		go c.Run(ctx, &wg, demandChan)
	}

	// 4. Czekamy na zakończenie wszystkich gorutyn
	wg.Wait()
	fmt.Println("[SYSTEM] Wszystkie komponenty zamknięte. Koniec.")
}
