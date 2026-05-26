package main

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

const WeatherStep = 5 * time.Millisecond
const GridStep = 100 * time.Millisecond
const WeatherPerGrid = GridStep / WeatherStep
const ForecastHorizon = 5
const PredictorBufferSize = WeatherPerGrid
const WindMax = 40.0
const SunMax = 100.0
const WINDPOWER = 450.0
const SUNPOWER = 300.0
const RaportEveryN = 3

// ─── Struktury komunikacyjne ─────────────────────────────────────────────────

type WeatherData struct {
	WindSpeed    float64
	SunIntensity float64
}

type ForecastData struct {
	predictedChange float64 // delta MW względem aktualnej produkcji
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

// ─── Interfejsy ──────────────────────────────────────────────────────────────

// NAPRAWIONO: dodano metodę Log() do interfejsu DataLogger —
// bez niej GridHub nie mógł pisać do loggera przez interfejs.
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
	// NAPRAWIONO: oryginalna sygnatura Run(*Broadcaster) była niespójna
	// z interfejsem — teraz przyjmuje gotowy kanał (<-chan WeatherData).
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

// ─── Logger ───────────────────────────────────────────────────────────────────
// NAPRAWIONO: oryginalne pole logChan było typu <-chan string (read-only),
// więc nikt nie mógł do niego wysyłać. Teraz jest chan string z publicznym
// API Log(). Konstruktor NewLogger obsługuje też tworzenie katalogu.
// NAPRAWIONO: Run() nie opróżniał kanału po ctx.Done() — ostatnie wpisy
// były gubione. Teraz drain loop wysysa wszystko przed wyjściem.

type Logger struct {
	logChan chan string
	file    *os.File
	writer  *bufio.Writer
}

func NewLogger(path string) (*Logger, error) {
	if err := os.MkdirAll("logs", 0755); err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Logger{
		logChan: make(chan string, 100),
		file:    f,
		writer:  bufio.NewWriter(f),
	}, nil
}

func (l *Logger) Log(msg string) {
	select {
	case l.logChan <- msg:
	default:
	}
}

func (l *Logger) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case msg := <-l.logChan:
			l.writer.WriteString(msg + "\n")
		case <-ctx.Done():
			// opróżnij bufor przed wyjściem
			for {
				select {
				case msg := <-l.logChan:
					l.writer.WriteString(msg + "\n")
				default:
					return
				}
			}
		}
	}
}

func (l *Logger) Flush() {
	l.writer.Flush()
	l.file.Close()
}

// ─── Broadcaster ─────────────────────────────────────────────────────────────
// NAPRAWIONO: Subscribe() tworzyło unbuffered channel — nawet z select/default
// w Send() wolny subskrybent mógłby porzucać każdy pakiet jeśli nie zdąży
// odebrać przed kolejnym Send. Bufor = 8 daje margines bezpieczeństwa.

type Broadcaster struct {
	subscribers []chan<- WeatherData
	mutex       sync.Mutex
}

func (b *Broadcaster) Subscribe() chan WeatherData {
	ch := make(chan WeatherData, 8)
	b.mutex.Lock()
	b.subscribers = append(b.subscribers, ch)
	b.mutex.Unlock()
	return ch
}

func (b *Broadcaster) Send(data WeatherData) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for _, ch := range b.subscribers {
		select {
		case ch <- data:
		default:
		}
	}
}

// ─── WeatherStation ───────────────────────────────────────────────────────────

type WeatherStation struct {
	windSpeed float64
	sun       float64
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
			ws.sun += rand.Float64()*20 - 10
			ws.windSpeed = math.Max(0, math.Min(WindMax, ws.windSpeed))
			ws.sun = math.Max(0, math.Min(SunMax, ws.sun))
			broadcast.Send(WeatherData{WindSpeed: ws.windSpeed, SunIntensity: ws.sun})
		}
	}
}

// ─── Predictor ────────────────────────────────────────────────────────────────
// NAPRAWIONO: stara prognoza liczyła absolutną moc z trendu wiatru/słońca
// i wysyłała ją jako "predictedPower". GridHub interpretował to jako całkowitą
// produkcję, nie jako deltę — porównanie było błędne.
// Teraz predictedChange = o ile MW zmieni się produkcja OZE (delta).

type WeatherPredictor struct {
	buffer []WeatherData
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
			if len(p.buffer) > int(PredictorBufferSize) {
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

			// ekstrapolacja: o ile zmieni się moc OZE w kolejnych krokach
			predictedChange := (trendWind/WindMax)*WINDPOWER + (trendSun/SunMax)*SUNPOWER

			select {
			case forecastChan <- ForecastData{predictedChange: predictedChange}:
				fmt.Printf("[Predictor]: Zmiana OZE: %+.2f MW (wiatr %+.2f, słońce %+.2f)\n",
					predictedChange, trendWind, trendSun)
			default:
			}
		}
	}
}

// ─── OZE ─────────────────────────────────────────────────────────────────────
// NAPRAWIONO: Run() przyjmował *Broadcaster zamiast <-chan WeatherData —
// niespójne z interfejsem EnergySource. Teraz zgodne z interfejsem.
// NAPRAWIONO: SetLimit() mogło ustawić limit ujemny co dawało output < 0.

type OZE struct {
	output float64
	mu     sync.Mutex
	limit  float64
}

func NewOZE() *OZE {
	return &OZE{limit: math.MaxFloat64}
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

func (o *OZE) Run(ctx context.Context, wg *sync.WaitGroup, weatherChan <-chan WeatherData) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-weatherChan:
			basePower := (data.WindSpeed/WindMax)*WINDPOWER + (data.SunIntensity/SunMax)*SUNPOWER
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

// ─── CoalPlant ────────────────────────────────────────────────────────────────
// NAPRAWIONO: Run() (przemianowane na Start()) uruchamiało wewnętrzną gorutynę
// bez wg.Add/Done — gorutyna żyła po wg.Wait(), co łamało graceful shutdown.
// Teraz CoalPlant trzyma referencję do *sync.WaitGroup i rejestruje się w niej.

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
	wg          *sync.WaitGroup // referencja do globalnego WaitGroup
}

func (cp *CoalPlant) Start(ctx context.Context) {
	cp.mu.Lock()
	if cp.state != PlantOff {
		cp.mu.Unlock()
		return
	}
	fmt.Println("[CoalPlant]: Uruchamiam elektrownię węglową (rozgrzewanie)...")
	cp.state = PlantWarmingUp
	cp.mu.Unlock()

	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		select {
		case <-time.After(GridStep * time.Duration(cp.warmingTime)):
			cp.mu.Lock()
			cp.state = PlantOn
			cp.output = 200.0
			cp.mu.Unlock()
			fmt.Println("[CoalPlant]: Elektrownia gotowa — 200 MW.")
		case <-ctx.Done():
			cp.mu.Lock()
			cp.state = PlantOff
			cp.mu.Unlock()
		}
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

func (cp *CoalPlant) GetState() PlantState {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.state
}

// ─── ESS ─────────────────────────────────────────────────────────────────────
// NAPRAWIONO: Charge() zwracało nadwyżkę (energię NIEprzyjętą), ale GridHub
// używał wartości > 0 jako "bateria pełna" — semantyka była spójna, ale
// myląca i różna od Discharge(). Teraz Charge zwraca faktycznie przyjętą
// energię (jak Discharge), a GridHub sam oblicza surplus = balance - accepted.

type ESS struct {
	capacity float64
	current  float64
	mu       sync.Mutex
}

func (ess *ESS) Charge(value float64) float64 {
	ess.mu.Lock()
	defer ess.mu.Unlock()
	space := ess.capacity - ess.current
	accepted := math.Min(value, space)
	ess.current += accepted
	return accepted // ile faktycznie przyjęto
}

func (ess *ESS) Discharge(value float64) float64 {
	ess.mu.Lock()
	defer ess.mu.Unlock()
	available := math.Min(value, ess.current)
	ess.current -= available
	return available
}

func (ess *ESS) GetSoC() float64 {
	ess.mu.Lock()
	defer ess.mu.Unlock()
	return ess.current / ess.capacity
}

// ─── Konsumenci ───────────────────────────────────────────────────────────────
// NAPRAWIONO we wszystkich typach: wysyłka do demandChan nie sprawdzała ctx —
// przy shutdown gorutyna mogła zablokować się na pełnym kanale na zawsze.
// Teraz select { case demandChan <- ...: case <-ctx.Done(): return }.

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

// ─── GridHub ──────────────────────────────────────────────────────────────────
// NAPRAWIONO: gh.demands nie było czyszczone na początku ticka — stare raporty
// z poprzedniej godziny sumowały się z nowymi, zawyżając popyt.
// NAPRAWIONO: curtailment przez SetLimit(GetPower() - unallocated) używał
// już ograniczonej mocy jako podstawy, więc przy kolejnych wywołaniach limit
// zbiegał do 0. Teraz przyjmujemy absolutny limit = ozeOutput - surplus.
// NAPRAWIONO: przy Load Shedding deficit był ujemny (balance < 0), ale kod
// porównywał `if deficit < 0` co pomijało pierwszego konsumenta. Teraz
// deficit = -balance (dodatnia liczba) i odejmujemy kolejne żądania.

type GridHub struct {
	oze          *OZE
	coalPlant    *CoalPlant
	ess          *ESS
	forecastChan <-chan ForecastData
	demandChan   <-chan DemandReport
	demands      map[string]DemandReport
	logger       DataLogger
	statsMu      sync.Mutex // wyłącznie do agregacji statystyk globalnych
}

func (gh *GridHub) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(GridStep)
	defer ticker.Stop()

	gh.demands = make(map[string]DemandReport)
	step := 0
	ozeOutput := 0.0
	coalOutput := 0.0

	for {
		select {
		case <-ctx.Done():
			return

		case forecast := <-gh.forecastChan:
			// Zdarzenie 2: prognoza — proaktywny rozruch węgla
			currentDemand := 0.0
			for _, dem := range gh.demands {
				currentDemand += dem.Demand
			}
			predictedOutput := ozeOutput + coalOutput + forecast.predictedChange
			if predictedOutput < currentDemand && gh.coalPlant.GetState() == PlantOff {
				fmt.Printf("[GridHub]: Prognoza %.2f MW < popyt %.2f MW — uruchamiam elektrownię!\n",
					predictedOutput, currentDemand)
				gh.coalPlant.Start(ctx)
			}

		case d := <-gh.demandChan:
			gh.demands[d.Id] = d

		case <-ticker.C:
			step++
			status := "STABLE"

			ozeOutput = gh.oze.GetPower()
			coalOutput = gh.coalPlant.GetPower()

			currentDemand := 0.0
			for _, dem := range gh.demands {
				currentDemand += dem.Demand
			}

			balance := ozeOutput + coalOutput - currentDemand

			if balance >= 0 {
				// Zdarzenie 3a: nadwyżka → ładuj ESS
				accepted := gh.ess.Charge(balance)
				surplus := balance - accepted

				if surplus > 0 {
					// ESS pełny → Curtailment OZE
					newLimit := math.Max(0, ozeOutput-surplus)
					gh.oze.SetLimit(newLimit)
					fmt.Printf("[GridHub]: Curtailment — limit OZE → %.1f MW\n", newLimit)
				} else {
					gh.oze.SetLimit(math.MaxFloat64)
				}

				for _, dem := range gh.demands {
					dem.RespChan <- SupplyStatus{AvailablePower: dem.Demand, message: "ok"}
				}

			} else {
				// Zdarzenie 3b: niedobór → rozładuj ESS
				status = "CRITICAL"
				fromESS := gh.ess.Discharge(-balance)
				balance += fromESS

				if balance >= 0 {
					for _, dem := range gh.demands {
						dem.RespChan <- SupplyStatus{AvailablePower: dem.Demand, message: "ok"}
					}
				} else {
					// Zdarzenie 4: Load Shedding
					activeReq := make([]DemandReport, 0, len(gh.demands))
					for _, v := range gh.demands {
						activeReq = append(activeReq, v)
					}
					sort.Slice(activeReq, func(i, j int) bool {
						return activeReq[i].Priority > activeReq[j].Priority
					})

					deficit := -balance // dodatnia: tyle brakuje
					for _, req := range activeReq {
						if deficit > 0 {
							fmt.Printf("[LoadShed]: Odłączam %s (priorytet %d, %.1f MW)\n",
								req.Id, req.Priority, req.Demand)
							req.RespChan <- SupplyStatus{AvailablePower: 0, message: "LoadShedding"}
							deficit -= req.Demand
							gh.logger.Log(fmt.Sprintf("LOADSHED,%s,%d,%.2f", req.Id, step, req.Demand))
						} else {
							req.RespChan <- SupplyStatus{AvailablePower: req.Demand, message: "ok"}
						}
					}
				}
			}

			gh.statsMu.Lock()
			if step%RaportEveryN == 0 {
				fmt.Printf("[Produkcja] OZE: %.1f MW | Konwencjonalna: %.1f MW | Baterie: %.0f%% (SoC)\n",
					ozeOutput, coalOutput, gh.ess.GetSoC()*100)
				fmt.Printf("[Sieć]      Popyt: %.1f MW | Bilans: %+.1f MW | Stan: [%s]\n",
					currentDemand, balance, status)
			}
			gh.statsMu.Unlock()

			gh.logger.Log(fmt.Sprintf("%d,%.2f,%.2f,%.3f,%.2f,%.2f,%s",
				step%24, ozeOutput, coalOutput, gh.ess.GetSoC(), currentDemand, balance, status))

			// Wyczyść demands — w kolejnym GridStep konsumenci wyślą nowe raporty
			gh.demands = make(map[string]DemandReport)
		}
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	fmt.Println("[SYSTEM] Uruchamianie sieci energetycznej...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n[SYSTEM] Ctrl+C — graceful shutdown...")
		cancel()
	}()

	var wg sync.WaitGroup

	logger, err := NewLogger("logs/grid_history.csv")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Błąd loggera: %v\n", err)
		os.Exit(1)
	}

	broadcaster := &Broadcaster{subscribers: make([]chan<- WeatherData, 0)}
	weatherStation := &WeatherStation{windSpeed: 10.0, sun: 70.0}
	predictor := &WeatherPredictor{buffer: make([]WeatherData, 0, int(PredictorBufferSize))}
	oze := NewOZE()
	coalPlant := &CoalPlant{state: PlantOff, warmingTime: 3, wg: &wg}
	ess := &ESS{capacity: 500.0, current: 250.0}

	demandChan := make(chan DemandReport, 50)
	forecastChan := make(chan ForecastData, 1)

	hub := &GridHub{
		oze:          oze,
		coalPlant:    coalPlant,
		ess:          ess,
		forecastChan: forecastChan,
		demandChan:   demandChan,
		logger:       logger,
	}

	consumers := []Consumer{
		&ResidentalConsumer{id: "r1", baseDemand: 5.0},
		&ResidentalConsumer{id: "r2", baseDemand: 8.0},
		&ResidentalConsumer{id: "r3", baseDemand: 4.0},
		&ResidentalConsumer{id: "r4", baseDemand: 13.0},
		&IndustrialConsumer{id: "i1", baseDemand: 50.0},
		&IndustrialConsumer{id: "i2", baseDemand: 83.0},
		&CriticalConsumer{id: "c1", baseDemand: 30.0},
	}

	wg.Add(1)
	go logger.Run(ctx, &wg)

	wg.Add(1)
	go weatherStation.Run(ctx, &wg, broadcaster)

	wg.Add(1)
	go oze.Run(ctx, &wg, broadcaster.Subscribe())

	wg.Add(1)
	go predictor.Run(ctx, &wg, broadcaster.Subscribe(), forecastChan)

	wg.Add(1)
	go hub.Run(ctx, &wg)

	for _, c := range consumers {
		wg.Add(1)
		go c.Run(ctx, &wg, demandChan)
	}

	wg.Wait()
	logger.Flush()
	fmt.Println("[SYSTEM] Wszystkie komponenty zamknięte. Koniec.")
}
