package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

// ─── Stałe symulacji ────────────────────────────────────────────────────────

const (
	WeatherStep      = 5 * time.Millisecond   // ~5 minut czasu symulacji
	GridStep         = 100 * time.Millisecond // 1 godzina czasu symulacji
	WeatherPerGrid   = GridStep / WeatherStep // = 12 kroków pogodowych
	ForecastHorizon  = 5                      // prognoza na 5 kroków GridStep
	PredictorBufSize = WeatherPerGrid         // bufor = 1 godzina historii
	ReportEveryN     = 3                      // raport co N kroków GridStep
)

// ─── Struktury komunikacyjne ─────────────────────────────────────────────────

type WeatherData struct {
	WindSpeed float64 // km/h
	SolarIrr  float64 // 0.0–1.0
	SimHour   int     // godzina symulacji (0–23)
}

type DemandReport struct {
	ID       string
	Demand   float64 // MW
	Priority int     // 1=Critical, 2=Industrial, 3=Residential
}

type SupplyStatus struct {
	AllocatedMW float64
	Reason      string
}

type ForecastReport struct {
	StepsAhead int
	DeltaPct   float64 // przewidywana zmiana produkcji OZE w %
	ExpectedMW float64 // przewidywana moc OZE
}

type LogEntry struct {
	SimHour     int
	WindSpeed   float64
	SolarIrr    float64
	RenewMW     float64
	ConvMW      float64
	SoC         float64
	TotalDemand float64
	Balance     float64
	Status      string
}

// ─── Interfejsy ──────────────────────────────────────────────────────────────

type EnergySource interface {
	CurrentOutput() float64
	SetCurtailment(factor float64)
	Run(ctx context.Context, weatherCh <-chan WeatherData)
}

type Predictor interface {
	Run(ctx context.Context, weatherCh <-chan WeatherData, forecastCh chan<- ForecastReport)
}

type Consumer interface {
	Run(ctx context.Context, demandCh chan<- DemandReport, replyCh <-chan SupplyStatus, ticker <-chan time.Time)
	GetID() string
	GetPriority() int
}

type EnergyStorage interface {
	Charge(mw float64) float64
	Discharge(mw float64) float64
	SoC() float64
}

type WeatherProvider interface {
	Run(ctx context.Context, outCh chan<- WeatherData)
}

type DataLogger interface {
	Log(entry LogEntry)
	Run(ctx context.Context)
	Flush()
}

// ─── WeatherStation ───────────────────────────────────────────────────────────

type WeatherStation struct {
	windSpeed float64
	simStep   int
}

func NewWeatherStation() *WeatherStation {
	return &WeatherStation{windSpeed: 20.0}
}

func (ws *WeatherStation) Run(ctx context.Context, outCh chan<- WeatherData) {
	ticker := time.NewTicker(WeatherStep)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ws.simStep++
			ws.windSpeed += rand.Float64()*2 - 1
			if ws.windSpeed < 0 {
				ws.windSpeed = 0
			}
			if ws.windSpeed > 60 {
				ws.windSpeed = 60
			}
			hour := float64(ws.simStep%288) / 12.0
			solar := math.Max(0, math.Sin((hour-6)*math.Pi/12))
			solarIrr := solar * (0.8 + rand.Float64()*0.2)
			select {
			case outCh <- WeatherData{
				WindSpeed: ws.windSpeed,
				SolarIrr:  solarIrr,
				SimHour:   int(hour) % 24,
			}:
			default:
			}
		}
	}
}

// ─── Broadcaster (Pub/Sub) ────────────────────────────────────────────────────

type Broadcaster struct {
	mu          sync.Mutex
	subscribers []chan<- WeatherData
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{}
}

func (b *Broadcaster) Subscribe(bufSize int) <-chan WeatherData {
	ch := make(chan WeatherData, bufSize)
	b.mu.Lock()
	b.subscribers = append(b.subscribers, ch)
	b.mu.Unlock()
	return ch
}

func (b *Broadcaster) Run(ctx context.Context, inCh <-chan WeatherData) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-inCh:
			if !ok {
				return
			}
			b.mu.Lock()
			subs := make([]chan<- WeatherData, len(b.subscribers))
			copy(subs, b.subscribers)
			b.mu.Unlock()
			for _, ch := range subs {
				select {
				case ch <- data:
				default:
				}
			}
		}
	}
}

// ─── WindFarm (EnergySource) ──────────────────────────────────────────────────

type WindFarm struct {
	mu          sync.Mutex
	capacity    float64
	output      float64
	curtailment float64
}

func NewWindFarm(capacity float64) *WindFarm {
	return &WindFarm{capacity: capacity, curtailment: 1.0}
}

func (wf *WindFarm) CurrentOutput() float64 {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	return wf.output
}

func (wf *WindFarm) SetCurtailment(factor float64) {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	wf.curtailment = factor
}

func (wf *WindFarm) Run(ctx context.Context, weatherCh <-chan WeatherData) {
	for {
		select {
		case <-ctx.Done():
			return
		case wd, ok := <-weatherCh:
			if !ok {
				return
			}
			factor := math.Pow(wd.WindSpeed/40.0, 3)
			if factor > 1 {
				factor = 1
			}
			wf.mu.Lock()
			wf.output = wf.capacity * factor * wf.curtailment
			wf.mu.Unlock()
		}
	}
}

// ─── SolarFarm (EnergySource) ─────────────────────────────────────────────────

type SolarFarm struct {
	mu          sync.Mutex
	capacity    float64
	output      float64
	curtailment float64
}

func NewSolarFarm(capacity float64) *SolarFarm {
	return &SolarFarm{capacity: capacity, curtailment: 1.0}
}

func (sf *SolarFarm) CurrentOutput() float64 {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return sf.output
}

func (sf *SolarFarm) SetCurtailment(factor float64) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	sf.curtailment = factor
}

func (sf *SolarFarm) Run(ctx context.Context, weatherCh <-chan WeatherData) {
	for {
		select {
		case <-ctx.Done():
			return
		case wd, ok := <-weatherCh:
			if !ok {
				return
			}
			sf.mu.Lock()
			sf.output = sf.capacity * wd.SolarIrr * sf.curtailment
			sf.mu.Unlock()
		}
	}
}

// ─── CoalPlant ────────────────────────────────────────────────────────────────

type PlantState int

const (
	PlantOff PlantState = iota
	PlantWarmingUp
	PlantOn
)

type CoalPlant struct {
	mu         sync.Mutex
	capacity   float64
	state      PlantState
	warmupLeft int
}

func NewCoalPlant(capacity float64) *CoalPlant {
	return &CoalPlant{capacity: capacity, state: PlantOff}
}

func (cp *CoalPlant) Start() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.state == PlantOff {
		cp.state = PlantWarmingUp
		cp.warmupLeft = 3
	}
}

func (cp *CoalPlant) Tick() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.state == PlantWarmingUp {
		cp.warmupLeft--
		if cp.warmupLeft <= 0 {
			cp.state = PlantOn
			fmt.Println("[CoalPlant] Elektrownia węglowa gotowa — pełna moc!")
		}
	}
}

func (cp *CoalPlant) CurrentOutput() float64 {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.state == PlantOn {
		return cp.capacity
	}
	return 0
}

func (cp *CoalPlant) IsOff() bool {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.state == PlantOff
}

// ─── ESS (EnergyStorage) ─────────────────────────────────────────────────────

type BatteryESS struct {
	mu       sync.Mutex
	capacity float64
	stored   float64
}

func NewBatteryESS(capacityMWh float64) *BatteryESS {
	return &BatteryESS{capacity: capacityMWh, stored: capacityMWh * 0.5}
}

func (b *BatteryESS) Charge(mw float64) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	canAccept := b.capacity - b.stored
	actual := math.Min(mw, canAccept)
	b.stored += actual
	return actual
}

func (b *BatteryESS) Discharge(mw float64) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	actual := math.Min(mw, b.stored)
	b.stored -= actual
	return actual
}

func (b *BatteryESS) SoC() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.capacity == 0 {
		return 0
	}
	return b.stored / b.capacity
}

// ─── WeatherPredictor ─────────────────────────────────────────────────────────

type WeatherPredictor struct {
	buffer []WeatherData
}

func NewPredictor() *WeatherPredictor {
	return &WeatherPredictor{
		buffer: make([]WeatherData, 0, int(PredictorBufSize)*2),
	}
}

func (p *WeatherPredictor) Run(ctx context.Context, weatherCh <-chan WeatherData, forecastCh chan<- ForecastReport) {
	gridTicker := time.NewTicker(GridStep)
	defer gridTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case wd, ok := <-weatherCh:
			if !ok {
				return
			}
			p.buffer = append(p.buffer, wd)
			if len(p.buffer) > int(PredictorBufSize)*2 {
				p.buffer = p.buffer[1:]
			}
		case <-gridTicker.C:
			if len(p.buffer) < 4 {
				continue
			}
			n := len(p.buffer)
			recent := p.buffer[n/2:]
			older := p.buffer[:n/2]
			avgRecent := avgRenew(recent)
			avgOlder := avgRenew(older)
			var deltaPct float64
			if avgOlder > 0 {
				deltaPct = (avgRecent - avgOlder) / avgOlder * 100
			}
			forecast := ForecastReport{
				StepsAhead: ForecastHorizon,
				DeltaPct:   deltaPct * float64(ForecastHorizon) / 2,
				ExpectedMW: avgRecent * (1 + deltaPct/100),
			}
			select {
			case forecastCh <- forecast:
			default:
			}
		}
	}
}

func avgRenew(data []WeatherData) float64 {
	if len(data) == 0 {
		return 0
	}
	sum := 0.0
	for _, d := range data {
		wind := math.Pow(d.WindSpeed/40.0, 3)
		if wind > 1 {
			wind = 1
		}
		sum += wind*200 + d.SolarIrr*150
	}
	return sum / float64(len(data))
}

// ─── Konsumenci ───────────────────────────────────────────────────────────────

type ResidentialConsumer struct {
	id string
}

func NewResidential(id string) *ResidentialConsumer {
	return &ResidentialConsumer{id: id}
}

func (r *ResidentialConsumer) GetID() string    { return r.id }
func (r *ResidentialConsumer) GetPriority() int { return 3 }

func (r *ResidentialConsumer) demand(hour int) float64 {
	morning := 50 * math.Exp(-math.Pow(float64(hour-8), 2)/2)
	evening := 80 * math.Exp(-math.Pow(float64(hour-20), 2)/4.5)
	return 20.0 + morning + evening
}

func (r *ResidentialConsumer) Run(ctx context.Context, demandCh chan<- DemandReport, replyCh <-chan SupplyStatus, ticker <-chan time.Time) {
	hour := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker:
			select {
			case demandCh <- DemandReport{ID: r.id, Demand: r.demand(hour % 24), Priority: 3}:
			case <-ctx.Done():
				return
			}
			select {
			case <-replyCh:
			case <-ctx.Done():
				return
			}
			hour++
		}
	}
}

// ─── IndustrialConsumer ───────────────────────────────────────────────────────

type IndustrialConsumer struct {
	id string
}

func NewIndustrial(id string) *IndustrialConsumer {
	return &IndustrialConsumer{id: id}
}

func (i *IndustrialConsumer) GetID() string    { return i.id }
func (i *IndustrialConsumer) GetPriority() int { return 2 }

func (i *IndustrialConsumer) demand(hour int) float64 {
	if hour >= 6 && hour <= 18 {
		base := 150.0
		if rand.Float64() < 0.1 {
			base += 50 // losowy pik rozruchu
		}
		return base
	}
	return 30.0
}

func (i *IndustrialConsumer) Run(ctx context.Context, demandCh chan<- DemandReport, replyCh <-chan SupplyStatus, ticker <-chan time.Time) {
	hour := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker:
			select {
			case demandCh <- DemandReport{ID: i.id, Demand: i.demand(hour % 24), Priority: 2}:
			case <-ctx.Done():
				return
			}
			select {
			case <-replyCh:
			case <-ctx.Done():
				return
			}
			hour++
		}
	}
}

// ─── CriticalConsumer ─────────────────────────────────────────────────────────

type CriticalConsumer struct {
	id string
}

func NewCritical(id string) *CriticalConsumer {
	return &CriticalConsumer{id: id}
}

func (c *CriticalConsumer) GetID() string    { return c.id }
func (c *CriticalConsumer) GetPriority() int { return 1 }

func (c *CriticalConsumer) Run(ctx context.Context, demandCh chan<- DemandReport, replyCh <-chan SupplyStatus, ticker <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker:
			select {
			case demandCh <- DemandReport{ID: c.id, Demand: 40.0, Priority: 1}:
			case <-ctx.Done():
				return
			}
			select {
			case <-replyCh:
			case <-ctx.Done():
				return
			}
		}
	}
}

// ─── DataLogger (CSV) ─────────────────────────────────────────────────────────

type CSVLogger struct {
	logCh chan LogEntry
	file  *os.File
	buf   []byte
}

func NewCSVLogger(path string) (*CSVLogger, error) {
	if err := os.MkdirAll("logs", 0755); err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	l := &CSVLogger{
		logCh: make(chan LogEntry, 128),
		file:  f,
		buf:   make([]byte, 0, 8192),
	}
	l.writeLine("sim_hour,wind_kmh,solar_irr,renew_mw,conv_mw,soc,demand_mw,balance_mw,status\n")
	return l, nil
}

func (l *CSVLogger) writeLine(s string) {
	l.buf = append(l.buf, s...)
	if len(l.buf) > 6144 {
		l.file.Write(l.buf)
		l.buf = l.buf[:0]
	}
}

func (l *CSVLogger) Log(e LogEntry) {
	select {
	case l.logCh <- e:
	default:
	}
}

func (l *CSVLogger) Run(ctx context.Context) {
	for {
		select {
		case e := <-l.logCh:
			l.writeLine(fmt.Sprintf("%d,%.2f,%.2f,%.2f,%.2f,%.3f,%.2f,%.2f,%s\n",
				e.SimHour, e.WindSpeed, e.SolarIrr, e.RenewMW, e.ConvMW,
				e.SoC, e.TotalDemand, e.Balance, e.Status))
		case <-ctx.Done():
			for {
				select {
				case e := <-l.logCh:
					l.writeLine(fmt.Sprintf("%d,%.2f,%.2f,%.2f,%.2f,%.3f,%.2f,%.2f,%s\n",
						e.SimHour, e.WindSpeed, e.SolarIrr, e.RenewMW, e.ConvMW,
						e.SoC, e.TotalDemand, e.Balance, e.Status))
				default:
					return
				}
			}
		}
	}
}

func (l *CSVLogger) Flush() {
	if len(l.buf) > 0 {
		l.file.Write(l.buf)
		l.buf = l.buf[:0]
	}
	l.file.Sync()
	l.file.Close()
}

// ─── GridHub ──────────────────────────────────────────────────────────────────

type consumerEntry struct {
	consumer Consumer
	replyCh  chan SupplyStatus
}

type GridHub struct {
	statsMu sync.Mutex // tylko do agregacji statystyk

	demandCh   chan DemandReport
	forecastCh <-chan ForecastReport
	registerCh chan consumerEntry

	windFarm  *WindFarm
	solarFarm *SolarFarm
	coalPlant *CoalPlant
	ess       *BatteryESS
	logger    DataLogger

	consumers map[string]consumerEntry
	stepCount int

	lastWeatherMu sync.Mutex
	lastWeather   WeatherData
}

func NewGridHub(
	forecastCh <-chan ForecastReport,
	windFarm *WindFarm,
	solarFarm *SolarFarm,
	coalPlant *CoalPlant,
	ess *BatteryESS,
	logger DataLogger,
) *GridHub {
	return &GridHub{
		demandCh:   make(chan DemandReport, 32),
		forecastCh: forecastCh,
		registerCh: make(chan consumerEntry, 8),
		windFarm:   windFarm,
		solarFarm:  solarFarm,
		coalPlant:  coalPlant,
		ess:        ess,
		logger:     logger,
		consumers:  make(map[string]consumerEntry),
	}
}

func (gh *GridHub) DemandChan() chan<- DemandReport {
	return gh.demandCh
}

func (gh *GridHub) Register(c Consumer) <-chan SupplyStatus {
	replyCh := make(chan SupplyStatus, 1)
	gh.registerCh <- consumerEntry{consumer: c, replyCh: replyCh}
	return replyCh
}

func (gh *GridHub) UpdateWeather(wd WeatherData) {
	gh.lastWeatherMu.Lock()
	gh.lastWeather = wd
	gh.lastWeatherMu.Unlock()
}

func (gh *GridHub) Run(ctx context.Context) {
	ticker := time.NewTicker(GridStep)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case entry := <-gh.registerCh:
			gh.consumers[entry.consumer.GetID()] = entry

		case forecast := <-gh.forecastCh:
			// Zdarzenie 2: proaktywne uruchomienie węgla
			if forecast.DeltaPct < -15 && gh.coalPlant.IsOff() {
				gh.coalPlant.Start()
				fmt.Printf("[GridHub] Prognoza: OZE spadnie o %.1f%% za %d kroków — uruchamiam elektrownię!\n",
					-forecast.DeltaPct, forecast.StepsAhead)
			}

		case <-ticker.C:
			gh.coalPlant.Tick()
			gh.balance()
		}
	}
}

func (gh *GridHub) balance() {
	gh.stepCount++

	// Zbierz żądania z kanału (z timeoutem = połowa GridStep)
	demands := make(map[string]DemandReport)
	deadline := time.After(GridStep / 2)
loop:
	for {
		select {
		case dr := <-gh.demandCh:
			demands[dr.ID] = dr
		case <-deadline:
			break loop
		}
	}

	// Produkcja
	renewMW := gh.windFarm.CurrentOutput() + gh.solarFarm.CurrentOutput()
	convMW := gh.coalPlant.CurrentOutput()
	totalProd := renewMW + convMW

	// Zapotrzebowanie
	totalDemand := 0.0
	for _, dr := range demands {
		totalDemand += dr.Demand
	}

	balance := totalProd - totalDemand

	// Zdarzenie 3: ESS
	if balance > 0 {
		if gh.ess.SoC() < 1.0 {
			charged := gh.ess.Charge(balance)
			balance -= charged
		}
		if balance > 0 {
			gh.windFarm.SetCurtailment(0.8)
			gh.solarFarm.SetCurtailment(0.8)
		} else {
			gh.windFarm.SetCurtailment(1.0)
			gh.solarFarm.SetCurtailment(1.0)
		}
	} else if balance < 0 {
		discharged := gh.ess.Discharge(-balance)
		balance += discharged
	}

	// Zdarzenie 4: Load Shedding
	status := "STABLE"
	if balance < 0 {
		status = "CRITICAL"
		gh.loadShed(demands, -balance)
	}

	// Odpowiedzi do konsumentów
	for id, dr := range demands {
		entry, ok := gh.consumers[id]
		if !ok {
			continue
		}
		reply := SupplyStatus{AllocatedMW: dr.Demand, Reason: "OK"}
		select {
		case entry.replyCh <- reply:
		default:
		}
	}

	// Statystyki (chronione przez statsMu — sync.Mutex wyłącznie do agregacji)
	gh.statsMu.Lock()
	gh.lastWeatherMu.Lock()
	wd := gh.lastWeather
	gh.lastWeatherMu.Unlock()
	gh.statsMu.Unlock()

	if gh.stepCount%ReportEveryN == 0 {
		fmt.Printf("\n[Pogoda]    Wiatr: %.1f km/h | Słońce: %.0f%%\n", wd.WindSpeed, wd.SolarIrr*100)
		fmt.Printf("[Produkcja] OZE: %.1f MW | Konwencjonalna: %.1f MW | Baterie: %.0f%% (SoC)\n",
			renewMW, convMW, gh.ess.SoC()*100)
		fmt.Printf("[Sieć]      Popyt: %.1f MW | Bilans: %+.1f MW | Stan: [%s]\n",
			totalDemand, balance, status)
	}

	gh.logger.Log(LogEntry{
		SimHour:     gh.stepCount % 24,
		WindSpeed:   wd.WindSpeed,
		SolarIrr:    wd.SolarIrr,
		RenewMW:     renewMW,
		ConvMW:      convMW,
		SoC:         gh.ess.SoC(),
		TotalDemand: totalDemand,
		Balance:     balance,
		Status:      status,
	})
}

func (gh *GridHub) loadShed(demands map[string]DemandReport, deficit float64) {
	type item struct {
		id       string
		demand   float64
		priority int
	}
	var list []item
	for id, dr := range demands {
		list = append(list, item{id, dr.Demand, dr.Priority})
	}
	// sortuj od najniższego priorytetu (3=Residential) do najwyższego (1=Critical)
	sort.Slice(list, func(i, j int) bool {
		return list[i].priority > list[j].priority
	})

	for _, e := range list {
		if deficit <= 0 {
			break
		}
		consumer, ok := gh.consumers[e.id]
		if !ok {
			continue
		}
		fmt.Printf("[LoadShed]  Odłączam: %s (priorytet %d, %.1f MW)\n", e.id, e.priority, e.demand)
		select {
		case consumer.replyCh <- SupplyStatus{AllocatedMW: 0, Reason: "LoadShed"}:
		default:
		}
		delete(demands, e.id)
		deficit -= e.demand
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	logger, err := NewCSVLogger("logs/grid_history.csv")
	if err != nil {
		log.Fatalf("Błąd loggera: %v", err)
	}

	// Komponenty
	weatherStation := NewWeatherStation()
	broadcaster := NewBroadcaster()
	windFarm := NewWindFarm(200)   // 200 MW
	solarFarm := NewSolarFarm(150) // 150 MW
	coalPlant := NewCoalPlant(300) // 300 MW
	ess := NewBatteryESS(500)      // 500 MWh
	predictor := NewPredictor()

	weatherRawCh := make(chan WeatherData, 16)
	forecastCh := make(chan ForecastReport, 1)

	windWeatherCh := broadcaster.Subscribe(8)
	solarWeatherCh := broadcaster.Subscribe(8)
	predictorWeatherCh := broadcaster.Subscribe(8)
	hubWeatherCh := broadcaster.Subscribe(8)

	hub := NewGridHub(forecastCh, windFarm, solarFarm, coalPlant, ess, logger)

	var wg sync.WaitGroup

	start := func(f func()) {
		wg.Add(1)
		go func() { defer wg.Done(); f() }()
	}

	start(func() { logger.Run(ctx) })
	start(func() { weatherStation.Run(ctx, weatherRawCh) })
	start(func() { broadcaster.Run(ctx, weatherRawCh) })
	start(func() { windFarm.Run(ctx, windWeatherCh) })
	start(func() { solarFarm.Run(ctx, solarWeatherCh) })
	start(func() { predictor.Run(ctx, predictorWeatherCh, forecastCh) })
	start(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case wd := <-hubWeatherCh:
				hub.UpdateWeather(wd)
			}
		}
	})
	start(func() { hub.Run(ctx) })

	// Pomocnicza funkcja tworząca ticker per-consumer
	makeConsumerTicker := func() <-chan time.Time {
		ch := make(chan time.Time, 1)
		start(func() {
			t := time.NewTicker(GridStep)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case tick := <-t.C:
					select {
					case ch <- tick:
					default:
					}
				}
			}
		})
		return ch
	}

	// Konsumenci startowi
	initialConsumers := []Consumer{
		NewResidential("dom_1"),
		NewResidential("dom_2"),
		NewIndustrial("fabryka_1"),
		NewCritical("szpital_1"),
	}

	for _, c := range initialConsumers {
		replyCh := hub.Register(c)
		tickerCh := makeConsumerTicker()
		c, replyCh, tickerCh := c, replyCh, tickerCh
		start(func() {
			c.Run(ctx, hub.DemandChan(), replyCh, tickerCh)
		})
	}

	// Dynamiczna rejestracja po 5 sekundach
	go func() {
		select {
		case <-time.After(5 * time.Second):
			newFactory := NewIndustrial("fabryka_2_dynamiczna")
			replyCh := hub.Register(newFactory)
			tickerCh := makeConsumerTicker()
			fmt.Println("\n[GridHub] ++ Dynamicznie dodano: fabryka_2_dynamiczna")
			wg.Add(1)
			go func() {
				defer wg.Done()
				newFactory.Run(ctx, hub.DemandChan(), replyCh, tickerCh)
			}()
		case <-ctx.Done():
		}
	}()

	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║   Symulator sieci energetycznej (Go)     ║")
	fmt.Println("║   Naciśnij Ctrl+C aby zakończyć          ║")
	fmt.Println("╚══════════════════════════════════════════╝")

	select {
	case <-sigCh:
		fmt.Println("\n[System] Sygnał przerwania — graceful shutdown...")
	case <-time.After(30 * time.Second):
		fmt.Println("\n[System] Koniec demonstracji (30s) — graceful shutdown...")
	}

	cancel()
	wg.Wait()
	logger.Flush()
	fmt.Println("[System] DataLogger.Flush() — plik logs/grid_history.csv zapisany.")
}
