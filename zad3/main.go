package main

import (
	"fmt"
)

type Plane struct {
	ID       int
	Capacity int
}

type Passenger struct {
	ID int
}

type Reservation struct {
	Flight    *Flight
	Passenger *Passenger
}

// ============ FLIGHT STRUCT ============

type Flight struct {
	ID            int
	Plane         *Plane
	Reservations  []*Reservation
	DeparturePort string
	Destination   string
}

func (f *Flight) String() string {
	return fmt.Sprintf("Lot-%d: %s=>%s | maszyna-%d | pasażerów-%d",
		f.ID, f.DeparturePort, f.Destination, f.Plane.ID, len(f.Reservations))
}

func (f *Flight) CheckCapacity() int {
	return f.Plane.Capacity - len(f.Reservations)
}

func (f *Flight) IsPassengerRegistered(p *Passenger) bool {
	for _, r := range f.Reservations {
		if r.Passenger.ID == p.ID {
			return true
		}
	}
	return false
}

func (f *Flight) MakeReservation(p *Passenger) bool {
	if f.CheckCapacity() <= 0 {
		fmt.Println("Brak miejsca w locie: ", f)
		return false
	}
	if f.IsPassengerRegistered(p) {
		fmt.Printf("Pasażer id-#%d, ma już rezerwację na lot: %s\n", p.ID, f)
		return false
	}
	newReservation := &Reservation{Flight: f, Passenger: p}
	f.Reservations = append(f.Reservations, newReservation)
	return true
}

func (f *Flight) CancelReservation(p *Passenger) bool {
	for i, r := range f.Reservations {
		if r.Passenger.ID == p.ID {
			f.Reservations = append(f.Reservations[:i], f.Reservations[i+1:]...)
			return true
		}
	}
	return false
}

// ============ SCHEDULE STRUCT ============

type Schedule struct {
	Flights []*Flight
}

func (s *Schedule) FindPassengerReservations(p *Passenger) []*Reservation {
	var passengerReservations []*Reservation
	for _, f := range s.Flights {
		for _, r := range f.Reservations {
			if r.Passenger.ID == p.ID {
				passengerReservations = append(passengerReservations, r)
			}
		}
	}
	return passengerReservations
}

func (s *Schedule) filter(pred func(*Flight) bool) []*Flight {
	var flightList []*Flight
	for _, f := range s.Flights {
		if pred(f) {
			flightList = append(flightList, f)
		}
	}
	return flightList
}

func (s *Schedule) ByDeparture(p string) []*Flight {
	return s.filter(func(f *Flight) bool {
		return f.DeparturePort == p
	})
}

func (s *Schedule) ByDestination(p string) []*Flight {
	return s.filter(func(f *Flight) bool {
		return f.Destination == p
	})
}

// func (s *Schedule) ByDestination(p string) []*Flight {
// 	var flightsList []*Flight
// 	for _, f := range s.Flights {
// 		if f.Destination == p {
// 			flightsList = append(flightsList, f)
// 		}
// 	}
// 	return flightsList
// }

// ===============

type SearchFlights interface {
	ByDeparture(port string) []*Flight
	ByDestination(port string) []*Flight
}
type SearchReservation interface {
	ByPassenger(p *Passenger) []*Reservation
}

// ============

func main() {
	p1 := &Plane{
		ID:       1,
		Capacity: 120,
	}
	p2 := &Plane{
		ID:       2,
		Capacity: 1,
	}

	// ============

	f1 := &Flight{
		ID:            1,
		Plane:         p1,
		DeparturePort: "Warszawa",
		Destination:   "Paryz",
	}
	f2 := &Flight{
		ID:            2,
		Plane:         p2,
		DeparturePort: "Waszington",
		Destination:   "Teheran",
	}
	f3 := &Flight{
		ID:            3,
		Plane:         p1,
		DeparturePort: "Tokio",
		Destination:   "Niujork",
	}

	// ============

	ps1 := &Passenger{
		ID: 1,
	}
	ps2 := &Passenger{
		ID: 2,
	}

	// ============

	sch := Schedule{
		Flights: []*Flight{f1, f2, f3},
	}

	fmt.Println("============ Rezerwacje ============")

	fmt.Println("  - f1-ps1 => ok")
	f1.MakeReservation(ps1)

	fmt.Println("\n  - f2-ps1+ps2 => brak miejsca")
	f2.MakeReservation(ps1)
	f2.MakeReservation(ps2)

	fmt.Println("\n  - f3-ps1+ps2+ps2 => nie mozna dwa razy")
	f3.MakeReservation(ps1)
	f3.MakeReservation(ps2)
	f3.MakeReservation(ps2)

	fmt.Println("\n============ Odwołanie ============")

	r := f2.CancelReservation(ps1)
	fmt.Println("\n  - f2-ps1 ; wynik: ", r)

	r = f2.CancelReservation(ps1)
	fmt.Println("\n  - f2-ps1 (nie ma go); wynik: ", r)

	fmt.Println("============ Wolne miejsca ============")
	fmt.Printf("  - f1-%d\n", f1.CheckCapacity())
	fmt.Printf("  - f2-%d\n", f2.CheckCapacity())
	fmt.Printf("  - f3-%d\n", f3.CheckCapacity())

	fmt.Println("============ Find By ============")
	l := sch.ByDeparture("Warszawa")
	fmt.Println("  - ByDeparture=Warszawa: ", l)

	l = sch.ByDestination("Niujork")
	fmt.Println("  - ByDestination=Niujork: ", l)

	l = sch.ByDestination("Brak")
	fmt.Println("  - ByDestination=Brak: ", l)

	fmt.Println("============ Rezerwacje danego pasażera ============")
	rl := sch.FindPassengerReservations(ps1)

	fmt.Println("  - Dla ps1:")
	for _, r := range rl {
		fmt.Println("    - ", r)
	}
}
