package main

import (
	"fmt"
	"math/rand"
)

const simulations int = 10000

func sym31(change bool) int {
	// N - pudeł
	var N int = 3
	var won int = 0

	for i := 0; i < simulations; i++ {
		var prize int = rand.Intn(N)  // indeks nagrody
		var choice int = rand.Intn(N) // indeks wybranego miejsca

		// Gdy zmienia
		if change {
			if choice != prize {
				won++
			}
		} else { // Nie zmienia
			if choice == prize {
				won++
			}
		}

	}

	return won
}

func sym(N int, k int, change bool) int {
	// N - pudeł
	// k - otwartych pudeł przez prowadzącego
	var won int = 0

	for i := 0; i < simulations; i++ {
		var prize int = rand.Intn(N)  // indeks nagrody
		var choice int = rand.Intn(N) // indeks wybranego miejsca

		// Gdy zmienia
		if change {
			if choice != prize {
				newBoxes := N - k - 1
				var newChoice = rand.Intn(newBoxes)

				if newChoice == 0 {
					won++
				}
			}
		} else { // Nie zmienia
			if choice == prize {
				won++
			}
		}

	}

	return won
}

func main() {
	var won, wonWhenChanged int

	fmt.Println("============ N = 3, k = 1 ============")
	// won = sym(3, 1, false)
	// wonWhenChanged = sym(3, 1, true)
	won = sym31(false)
	wonWhenChanged = sym31(true)
	fmt.Printf("Wygranych bez zmiany: %d (%.2f%%)\n", won, float32(won)/float32(simulations)*100)
	fmt.Printf("Wygranych po zmianie: %d (%.2f%%)\n", wonWhenChanged, float32(wonWhenChanged)/float32(simulations)*100)
	fmt.Println("============ N = 10, k = 4 ============")
	won = sym(10, 4, false)
	wonWhenChanged = sym(10, 4, true)
	fmt.Printf("Wygranych bez zmiany: %d (%.2f%%)\n", won, float32(won)/float32(simulations)*100)
	fmt.Printf("Wygranych po zmianie: %d (%.2f%%)\n", wonWhenChanged, float32(wonWhenChanged)/float32(simulations)*100)

	fmt.Println("============ N = 50, k = 20 ============")
	won = sym(50, 20, false)
	wonWhenChanged = sym(50, 20, true)
	fmt.Printf("Wygranych bez zmiany: %d (%.2f%%)\n", won, float32(won)/float32(simulations)*100)
	fmt.Printf("Wygranych po zmianie: %d (%.2f%%)\n", wonWhenChanged, float32(wonWhenChanged)/float32(simulations)*100)
}
