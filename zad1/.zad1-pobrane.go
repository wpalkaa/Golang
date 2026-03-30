package main

import (
	"fmt"
	"math/rand"
)

func main() {

	var parzyste, nieparzyste int

	for i := 1; i <= 10; i++ {

		if rand.Intn(10)%2 == 0 { //jezeli losowa liczba jest parzysta

			parzyste++

		} else {

			nieparzyste++

		}

	}

	fmt.Println(parzyste, nieparzyste)

	// omówienie na wykladzie
	var liczby []int
	i := 0
	for {
		if len(liczby) == 10 {
			break
		} else {
			liczby = append(liczby, i)

			i++
		}
		fmt.Println(liczby)

	}

}
