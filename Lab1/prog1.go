// Podstawowe rzeczy:
// go mod init main - inicjalizacja pakietu 'main'
// go fmt ./  - sformatowanie wszystkich plików w katalogu w sposób jaki 'go' chce
// go build -o nazwa  - kompilacja

package main

import "fmt"

func main() {

	var liczba1 int64 // dostaje domyślną wartość = 0
	// var liczba1 int64 = 10

	// kilka liczb
	var (
		liczba2 int64   = 8
		liczba3 float64 = 12.2332
	)

	//niejawna deklaracja, kompilator sam decyduje
	var mnoznik = 5.0

	// :=   - operator przypisania, kompilator sam określa typ
	n := 9.0

	liczba1++
	liczba1 = 10

	liczba3 *= mnoznik

	liczba3 += n

	// stałe
	const stala float64 = 2.0

	fmt.Println("Hello World!", liczba1, liczba2, liczba3)
}
