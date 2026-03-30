package main

import (
	"fmt"
	"maps"
	"sort"
)

const (
	stage_I   = 0
	stage_II  = 1
	stage_III = 2
	final     = 3
)

var maxDeviation = map[int]int{
	stage_I:   3,
	stage_II:  2,
	stage_III: 2,
	final:     2,
}

var stagesWeigh = map[int]float32{
	stage_I:   0.1,
	stage_II:  0.3,
	stage_III: 0.5,
	final:     0.8,
}

type participant struct {
	name       string
	repertoire map[int][]string // stage -> songs
	scores     map[string][]int // song -> scores
	finalScore float32
}

func assingRepertoire(r []string, p participant, stage int) participant {
	newP := p
	newP.repertoire = make(map[int][]string)

	// Assign repertoire
	maps.Copy(newP.repertoire, p.repertoire)
	newRepertoire := make([]string, len(r))
	copy(newRepertoire, r)
	newP.repertoire[stage] = newRepertoire

	// Copy participant's acutal scores
	newP.scores = make(map[string][]int)
	maps.Copy(newP.scores, p.scores) // maps.Copy - działa na mapach, samo dodaje klucze itp.

	return newP
}

func assingScore(p participant, song string, scores []int) participant {
	newP := p

	// Copy participant's repertoire
	newP.repertoire = make(map[int][]string, len(p.repertoire))
	maps.Copy(newP.repertoire, p.repertoire)

	// Copy participant's acutal scores
	newP.scores = make(map[string][]int)
	for song, scores := range p.scores {
		newPScores := make([]int, len(scores))
		copy(newPScores, scores)
		newP.scores[song] = newPScores
	}

	// Assign new scores
	newP.scores[song] = scores
	return newP
}

func calculateStageScore(scores []int, stage int) float32 {
	var validScores []float32

	for _, score := range scores {
		if score > 0 && score <= 25 {
			validScores = append(validScores, float32(score))
		}
	}

	var sum float32 = 0.0
	for _, v := range validScores {
		sum += v
	}
	avg := sum / float32(len(validScores))

	deviation := float32(maxDeviation[stage])
	var fixedSum float32 = 0.0
	for _, v := range validScores {
		if v > avg+deviation {
			fixedSum += avg + deviation
		} else if v < avg-deviation {
			fixedSum += avg - deviation
		} else {
			fixedSum += v
		}
	}

	stageScore := fixedSum / float32(len(validScores))

	return stageScore
}

func calculateFinalScore(participants []participant, stage int) []participant {
	newParticipants := make([]participant, len(participants))

	for i, p := range participants {
		newP := p
		totalScore := float32(0.0)
		piecesCount := 0

		for _, scores := range p.scores { // utwór -> [oceny]

			if len(scores) > 0 {
				totalScore += float32(calculateStageScore(scores, stage))
				piecesCount++
			}
		}

		if piecesCount > 0 {
			avgStageScore := totalScore / float32(piecesCount)
			newP.finalScore += stagesWeigh[stage] * avgStageScore
		}

		// fmt.Println(totalScore, piecesCount, newP.finalScore)
		newParticipants[i] = newP
	}

	return newParticipants
}

func sortByScore(p []participant) []participant {
	sortedParticipants := make([]participant, len(p))
	copy(sortedParticipants, p)

	sort.Slice(sortedParticipants, func(i int, j int) bool {
		return sortedParticipants[i].finalScore > sortedParticipants[j].finalScore
	})

	return sortedParticipants
}

func bestBySong(participants []participant, song string) (participant, int) {
	var bestParticipant participant
	max := -1000

	for _, p := range participants {
		if scores, ok := p.scores[song]; ok {
			var score int = 0
			for _, v := range scores {
				score += v
			}

			if score > max {
				bestParticipant = p
				max = score
			}
		}
	}

	return bestParticipant, max
}

func main() {
	// zakładamy że dany utwór tylko raz w repertuarze jest bo inaczej
	// się całość wywali :I
	rep := []string{"Mozart", "Bach", "Bach2", "Liszt"}
	rep2 := []string{"Mozartini", "Bachini", "Bach2ini", "Liszini"}

	p1 := participant{name: "Marek"}
	p1 = assingRepertoire(rep, p1, stage_I)
	p1 = assingScore(p1, "Bach2", []int{23, 21, 19, 0, 10}) // avg = 18.25 / 17.8125
	p1 = assingScore(p1, "Bach", []int{3, 8, 19, 0, 23})    // 13.32 /
	p1 = assingScore(p1, "Mozart", []int{8, 25, 13, 0, 16})
	p1 = assingRepertoire(rep2, p1, stage_II)
	p1 = assingScore(p1, "Mozartini", []int{8, 25, 13, 0, 16})

	p2 := participant{name: "Chopin"}
	p2 = assingRepertoire(rep, p2, stage_I)
	p2 = assingScore(p2, "Bach2", []int{20, 21, 0, 20, 24})
	p2 = assingScore(p2, "Bach", []int{17, 12, 0, 18, 16})
	p2 = assingScore(p2, "Mozart", []int{18, 16, 0, 18, 17})
	p2 = assingRepertoire(rep2, p2, stage_II)
	p1 = assingScore(p2, "Mozartini", []int{12, 12, 0, 13, 25})

	p3 := participant{name: "Babels"}
	p3 = assingRepertoire(rep, p3, stage_I)
	p3 = assingScore(p3, "Bach2", []int{6, 1, 1, 0, 7})
	p3 = assingScore(p3, "Bach", []int{21, 3, 7, 0, 15})
	p3 = assingScore(p3, "Mozart", []int{14, 10, 12, 0, 13})
	p3 = assingRepertoire(rep2, p3, stage_II)
	p1 = assingScore(p3, "Mozartini", []int{24, 25, 25, 0, 25})

	participantList := []participant{p1, p2, p3}

	participantList = calculateFinalScore(participantList, stage_I)

	sortedParticipants := sortByScore(participantList)

	// wypisywanie

	for i, p := range participantList {
		fmt.Printf("\n============ Uczestnik-%d ============\n", i+1)
		fmt.Println("Name: ", p.name)
		fmt.Println("Repertoire: ", p.repertoire)
		fmt.Println("Scores: ", p.scores)
		fmt.Println("Final Score: ", p.finalScore)
	}

	fmt.Println("\n\nPosortowani uczestnicy:")

	for i, p := range sortedParticipants {
		fmt.Printf("\n============ Uczestnik-%d ============\n", i+1)
		fmt.Println("Name: ", p.name)
		fmt.Println("Repertoire: ", p.repertoire)
		fmt.Println("Scores: ", p.scores)
		fmt.Println("Final Score: ", p.finalScore)
	}

	var checkedSong string = "Mozartini"
	bestByBach, score := bestBySong(participantList, checkedSong)
	fmt.Println("\n================ BestBySong ===============")
	fmt.Printf("Najwięcej za %s uzyskał: %s (%d pkt.)\n", checkedSong, bestByBach.name, score)
}
