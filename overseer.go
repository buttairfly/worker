package worker

import (
	"strconv"
	"sync"
)

type Work interface{}

type overseer struct {
	numWorker     int
	input, output chan Work
	f             func(in Work) Work
	workers       []*worker
}

func NewOverseer(
	input, output chan Work,
	numWorker int,
	f func(Work) Work,
) *overseer {
	workers := make([]*worker, numWorker)
	for i := range workers {
		workers[i] = NewWorker(strconv.Itoa(i), input, output, f)
		if i > 1 {
			workers[i].SetWire(workers[i-1].GetWire())
		}
	}
	workers[0].SetWire(workers[numWorker-1].GetWire())
	workers[0].SetToken()
	return &overseer{
		input:     input,
		output:    output,
		numWorker: numWorker,
		workers:   workers,
	}
}

func (self *overseer) Run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(self.output)
	overseerWg := new(sync.WaitGroup)
	for _, w := range self.workers {
		overseerWg.Add(1)
		w.Run(overseerWg)
	}
	overseerWg.Wait()
}
