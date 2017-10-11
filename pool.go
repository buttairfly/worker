package worker

import "sync"

type Work interface{}

type pool struct {
	input, output chan Work
	workers       []*worker
}

func NewPool(
	name string,
	input, output chan Work,
	numWorker int,
	f func(workerId Id, work Work) Work,
) *pool {
	w := make([]*worker, numWorker)
	for i := range w {
		w[i] = newWorker(name, i, input, output, f)
		if i > 0 {
			w[i].setWire(w[i-1].getWire())
		}
	}
	w[0].setWire(w[numWorker-1].getWire())
	w[0].setToken()
	return &pool{
		input:   input,
		output:  output,
		workers: w,
	}
}

func (p *pool) Run() {
	defer close(p.output)
	poolWg := new(sync.WaitGroup)
	for _, w := range p.workers {
		poolWg.Add(1)
		go w.run(poolWg)
	}
	poolWg.Wait()
}
