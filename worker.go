package worker

import (
	"fmt"
	"sync"

	"bitbucket.org/smartcast/sledgehammer-common/log"
)

type Id string
type token struct{}

type worker struct {
	poolName             string
	id                   int
	rTokenOut, wTokenOut chan token
	rTokenIn, wTokenIn   chan token
	input, output        chan Work
	f                    func(Id, Work) Work
}

func newWorker(poolName string, id int, input, output chan Work, f func(Id, Work) Work) *worker {
	return &worker{
		poolName:  poolName,
		id:        id,
		rTokenOut: make(chan token, 1),
		wTokenOut: make(chan token, 1),
		input:     input,
		output:    output,
		f:         f,
	}
}

func (w *worker) setToken() {
	w.rTokenIn <- token{}
	w.wTokenIn <- token{}
}

func (w *worker) setWire(rIn, wIn chan token) {
	w.rTokenIn = rIn
	w.wTokenIn = wIn
}

func (w *worker) getWire() (rOut, wOut chan token) {
	return w.rTokenOut, w.wTokenOut
}

func (w *worker) run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(w.wTokenOut)
	defer close(w.rTokenOut)
	log.Tracef("start worker %v", w.getId())
	defer log.Tracef("stop worker %v", w.getId())
	for {
		workTodo := w.readInput()
		if workTodo == nil {
			break
		}
		workDone := w.f(w.getId(), workTodo)
		w.writeOutput(workDone)
	}
}

func (w *worker) getId() Id {
	return Id(fmt.Sprintf("%v:%v", w.poolName, w.id))
}

func (w *worker) readInput() (work Work) {
	<-w.rTokenIn
	work = <-w.input
	w.rTokenOut <- token{}
	return
}

func (w *worker) writeOutput(work Work) {
	<-w.wTokenIn
	w.output <- work
	w.wTokenOut <- token{}
}
