package worker

import (
	"log"
	"sync"
)

type worker struct {
	id                   string
	rTokenOut, wTokenOut chan struct{}
	rTokenIn, wTokenIn   chan struct{}
	input, output        chan Work
	f                    func(work Work) Work
}

func NewWorker(id string, input, output chan Work, f func(Work) Work) *worker {
	return &worker{
		id:        id,
		rTokenOut: make(chan struct{}),
		wTokenOut: make(chan struct{}),
		input:     input,
		output:    output,
		f:         f,
	}
}

func (self *worker) SetToken() {
	go func() {
		self.wTokenIn <- struct{}{}
		self.rTokenIn <- struct{}{}
	}()
}

func (self *worker) SetWire(rIn, wIn chan struct{}) {
	self.rTokenIn = rIn
	self.wTokenIn = wIn
}

func (self *worker) GetWire() (rOut, wOut chan struct{}) {
	return self.rTokenOut, self.wTokenOut
}

func (self *worker) Run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(self.wTokenOut)
	defer close(self.rTokenOut)
	log.Printf("start %v", self.id)
	defer log.Printf("stop %v", self.id)
	for {
		workTodo := self.readInput()
		if workTodo == nil {
			break
		}
		workDone := self.f(workTodo)
		self.writeOutput(workDone)
	}
}

func (self *worker) readInput() (work Work) {
	<-self.rTokenIn
	work = <-self.input
	self.rTokenOut <- struct{}{}
	return
}

func (self *worker) writeOutput(work Work) {
	<-self.wTokenIn
	self.output <- work
	self.wTokenOut <- struct{}{}
}
