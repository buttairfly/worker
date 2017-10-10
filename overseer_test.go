package worker

import (
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOverseerRun(t *testing.T) {
	cases := []struct {
		desc       string
		f          func(work Work) Work
		numWorker  int
		numSamples int
		expected   Work
	}{
		{
			desc:       "lots_of_stuff",
			f:          addNumbers,
			numWorker:  4,
			numSamples: 100,
		},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			wg := new(sync.WaitGroup)
			in := make(chan Work, c.numSamples+1)
			out := make(chan Work, c.numSamples)
			fill(c.numSamples, in)
			t.Logf("1")
			o := NewOverseer(in, out, c.numWorker, c.f)
			t.Logf("2")
			wg.Add(1)
			t.Logf("3")
			o.Run(wg)
			t.Logf("4")
			wg.Wait()
			read(t, c.expected, c.numSamples, out)
		})
	}
}

func addNumbers(work Work) Work {
	const iterations = 1
	switch reflect.TypeOf(work).Kind() {
	case reflect.Int:
		for i := 0; i < iterations; i++ {
			p := reflect.ValueOf(work).Int()
			p++
		}
	default:

	}
	return work
}

func fill(n int, in chan Work) {
	for i := 0; i < n; i++ {
		in <- 0
	}
	close(in)
}

func read(t *testing.T, expected Work, numReadExpected int, out chan Work) {
	numRead := 0
	for i := range out {
		numRead++
		assert.Equal(t, expected, i)
	}
	assert.Equal(t, numReadExpected, numRead)
}
