package worker

import (
	"fmt"
	"reflect"
	"testing"
	"time"
	"log"

	"github.com/stretchr/testify/assert"
)

const poolName = "testPool"

var iterations int64

func TestPoolRun(t *testing.T) {
	cases := []struct {
		desc       string
		workFunc   func(Id, Work) Work
		inFunc     func(int) Work
		numWorker  int
		numSamples int
		iterations int64
		expected   []Work
	}{
		{
			desc:       "debug",
			numWorker:  1,
			workFunc:   workDebug,
			inFunc:     inZeros,
			numSamples: 2,
			expected:   []Work{wId(0), wId(0)},
		},
		{
			desc:       "debug_2worker",
			numWorker:  2,
			workFunc:   workDebug,
			inFunc:     inZeros,
			numSamples: 4,
			expected:   []Work{wId(0), wId(1), wId(0), wId(1)},
		},
		{
			desc:       "debug_4worker",
			numWorker:  4,
			workFunc:   workDebug,
			inFunc:     inSampleNum,
			numSamples: 10,
			expected: []Work{
				wId(0), wId(1), wId(2), wId(3),
				wId(0), wId(1), wId(2), wId(3),
				wId(0), wId(1),
			},
		},
		{
			desc:       "inOut_4worker",
			numWorker:  4,
			workFunc:   workInOut,
			inFunc:     inSampleNum,
			numSamples: 10,
			expected:   []Work{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			desc:       "addNumbers",
			iterations: 100000,
			workFunc:   workAddNumbers,
			inFunc:     inSampleNum,
			numWorker:  16,
			numSamples: 100,
		},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			iterations = c.iterations
			in := make(chan Work, c.numSamples)
			out := make(chan Work, c.numSamples)
			pool := NewPool(poolName, in, out, c.numWorker, c.workFunc)
			go pool.Run()
			go fillPoolInput(c.numSamples, in, c.inFunc)
			readPoolAndAssert(t, c.expected, c.numSamples, out, true)
		})
	}
}

func BenchmarkPoolRun(b *testing.B) {
	/*
		start benchmark with
		go test -bench BenchmarkPoolRun -run=^$ .
	*/
	const (
		it      = int64(100000000)
		samples = 1000
	)

	cases := []struct {
		desc       string
		workFunc   func(Id, Work) Work
		inFunc     func(int) Work
		numWorker  int
		numSamples int
		iterations int64
		expected   []Work
	}{
		{
			desc:       "addNumbers_1Worker",
			iterations: it,
			workFunc:   workAddNumbers,
			inFunc:     inSampleNum,
			numWorker:  1,
			numSamples: samples,
		},
		{
			desc:       "addNumbers_4Worker",
			iterations: it,
			workFunc:   workAddNumbers,
			inFunc:     inSampleNum,
			numWorker:  4,
			numSamples: samples,
		},
		{
			desc:       "addNumbers_8Worker",
			iterations: it,
			workFunc:   workAddNumbers,
			inFunc:     inSampleNum,
			numWorker:  16,
			numSamples: samples,
		},
		{
			desc:       "addNumbers_32Worker",
			iterations: it,
			workFunc:   workAddNumbers,
			inFunc:     inSampleNum,
			numWorker:  32,
			numSamples: samples,
		},
	}
	for _, c := range cases {
		b.Run(c.desc, func(b *testing.B) {
			iterations = c.iterations
			in := make(chan Work, c.numSamples)
			out := make(chan Work, c.numSamples)
			startTime := time.Now()
			pool := NewPool(poolName, in, out, c.numWorker, c.workFunc)
			go pool.Run()
			go fillPoolInput(c.numSamples, in, c.inFunc)
			readPoolAndAssert(b, c.expected, c.numSamples, out, false)
			duration := time.Now().Sub(startTime)
			b.Logf("numWorker %v took %v", c.numWorker, duration)
		})
	}
}

func fillPoolInput(n int, in chan Work, inFunc func(int) Work) {
	defer close(in)
	for i := 0; i < n; i++ {
		in <- inFunc(i)
	}
}

func readPoolAndAssert(t assert.TestingT, expected []Work, numReadExpected int, out chan Work, printVal bool) {
	numRead := 0
	if expected == nil {
		expected = getExpectedWorkDoneAddNumbers(numReadExpected)
	}
	for work := range out {
		assert.Equal(t, expected[numRead], work)
		if printVal {
			log.Printf("%v", work)
		}
		numRead++
	}
	assert.Equal(t, numReadExpected, numRead)
}

func wId(id int) Id {
	return Id(fmt.Sprintf("%v:%v", poolName, id))
}

func inZeros(_ int) Work {
	return 0
}

func inSampleNum(sampleNum int) Work {
	return sampleNum
}

func workDebug(id Id, _ Work) Work {
	return id
}

func workInOut(_ Id, work Work) Work {
	return work
}

func workAddNumbers(id Id, work Work) Work {
	switch reflect.TypeOf(work).Kind() {
	case reflect.Int:
		p := reflect.ValueOf(work).Int()
		for i := int64(0); i < iterations; i++ {
			p += i
		}
		return p
	default:
		log.Fatalf("[%v] unknown type %v of work %v", id, reflect.TypeOf(work), work)
	}
	return work
}

func getExpectedWorkDoneAddNumbers(num int) []Work {
	p := int64(0)
	for i := int64(0); i < iterations; i++ {
		p += i
	}
	work := make([]Work, num)
	for i := range work {
		work[i] = p + int64(i)
	}
	return work
}
