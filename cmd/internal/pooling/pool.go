package pooling

import "sync"

type WorkerPool struct {
	limiter       chan struct{}
	wg            sync.WaitGroup
	errMutex      sync.RWMutex
	err           error
	skipWhenError bool
}

func NewWorkerPool(size int, skipWhenError bool) WorkerPool {
	if size < 1 {
		size = 1
	}

	return WorkerPool{
		limiter:       make(chan struct{}, size),
		skipWhenError: skipWhenError,
	}
}

func (p *WorkerPool) Wait() error {
	p.wg.Wait()
	return p.err
}

// func (p *WorkerPool) Add(work func() error) {
// 	p.wg.Add(1)
// 	go func(fn func() error) {

// 	}
// }
