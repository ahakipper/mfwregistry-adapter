package gpool

import "sync"

// task is a job in the pool
type task struct {
	f func()
}

// newTask create a task
func newTask(f func()) *task {
	t := task{
		f: f,
	}
	return &t
}

// execute task
func (t *task) execute() {
	t.f()
}

// goroutine pool
type Pool struct {
	wg sync.WaitGroup

	// jobs use for receive task
	jobs chan *task

	// workerNum represent how many goroutines
	workerNum int8
}

// NewPool create a goroutine pool
func NewPool(num int8) *Pool {
	return &Pool{
		jobs:      make(chan *task, num*3),
		workerNum: num,
	}
}

// worker execute pool job
func (p *Pool) worker() {
	p.wg.Add(1)
	for job := range p.jobs {
		job.execute()
	}
	p.wg.Done()
}

// Submit add a job to the pool
func (p *Pool) Submit(job func()) {
	p.jobs <- newTask(job)
}

// Run create pool workers
func (p *Pool) Run() {
	for i := 0; i < int(p.workerNum); i++ {
		go p.worker()
	}

}

// Close pool stop receive jobs and wait for job finish
func (p *Pool) Close() {
	close(p.jobs)
	p.wg.Wait()
}
