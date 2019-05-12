package jobqueue

type Job interface {
	Process()
}

type JobQueue struct {
	Queue chan Job
}

func New(maxWorker, maxQueue int) *JobQueue {
	jobq := &JobQueue{
		Queue: make(chan Job, maxQueue),
	}

	dispatcher := NewDispatcher(maxWorker, jobq)
	dispatcher.Run()

	return jobq
}

func (jobq *JobQueue) Put(job Job) {
	jobq.Queue <- job
}

type Dispatcher struct {
	WorkerPool chan chan Job
	MaxWorkers int
	JobQ       *JobQueue
}

func NewDispatcher(maxWorkers int, jobq *JobQueue) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{
		WorkerPool: pool,
		MaxWorkers: maxWorkers,
		JobQ:       jobq,
	}
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-d.JobQ.Queue:
			// a job request has been received
			go func(job Job) {
				jobChannel := <-d.WorkerPool

				jobChannel <- job
			}(job)
		}
	}
}

type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
	}
}

func (w Worker) Start() {
	go func() {
		for {
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				job.Process()
			case <-w.quit:
				return
			}
		}
	}()
}

func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}
