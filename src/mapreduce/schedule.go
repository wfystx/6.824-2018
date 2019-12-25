package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	tasks := make(chan int)
	var wg sync.WaitGroup
	go func() {
		for taskNumber := 0; taskNumber < ntasks; taskNumber++ {
			tasks <- taskNumber
			fmt.Printf("taskChan <- %d in %s\n", taskNumber, phase)
			wg.Add(1)

		}
		wg.Wait()
		close(tasks)
	}()

	for task := range tasks {
		worker := <- registerChan
		fmt.Printf("given task %d to %s in %s\n", task, worker, phase)
		var args DoTaskArgs
		args.JobName = jobName
		args.Phase = phase
		args.TaskNumber = task
		if phase == mapPhase {
			args.File = mapFiles[task]
		}
		args.NumOtherPhase = n_other

		go func(worker string, args DoTaskArgs) {
			ok := call(worker, "Worker.DoTask", args, nil)
			if ok {
				wg.Done()
				fmt.Printf("Schedule: RPC %s DoTask %d succeeded\n", worker, task)
				registerChan <- worker
			} else {
				tasks <- args.TaskNumber
				fmt.Printf("Schedule: RPC %s DoTask %d error\n", worker, task)
			}
		}(worker, args)
	}

	//wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
