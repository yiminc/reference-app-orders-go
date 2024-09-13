package temporalutil

import (
	// "go.temporal.io/sdk/contrib/resourcetuner"
	"go.temporal.io/sdk/worker"
)

func GetWorkerOptions() (worker.Options, error) {
	// tuner, err := resourcetuner.NewResourceBasedTuner(resourcetuner.ResourceBasedTunerOptions{
	// 	TargetMem: 0.7,
	// 	TargetCpu: 0.7,
	// })
	// if err != nil {
	// 	return worker.Options{}, err
	// }
	// return worker.Options{Tuner: tuner, MaxConcurrentActivityTaskPollers: 20, MaxConcurrentWorkflowTaskPollers: 20}, nil
	return worker.Options{MaxConcurrentActivityTaskPollers: 20, MaxConcurrentWorkflowTaskPollers: 20}, nil
}
