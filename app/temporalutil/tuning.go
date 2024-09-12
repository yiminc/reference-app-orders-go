package temporalutil

import (
	"go.temporal.io/sdk/contrib/resourcetuner"
	"go.temporal.io/sdk/worker"
)

func GetWorkerOptions() (worker.Options, error) {
	tuner, err := resourcetuner.NewResourceBasedTuner(resourcetuner.ResourceBasedTunerOptions{
		TargetMem: 0.9,
		TargetCpu: 0.9,
	})
	if err != nil {
		return worker.Options{}, err
	}
	return worker.Options{Tuner: tuner}, nil
	//return worker.Options{}, nil
}
