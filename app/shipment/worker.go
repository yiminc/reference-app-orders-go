package shipment

import (
	"context"

	"github.com/temporalio/reference-app-orders-go/app/config"
	"github.com/temporalio/reference-app-orders-go/app/temporalutil"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/resourcetuner"
	"go.temporal.io/sdk/worker"
)

// Config is the configuration for the Order system.
type Config struct {
	ShipmentURL string
}

// RunWorker runs a Workflow and Activity worker for the Shipment system.
func RunWorker(ctx context.Context, config config.AppConfig, client client.Client) error {
	tuner, err := resourcetuner.NewResourceBasedTuner(resourcetuner.ResourceBasedTunerOptions{
		TargetMem: 0.9,
		TargetCpu: 0.9,
	})
	if err != nil {
		return err
	}

	w := worker.New(client, TaskQueue, worker.Options{Tuner: tuner})

	w.RegisterWorkflow(Shipment)
	w.RegisterActivity(&Activities{ShipmentURL: config.ShipmentURL})

	return w.Run(temporalutil.WorkerInterruptFromContext(ctx))
}
