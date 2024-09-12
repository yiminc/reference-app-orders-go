package billing

import (
	"context"

	"github.com/temporalio/reference-app-orders-go/app/config"
	"github.com/temporalio/reference-app-orders-go/app/temporalutil"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// RunWorker runs a Workflow and Activity worker for the Billing system.
func RunWorker(ctx context.Context, config config.AppConfig, client client.Client) error {
	options, err := temporalutil.GetWorkerOptions()
	if err != nil {
		return err
	}

	w := worker.New(client, TaskQueue, options)

	w.RegisterWorkflow(Charge)
	w.RegisterActivity(&Activities{FraudCheckURL: config.FraudURL})

	return w.Run(temporalutil.WorkerInterruptFromContext(ctx))
}
