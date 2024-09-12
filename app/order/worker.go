package order

import (
	"context"

	"github.com/temporalio/reference-app-orders-go/app/config"
	"github.com/temporalio/reference-app-orders-go/app/temporalutil"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// RunWorker runs a Workflow and Activity worker for the Order system.
func RunWorker(ctx context.Context, config config.AppConfig, client client.Client) error {
	w := worker.New(client, TaskQueue, worker.Options{})

	w.RegisterWorkflow(Order)
	w.RegisterActivity(&Activities{BillingURL: config.BillingURL, OrderURL: config.OrderURL})

	return w.Run(temporalutil.WorkerInterruptFromContext(ctx))
}

// RunBatchOrderWorker runs a Workflow and Activity worker for the BatchOrder system.
func RunBatchOrderWorker(ctx context.Context, config config.AppConfig, client client.Client) error {
	w := worker.New(client, TaskQueueBatchOrders, worker.Options{})

	w.RegisterWorkflow(BatchOrders)
	w.RegisterActivity(&Activities{BillingURL: config.BillingURL, OrderURL: config.OrderURL, TemporalClient: client})

	return w.Run(temporalutil.WorkerInterruptFromContext(ctx))
}
