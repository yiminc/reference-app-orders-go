package order

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/temporalio/reference-app-orders-go/app/billing"
	"github.com/temporalio/reference-app-orders-go/app/shipment"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

type orderImpl struct {
	id           string
	customerID   string
	status       string
	fulfillments []*Fulfillment
	logger       log.Logger
}

type batchOrderImpl struct {
	orderInfo []*orderImpl // stores info related to multiple orders
}

// Aggressively low for demo purposes.
const customerActionTimeout = 30 * time.Second

// SplitOrderIds is a helper to split orderIds for multiple parallel activities
func SplitOrderIds(orderIds []string, orders int, concurrentActivities int) ([][]string, error) {
	concurrentActivities = min(concurrentActivities, orders)
	orderIDSplits := make([][]string, concurrentActivities)
	maxWindowSize := (orders + concurrentActivities - 1) / concurrentActivities

	// sliding window technique
	start := 0
	for i := 0; i < len(orderIds); i++ {
		end := start + maxWindowSize
		if end > len(orderIds) {
			end = len(orderIds)
		}
		// Append the slice of orderIds for this activity
		orderIDSplits[i] = append(orderIDSplits[i], orderIds[start:end]...)

		// Move the start index to the next batch
		start = end

		// If there are no more orders left to distribute, break early
		if start >= len(orderIds) {
			break
		}
	}

	return orderIDSplits, nil
}

func BatchOrders(ctx workflow.Context, orders int) (*BatchOrderResult, error) {

	// TODO Shivam - this currently only runs one activity
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting workflow execution!")

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	orderIds := make([]string, orders)

	encodedOrderIds := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		var encodedOrderIds []string
		for i := 0; i < orders; i++ {
			encodedOrderIds = append(encodedOrderIds, uuid.New().String())
		}
		return encodedOrderIds
	})
	err := encodedOrderIds.Get(&orderIds)
	if err != nil {
		return nil, fmt.Errorf("SideEffects failed while generating orderIds with err: %w", err)
	}

	// TODO Shivam - helper function
	concurrentActivities := 2 // Kept 2, as default.
	orderIDSplits, err := SplitOrderIds(orderIds, orders, concurrentActivities)
	if err != nil {
		logger.Error("SplitOrderIds failed with error:", err)
		return nil, err
	}

	// Use workflow.Go to launch concurrent activity executions instead of goroutines
	var futures []workflow.Future
	var a Activities

	for _, split := range orderIDSplits {
		future := workflow.ExecuteActivity(ctx, a.StartOrders, split)
		futures = append(futures, future)
	}

	// Wait for all futures to complete
	finalBatchOrderResult := BatchOrderResult{OrderResults: make([]*OrderResult, 0)}
	for _, future := range futures {
		var batchOrderResult *BatchOrderResult
		if err := future.Get(ctx, &batchOrderResult); err != nil {
			return nil, err
		}
		logger.Info("Activity returned with result: ", batchOrderResult)
		finalBatchOrderResult.OrderResults = append(finalBatchOrderResult.OrderResults, batchOrderResult.OrderResults...)
	}

	logger.Info("Completed processing all batch orders")
	return &finalBatchOrderResult, nil

	//future := workflow.ExecuteActivity(ctx, a.StartOrders, orderIds)
	//futures = append(futures, future)
	//
	//logger.Info("Activities Started")
	//
	//// accumulating the results
	//for _, future := range futures {
	//	var orderResult OrderResult
	//	err := future.Get(ctx, &orderResult)
	//	if err != nil {
	//		fmt.Printf("Executing Activity failed with the error %s\n", err)
	//		return nil, err
	//	}
	//	batchOrderResult.OrderResults = append(batchOrderResult.OrderResults, &orderResult)
	//}
	//logger.Info("Completed processing all batch orders")
	//return &batchOrderResult, nil
}

// Order Workflow process an order from a customer.
func Order(ctx workflow.Context, input *OrderInput) (*OrderResult, error) {
	wf := new(orderImpl)

	if err := wf.setup(ctx, input); err != nil {
		return nil, err
	}

	return wf.run(ctx, input)
}

func (wf *orderImpl) setup(ctx workflow.Context, input *OrderInput) error {
	if input.ID == "" {
		return fmt.Errorf("ID is required")
	}

	if input.CustomerID == "" {
		return fmt.Errorf("CustomerID is required")
	}

	if len(input.Items) == 0 {
		return fmt.Errorf("order must contain items")
	}

	wf.id = input.ID
	wf.customerID = input.CustomerID

	wf.logger = log.With(
		workflow.GetLogger(ctx),
		"orderID", wf.id,
		"customerId", wf.customerID,
	)

	return workflow.SetQueryHandler(ctx, StatusQuery, func() (*OrderStatus, error) {
		return &OrderStatus{
			ID:           wf.id,
			Status:       wf.status,
			CustomerID:   wf.customerID,
			Fulfillments: wf.fulfillments,
		}, nil
	})
}

func (wf *orderImpl) run(ctx workflow.Context, order *OrderInput) (*OrderResult, error) {
	err := wf.buildFulfillments(ctx, order.Items)
	if err != nil {
		return nil, err
	}

	if wf.customerActionRequired() {
		err = wf.updateStatus(ctx, OrderStatusCustomerActionRequired)
		if err != nil {
			return nil, err
		}

		// TODO Shivam - don't understand this fully *yet*
		action, err := wf.waitForCustomer(ctx)
		if err != nil {
			return nil, err
		}

		switch action {
		case CustomerActionCancel:
			err := wf.updateStatus(ctx, OrderStatusCancelled)
			return &OrderResult{Status: wf.status}, err
		case CustomerActionTimedOut:
			err := wf.updateStatus(ctx, OrderStatusTimedOut)
			wf.cancelAllFulfillments()
			return &OrderResult{Status: wf.status}, err
		case CustomerActionAmend:
			wf.cancelUnavailableFulfillments()
		default:
			return nil, fmt.Errorf("unhandled customer action %q", action)
		}
	}

	if err := wf.updateStatus(ctx, OrderStatusProcessing); err != nil {
		return nil, err
	}

	workflow.Go(ctx, wf.handleShipmentStatusUpdates)

	completed := 0
	for _, f := range wf.fulfillments {
		f := f
		workflow.Go(ctx, func(ctx workflow.Context) {
			f.process(ctx)
			completed++
		})
	}

	workflow.Await(ctx, func() bool { return completed == len(wf.fulfillments) })

	status := OrderStatusCompleted
	if wf.allFulfillmentsFailed() {
		status = OrderStatusFailed
	}
	if err := wf.updateStatus(ctx, status); err != nil {
		return nil, err
	}

	return &OrderResult{Status: wf.status}, nil
}

func (wf *orderImpl) updateStatus(ctx workflow.Context, status string) error {
	wf.status = status

	update := &OrderStatusUpdate{
		ID:     wf.id,
		Status: wf.status,
	}

	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: 5 * time.Second,
	})
	return workflow.ExecuteLocalActivity(ctx, a.UpdateOrderStatus, update).Get(ctx, nil)
}

func (wf *orderImpl) buildFulfillments(ctx workflow.Context, items []*Item) error {
	ctx = workflow.WithActivityOptions(ctx,
		workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
		},
	)

	var result ReserveItemsResult

	err := workflow.ExecuteActivity(ctx,
		a.ReserveItems,
		ReserveItemsInput{
			OrderID: wf.id,
			Items:   items,
		},
	).Get(ctx, &result)
	if err != nil {
		return err
	}

	for i, r := range result.Reservations {
		id := fmt.Sprintf("%s:%d", wf.id, i+1)
		logger := log.With(wf.logger, "fulfillment", id)
		f := &Fulfillment{
			orderID:    wf.id,
			customerID: wf.customerID,
			logger:     logger,

			ID:       id,
			Items:    r.Items,
			Location: r.Location,
			Status:   FulfillmentStatusPending,
		}
		if !r.Available {
			f.Status = FulfillmentStatusUnavailable
		}
		wf.fulfillments = append(wf.fulfillments, f)
	}

	return nil
}

func (wf *orderImpl) customerActionRequired() bool {
	for _, f := range wf.fulfillments {
		if f.Status == FulfillmentStatusUnavailable {
			return true
		}
	}

	return false
}

func (wf *orderImpl) cancelUnavailableFulfillments() {
	wf.logger.Info("Cancelling unavailable fulfillments")

	for _, f := range wf.fulfillments {
		if f.Status == FulfillmentStatusUnavailable {
			f.Status = FulfillmentStatusCancelled
		}
	}
}

func (wf *orderImpl) cancelAllFulfillments() {
	wf.logger.Info("Cancelling all fulfillments")

	for _, f := range wf.fulfillments {
		f.Status = FulfillmentStatusCancelled
	}
}

func (wf *orderImpl) allFulfillmentsFailed() bool {
	failures := 0
	for _, f := range wf.fulfillments {
		if f.Status == FulfillmentStatusFailed {
			failures++
		}
	}

	return failures >= 1 && failures == len(wf.fulfillments)
}

func (wf *orderImpl) waitForCustomer(ctx workflow.Context) (string, error) {
	var signal CustomerActionSignal

	s := workflow.NewSelector(ctx)

	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	t := workflow.NewTimer(timerCtx, customerActionTimeout)

	var err error

	s.AddFuture(t, func(f workflow.Future) {
		if err = f.Get(timerCtx, nil); err != nil {
			return
		}

		wf.logger.Info("Timed out waiting for customer action", "timeout", customerActionTimeout)

		signal.Action = CustomerActionTimedOut
	})

	ch := workflow.GetSignalChannel(ctx, CustomerActionSignalName)
	s.AddReceive(ch, func(c workflow.ReceiveChannel, _ bool) {
		c.Receive(ctx, &signal)

		wf.logger.Info("Received customer action", "action", signal.Action)

		cancelTimer()
	})

	wf.logger.Info("Waiting for customer action")

	s.Select(ctx)

	if err != nil {
		return "", err
	}

	switch signal.Action {
	case CustomerActionAmend:
	case CustomerActionCancel:
	case CustomerActionTimedOut:
	default:
		return "", fmt.Errorf("invalid customer action %q", signal.Action)
	}

	return signal.Action, nil
}

func (wf *orderImpl) handleShipmentStatusUpdates(ctx workflow.Context) {
	ch := workflow.GetSignalChannel(ctx, shipment.ShipmentStatusUpdatedSignalName)

	for {
		var signal shipment.ShipmentStatusUpdatedSignal
		_ = ch.Receive(ctx, &signal)
		for _, f := range wf.fulfillments {
			if f.ID == signal.ShipmentID {
				f.Shipment.Status = signal.Status
				f.Shipment.UpdatedAt = signal.UpdatedAt

				wf.logger.Info("Shipment status updated", "shipmentID", signal.ShipmentID, "status", signal.Status)

				break
			}
		}
	}
}

func (f *Fulfillment) process(ctx workflow.Context) error {
	defer func() {
		f.logger.Info("Fulfillment processed", "status", f.Status)
	}()

	if f.Status == FulfillmentStatusCancelled {
		return nil
	}

	f.Status = FulfillmentStatusProcessing

	err := f.processPayment(ctx)
	if err != nil || f.Payment.Status != PaymentStatusSuccess {
		f.Status = FulfillmentStatusFailed
		return err
	}

	if err := f.processShipment(ctx); err != nil {
		f.Status = FulfillmentStatusFailed
		return err
	}

	f.Status = FulfillmentStatusCompleted

	return nil
}

func (f *Fulfillment) processPayment(ctx workflow.Context) error {
	var billingItems []billing.Item
	for _, i := range f.Items {
		billingItems = append(billingItems, billing.Item{SKU: i.SKU, Quantity: i.Quantity})
	}

	var charge ChargeResult

	ctx = workflow.WithActivityOptions(ctx,
		workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
		},
	)

	f.Payment = &PaymentStatus{Status: PaymentStatusPending}

	var chargeKey string
	v := workflow.SideEffect(ctx, func(_ workflow.Context) any {
		return uuid.NewString()
	})
	if err := v.Get(&chargeKey); err != nil {
		f.Payment.Status = PaymentStatusFailed
		return err
	}

	c := workflow.ExecuteActivity(ctx,
		a.Charge,
		&ChargeInput{
			CustomerID:     f.customerID,
			Reference:      f.ID,
			Items:          billingItems,
			IdempotencyKey: chargeKey,
		},
	)
	if err := c.Get(ctx, &charge); err != nil {
		f.Payment.Status = PaymentStatusFailed
		return err
	}

	p := f.Payment

	p.SubTotal = charge.SubTotal
	p.Tax = charge.Tax
	p.Shipping = charge.Shipping
	p.Total = charge.Total
	if charge.Success {
		p.Status = PaymentStatusSuccess
	} else {
		p.Status = PaymentStatusFailed
	}

	f.logger.Info("Payment processed", "total", p.Total, "status", p.Status)

	return nil
}

func (f *Fulfillment) processShipment(ctx workflow.Context) error {
	ctx = workflow.WithChildOptions(ctx,
		workflow.ChildWorkflowOptions{
			TaskQueue:  shipment.TaskQueue,
			WorkflowID: shipment.ShipmentWorkflowID(f.ID),
		},
	)

	var shippingItems []shipment.Item
	for _, i := range f.Items {
		shippingItems = append(shippingItems, shipment.Item{SKU: i.SKU, Quantity: i.Quantity})
	}

	f.Shipment = &ShipmentStatus{
		ID:        f.ID,
		Status:    shipment.ShipmentStatusPending,
		UpdatedAt: workflow.Now(ctx),
	}

	err := workflow.ExecuteChildWorkflow(ctx,
		shipment.Shipment,
		shipment.ShipmentInput{
			RequestorWID: workflow.GetInfo(ctx).WorkflowExecution.ID,

			ID:    f.ID,
			Items: shippingItems,
		},
	).Get(ctx, nil)

	f.logger.Info("Shipment processed", "status", f.Shipment.Status)

	return err
}
