package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
)

type EventHub struct {
	routeService    *RouteService
	mongoClient     *mongo.Client
	chDriverMoved   chan *DriverMovedEvent
	freightWriter   *kafka.Writer
	simulatorWriter *kafka.Writer
}

func NewEventHub(mongoClient *mongo.Client, routeService *RouteService, chDriverMoved chan *DriverMovedEvent, freightWriter *kafka.Writer, simulatorWriter *kafka.Writer) *EventHub {
	return &EventHub{
		routeService:    routeService,
		mongoClient:     mongoClient,
		chDriverMoved:   chDriverMoved,
		freightWriter:   freightWriter,
		simulatorWriter: simulatorWriter,
	}
}
func (eh *EventHub) HandleEvent(msg []byte) error {

	var baseEvent struct {
		EventName string `json:"eventName"`
	}
	err := json.Unmarshal(msg, &baseEvent)
	if err != nil {
		return fmt.Errorf("Error unmarshalling event: %w", err)

	}

	switch baseEvent.EventName {
	case "RouteCreated":
		var event RouteCreatedEvent
		err := json.Unmarshal(msg, &event)
		if err != nil {
			return fmt.Errorf("Error unmarshalling event: %w", err)

		}
		return eh.handleRouteCreated(event)
	case "DeliveryStarted":
		var event DeliveryStartedEvent
		err := json.Unmarshal(msg, &event)
		if err != nil {
			return fmt.Errorf("Error unmarshalling event: %w", err)

		}
		return eh.handleDeliveryStarted(event)

	default:
		return errors.New("Unknown event")
	}

}

func (eh *EventHub) handleRouteCreated(event RouteCreatedEvent) error {
	fmt.Println("handleRouteCreated")
	freightCalculatedEvent, err := RouteCreatedHandler(&event, eh.routeService)
	if err != nil {
		return err

	}
	value, err := json.Marshal(freightCalculatedEvent)
	if err != nil {
		return fmt.Errorf("Error marshalling event: %w", err)
	}
	err = eh.freightWriter.WriteMessages(context.Background(), kafka.Message{Key: []byte(freightCalculatedEvent.RouteId),
		Value: value})
	if err != nil {
		return fmt.Errorf("Error writing message: %w", err)
	}

	return nil
}
func (eh *EventHub) handleDeliveryStarted(event DeliveryStartedEvent) error {
	err := DeliveryStartedHandler(&event, eh.routeService, eh.chDriverMoved)
	if err != nil {
		return fmt.Errorf("handleDeliveryStarted  message: %w", err)

	}
	go eh.sendDirections() // roda em background (go rotina)
	return nil
}
func (eh *EventHub) sendDirections() {
	for {
		select {
		case movedEvent := <-eh.chDriverMoved:
			value, _ := json.Marshal(movedEvent)

			if err := eh.simulatorWriter.WriteMessages(context.Background(), kafka.Message{Key: []byte(movedEvent.RouteId),
				Value: value}); err != nil {
				return

			}
		case <-time.After(500 * time.Microsecond):
			return
		}
	}

}
