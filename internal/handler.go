package internal

import (
	"fmt"
	"time"
)

type RouteCreatedEvent struct {
	EventName  string       `json:"eventName"`
	RouteId    string       `json:"id"`
	Distance   int          `json:"distance"`
	Directions []Directions `json:"directions"`
}

func NewRouteCreatedEvent(routeId string, distance int, directions []Directions) *RouteCreatedEvent {
	return &RouteCreatedEvent{
		EventName:  "RouteCreated",
		RouteId:    routeId,
		Distance:   distance,
		Directions: directions,
	}
}

type FreightCalculatedEvent struct {
	EventName string  `json:"eventName"`
	RouteId   string  `json:"routeId"`
	Amount    float64 `json: "amount"`
}

func NewFreightCalculatedEvent(routeId string, amount float64) *FreightCalculatedEvent {
	return &FreightCalculatedEvent{
		EventName: "FreightCalculated",
		RouteId:   routeId,
		Amount:    amount,
	}
}

type DeliveryStartedEvent struct {
	EventName string `json:"eventName"`
	RouteId   string `json:"routeId"`
}

func NewDeliveryStartedEvent(routeId string) *DeliveryStartedEvent {
	return &DeliveryStartedEvent{
		EventName: "DeliveryStarted",
		RouteId:   routeId,
	}
}

type DriverMovedEvent struct {
	EventName string  `json:"eventName"`
	RouteId   string  `json:"routeId"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
}

func NewDriverMovedEvent(routeId string, lat float64, lng float64) *DriverMovedEvent {
	return &DriverMovedEvent{
		EventName: "DriverMoved",
		RouteId:   routeId,
		Lat:       lat,
		Lng:       lng,
	}
}

func RouteCreatedHandler(event *RouteCreatedEvent, routeService *RouteService) (*FreightCalculatedEvent, error) {
	route := NewRoute(event.RouteId, event.Distance, event.Directions)
	routeCreated, err := routeService.CreateRoute(route)
	if err != nil {
		return nil, err
	}
	freightCalculatedEvent := NewFreightCalculatedEvent(routeCreated.Id, routeCreated.FreightPrice)
	return freightCalculatedEvent, nil

}

func DeliveryStartedHandler(event *DeliveryStartedEvent, routeService *RouteService, ch chan *DriverMovedEvent) error {
	route, err := routeService.GetRoute(event.RouteId)

	fmt.Println(route)
	if err != nil {
		return err
	}
	go func() {
		for _, direction := range route.Directions {
			driverMovedEvent := NewDriverMovedEvent(route.Id, direction.Lat, direction.Lng)
			ch <- driverMovedEvent
			time.Sleep(time.Second)

		}
	}()
	return nil
}
