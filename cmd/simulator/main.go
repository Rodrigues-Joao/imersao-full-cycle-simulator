package main

import (
	"context"
	"fmt"
	"log"

	"github.com/devfullcycle/imersao20/simulator/internal"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	mongoStr := "mongodb://admin:admin@localhost:27017/routes?authSource=admin"
	mongoConnection, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoStr))
	if err != nil {
		panic(err)
	}
	freightService := internal.NewFreightService()
	routeService := internal.NewRouteService(mongoConnection, freightService)
	chDriverMoved := make(chan *internal.DriverMovedEvent)

	freightWriter := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "freight",
		Balancer: &kafka.LeastBytes{},
	}

	simulatortWriter := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "simulator",
		Balancer: &kafka.LeastBytes{},
	}

	routeReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "route",
		GroupID: "simulator",
	})
	hub := internal.NewEventHub(mongoConnection, routeService, chDriverMoved, freightWriter, simulatortWriter)
	fmt.Println("Starting simulator")
	for {
		m, err := routeReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}
		go func(msg []byte) {
			err = hub.HandleEvent(m.Value)
			if err != nil {
				log.Printf("Error handling event: %v", err)
			}
		}(m.Value)
	}

}
