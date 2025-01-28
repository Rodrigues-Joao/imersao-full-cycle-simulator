package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/devfullcycle/imersao20/simulator/internal"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	mongoURI := getEnv("MONGO_URI", "mongodb://admin:admin@mongo:27017/routes?authSource=admin")
	kafkaBroker := getEnv("KAFKA_BROKER", "kafka:9092")
	kafkaRouteTopic := getEnv("KAFKA_ROUTE_TOPIC", "route")
	kafkaFreightTopic := getEnv("KAFKA_FREIGHT_TOPIC", "freight")
	kafkaSimulationTopic := getEnv("KAFKA_SIMULATION_TOPIC", "simulation")
	kafkaGroupID := getEnv("KAFKA_GROUP_ID", "route-group")

	mongoConnection, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		panic(err)
	}
	freightService := internal.NewFreightService()
	routeService := internal.NewRouteService(mongoConnection, freightService)
	chDriverMoved := make(chan *internal.DriverMovedEvent)

	freightWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaFreightTopic,
		Balancer: &kafka.LeastBytes{},
	}

	simulatortWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaSimulationTopic,
		Balancer: &kafka.LeastBytes{},
	}

	routeReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   kafkaRouteTopic,
		GroupID: kafkaGroupID,
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
			err = hub.HandleEvent(msg)
			if err != nil {
				log.Printf("Error handling event: %v", err)
			}
		}(m.Value)
	}

}
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
