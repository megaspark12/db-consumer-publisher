package main

import (
	"fmt"
	"time"

	"firefly.io/atmos/mongo"
)

func main() {
	mongoService, err := mongo.NewMongoService("localhost", 27017, "admin", "admin")
	if err != nil {
		panic(err)
	}

	result, err := mongoService.Consume("db1", "c1", 10*time.Second)
	if err != nil {
		panic(err)
	}
	for _, doc := range result {
		fmt.Println(string(doc))
		fmt.Println()
	}
}
