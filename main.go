package main

import (
	"errors"
	"fmt"
	"time"

	"firefly.io/atmos/mongo"
)

func main() {
	mongoService, err := mongo.NewMongoService("localhost", 27017, "admin", "admin")
	if err != nil {
		panic(err)
	}

	mongoService.Purge("db1", "c1")
	fmt.Println("purged")
	err = mongoService.Populate("db1", "c1")
	if err != nil {
		panic(err)
	}

	resultChan := make(chan any)
	go func() {
		fmt.Println(time.Now())
		detailedMessages, err := mongoService.DetailedConsume("db1", "c1", 5*time.Second)
		if err != nil {
			resultChan <- err
			return
		}
		resultChan <- detailedMessages
	}()

	time.Sleep(3 * time.Second)
	fmt.Println("purged again")
	fmt.Println(time.Now())
	mongoService.Purge("db1", "c1")

	time.Sleep(3 * time.Second)
	fmt.Println("re-populate")
	mongoService.PupolateBen("db1", "c1")

	result := <-resultChan
	switch v := result.(type) {
	case error:
		panic(v)
	case []*mongo.DetailedMessage:
		for _, doc := range v {
			// fmt.Println(string(doc.Body))
			fmt.Println(doc.Timestamp)
			fmt.Println()
		}
	default:
		panic(errors.New("unknown type"))
	}
	fmt.Println(time.Now())
}
