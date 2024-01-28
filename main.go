package main

import (
	"fmt"
	"time"

	"firefly.io/atmos/mongo"
)

func main() {
	mongoService, err := mongo.NewMongoService("localhost", 27017, "", "")
	if err != nil {
		panic(err)
	}

	mongoService.Purge("db1", "c1")
	fmt.Println("purged")
	err = mongoService.Populate("db1", "c1")
	if err != nil {
		panic(err)
	}

	// resultChan := make(chan any)
	// go func() {
	// 	fmt.Println(time.Now())
	// 	detailedMessages, err := mongoService.Consume("db1", "c1", 5*time.Second)
	// 	if err != nil {
	// 		resultChan <- err
	// 		return
	// 	}
	// 	resultChan <- detailedMessages
	// }()

	resultChan, errChan := mongoService.AsyncDetailedConsume("db1", "c1", 5*time.Second)

	time.Sleep(3 * time.Second)
	fmt.Println("purged again")
	fmt.Println(time.Now())
	mongoService.Purge("db1", "c1")

	time.Sleep(3 * time.Second)
	fmt.Println("re-populate")
	mongoService.PupolateBen("db1", "c1")

	result, err := mongo.WaitUntilDone(resultChan, errChan)
	if err != nil {
		panic(err)
	}
	for _, doc := range result {
		fmt.Println(string(doc.Body))
		fmt.Println(doc.Timestamp)
		fmt.Println()
	}

	// result := <-resultChan
	// switch v := result.(type) {
	// case error:
	// 	panic(v)
	// case []*mongo.DetailedMessage[[]byte]:
	// 	for _, doc := range v {
	// 		fmt.Println(string(doc.Body))
	// 		fmt.Println(doc.Timestamp)
	// 		fmt.Println()
	// 	}
	// case [][]byte:
	// 	fmt.Println("[][]byte")
	// 	for _, doc := range v {
	// 		fmt.Println(string(doc))
	// 		fmt.Println()
	// 	}
	// default:
	// 	panic(errors.New("unknown type"))
	// }
	// fmt.Println(time.Now())
}
