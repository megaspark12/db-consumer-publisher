package main

func main() {
	// mongoService, err := mongo.NewMongoService("localhost", 27017, "", "")
	// if err != nil {
	// 	panic(err)
	// }

	// mongoService.Purge("db1", "c1")
	// fmt.Println("purged")

	// docsChan, errChan := mongoService.AsyncDetailedConsume("db1", "c1", 3*time.Second)

	// time.Sleep(1 * time.Second)

	// var id primitive.ObjectID
	// id, err = mongoService.InsertDocument("db1", "c1", &mongo.Document{Name: "Ben", Age: 23, Gender: true})
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("Added a doc")

	// time.Sleep(time.Second)
	// _, err = mongoService.ModifyDocument("db1", "c1", id, "name", "Raz")
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("Modified a doc")

	// time.Sleep(time.Second)
	// _, err = mongoService.ModifyDocument("db1", "c1", id, "name", "Misha")
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("Modified a doc")

	// time.Sleep(time.Second)
	// _, err = mongoService.ModifyDocument("db1", "c1", id, "age", 69)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("Modified a doc")

	// time.Sleep(time.Second)
	// err = mongoService.DeleteDocument("db1", "c1", id)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("Deleted a doc")

	// time.Sleep(time.Second)
	// err = mongoService.DeleteDocument("db1", "c1", id)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("Deleted a doc")

	// id, err = mongoService.InsertDocument("db1", "c1", &mongo.Document{Name: "Ben", Age: 23, Gender: true})
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("Added a doc")

	// docs, err := mongo.WaitUntilDone(docsChan, errChan)
	// fmt.Println(time.Now())
	// if err != nil {
	// 	panic(err)
	// }
	// for _, doc := range docs {
	// 	fmt.Println(string(doc.Body))
	// 	fmt.Println(doc.Timestamp)
	// 	fmt.Println()
	// }

}
