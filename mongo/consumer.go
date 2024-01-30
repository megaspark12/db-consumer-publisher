package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type DetailedMessage[T any] struct {
	Body      T
	Timestamp time.Time
}

func (m *MongoService) Consume(databaseName, collectionName string, timeout time.Duration) (result [][]byte, err error) {
	detailedMessages, err := m.DetailedConsume(databaseName, collectionName, timeout)
	if err != nil {
		return nil, err
	}

	for _, detailedMessage := range detailedMessages {
		result = append(result, detailedMessage.Body)
	}

	return
}

func (m *MongoService) DetailedConsume(databaseName, collectionName string, timeout time.Duration) (result []*DetailedMessage[[]byte], err error) {
	collection := m.client.Database(databaseName).Collection(collectionName)

	documents, err := m.fetchAll(context.Background(), collection, bson.M{})
	if err != nil {
		return nil, err
	}

	detailedMessagesMap, err := mapDocs(documents)
	if err != nil {
		return nil, err
	}

	eventChan := make(chan *changeEvent)
	errChan := make(chan error)

	go watchForChanges(eventChan, errChan, collection)

	watch := true
	for watch {
		timeoutChan := time.After(timeout)
		select {
		case <-timeoutChan:
			watch = false
		case changeEvent, open := <-eventChan:
			if !open {
				watch = false
				break
			}
			if changeEvent.doc == nil {
				delete(detailedMessagesMap, changeEvent.id)
			} else {
				detailedMessagesMap[changeEvent.id] = &DetailedMessage[bson.M]{Body: changeEvent.doc, Timestamp: time.Now()}
			}
		case err := <-errChan:
			return nil, err
		}
	}

	for _, value := range detailedMessagesMap {
		docJson, err := json.Marshal(value.Body)
		if err != nil {
			return nil, err
		}
		result = append(result, &DetailedMessage[[]byte]{Body: docJson, Timestamp: value.Timestamp})
	}
	return
}

func (m *MongoService) AsyncConsume(databaseName, collectionName string, timeout time.Duration) (chan []byte, chan error) {
	docsChan := make(chan []byte)
	errChan := make(chan error)

	go func() {
		detailedMessages, err := m.DetailedConsume(databaseName, collectionName, timeout)
		if err != nil {
			errChan <- err
			close(errChan)
			return
		}
		for _, detailedMessage := range detailedMessages {
			docsChan <- detailedMessage.Body
		}
		close(docsChan)
	}()
	return docsChan, errChan
}

func (m *MongoService) AsyncDetailedConsume(databaseName, collectionName string, timeout time.Duration) (chan *DetailedMessage[[]byte], chan error) {
	detailedMessagesChan := make(chan *DetailedMessage[[]byte])
	errChan := make(chan error)

	go func() {
		detailedMessages, err := m.DetailedConsume(databaseName, collectionName, timeout)
		if err != nil {
			errChan <- err
			close(errChan)
			return
		}
		for _, detailedMessage := range detailedMessages {
			detailedMessagesChan <- detailedMessage
		}
		close(detailedMessagesChan)
	}()
	return detailedMessagesChan, errChan
}

func (m *MongoService) TimestampConsume(databaseName, collectionName string, timeout time.Duration) (timestamps []time.Time, err error) {
	detailedMessages, err := m.DetailedConsume(databaseName, collectionName, timeout)
	if err != nil {
		return nil, err
	}

	for _, detailedMessage := range detailedMessages {
		timestamps = append(timestamps, detailedMessage.Timestamp)
	}

	return
}

// mapDocs get a slice of MongoDB documents and mapping them by their '_id'
func mapDocs(docs []bson.M) (map[primitive.ObjectID]*DetailedMessage[bson.M], error) {
	docsMap := make(map[primitive.ObjectID]*DetailedMessage[bson.M])

	for _, doc := range docs {
		id, err := getDocId(doc)
		if err != nil {
			return nil, err
		}
		timestamp := time.Now()
		docsMap[id] = &DetailedMessage[bson.M]{Body: doc, Timestamp: timestamp}
	}

	return docsMap, nil
}

// getDocId extract the value of the field '_id' of the MongoDB document
func getDocId(doc bson.M) (id primitive.ObjectID, err error) {
	objectId, ok := doc["_id"]
	if !ok {
		return id, errors.New("key '_id' does not exist in document")
	}

	if id, ok = objectId.(primitive.ObjectID); !ok {
		return id, errors.New("could not infer type of value with key: '_id'")
	}
	return
}

// fetchAll gets all existsing documents from the collection in MongoDB
func (m *MongoService) fetchAll(ctx context.Context, collection *mongo.Collection, filter bson.M) (docs []bson.M, err error) {
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return
	}

	defer cursor.Close(ctx)

	err = cursor.All(ctx, &docs)
	if err != nil {
		return nil, err
	}

	return
}

func watchForChanges(changeEventChan chan *changeEvent, errChan chan error, collection *mongo.Collection) {
	ctx := context.Background()
	changeStream, err := collection.Watch(ctx, mongo.Pipeline{})
	if err != nil {
		errChan <- err
		close(errChan)
		return
	}
	defer changeStream.Close(ctx)

	for changeStream.Next(ctx) {
		var event bson.M
		if err = changeStream.Decode(&event); err != nil {
			errChan <- err
			close(errChan)
			return
		}

		changeEventId, err := getChangeEventDocId(event)
		if err != nil {
			errChan <- err
			close(errChan)
			return
		}

		doc, ok := event["fullDocument"]
		if ok {
			changeEventChan <- &changeEvent{
				id:  changeEventId,
				doc: doc.(bson.M),
			}
		} else {
			changeEventChan <- &changeEvent{
				id:  changeEventId,
				doc: nil,
			}
		}
	}
	close(changeEventChan)
}

func getChangeEventDocId(event bson.M) (changeEventId primitive.ObjectID, err error) {
	documentKey, ok := event["documentKey"]
	if !ok {
		return changeEventId, errors.New("key 'documentKey' does not exists in change event")
	}

	docKeyMap, ok := documentKey.(bson.M)
	if !ok {
		return changeEventId, errors.New("value of key 'documentKey' is not bson.M")
	}

	id, ok := docKeyMap["_id"]
	if !ok {
		return changeEventId, errors.New("key '_id' does not exist")
	}

	if changeEventId, ok = id.(primitive.ObjectID); !ok {
		return changeEventId, errors.New("value of key '_id' is not ObjectID")
	}
	return changeEventId, nil
}

// func (m *MongoService) Populate(databaseName, collectionName string) (err error) {
// 	// Set up MongoDB connection parameters

// 	// Use a database and collection
// 	database := m.client.Database(databaseName)
// 	collection := database.Collection(collectionName)

// 	// Create the database and collection if they don't exist
// 	// err := database.CreateCollection(context.Background(), collectionName)
// 	// if err != nil {
// 	// 	log.Printf("Database or collection creation error: %v", err)
// 	// } else {
// 	// 	fmt.Printf("Database '%s' and collection '%s' created\n", databaseName, collectionName)
// 	// }

// 	// Insert documents
// 	documents := []interface{}{
// 		map[string]interface{}{"name": "John", "age": 30},
// 		map[string]interface{}{"name": "Jane", "age": 25},
// 		map[string]interface{}{"name": "Bob", "age": 35},
// 		// map[string]interface{}{"name": "Ben", "age": age},
// 	}

// 	insertResult, err := collection.InsertMany(context.Background(), documents)
// 	if err != nil {
// 		return err
// 	}

// 	fmt.Printf("Inserted %v documents\n", len(insertResult.InsertedIDs))
// 	return nil
// }

// func (m *MongoService) PupolateBen(databaseName, collectionName string) {
// 	// Set up MongoDB connection parameters

// 	collection := m.client.Database(databaseName).Collection(collectionName)

// 	// Insert documents
// 	documents := []interface{}{
// 		map[string]interface{}{"name": "Ben", "age": 23},
// 	}

// 	insertResult, err := collection.InsertMany(context.Background(), documents)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	fmt.Printf("Inserted %v documents\n", len(insertResult.InsertedIDs))
// }

// func (m *MongoService) Purge(databaseName, collectionName string) {
// 	collection := m.client.Database(databaseName).Collection(collectionName)
// 	_, err := collection.DeleteMany(context.Background(), map[string]interface{}{})
// 	if err != nil {
// 		panic(err)
// 	}
// }
