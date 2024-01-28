package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoService struct {
	client *mongo.Client
}

type DetailedMessage[T any] struct {
	Body      T
	Timestamp time.Time
}

type changeEvent struct {
	id  string
	doc bson.M
}

func NewMongoService(hostname string, port int, username, password string) (*MongoService, error) {
	var clientOptions *options.ClientOptions

	if username != "" && password != "" {
		clientOptions = options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%d", hostname, port)).SetAuth(options.Credential{Username: username, Password: password})
	} else {
		clientOptions = options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%d", hostname, port))
	}

	ctx := context.Background()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, readpref.PrimaryPreferred())
	if err != nil {
		return nil, err
	}

	return &MongoService{client: client}, nil
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

	docs, err := mapDocs(documents)
	if err != nil {
		return nil, err
	}

	eventChan := make(chan *changeEvent)
	errChan := make(chan error)

	go watchForChanges(eventChan, errChan, collection)

	watch := true
	for watch {
		timeoutChan := time.After(timeout)
		fmt.Println("restart timer")
		select {
		case <-timeoutChan:
			fmt.Println("timeout reached")
			watch = false
		case changeEvent := <-eventChan:
			if changeEvent.doc == nil {
				delete(docs, changeEvent.id)
			} else {
				docs[changeEvent.id] = &DetailedMessage[bson.M]{Body: changeEvent.doc, Timestamp: time.Now()}
			}
		case err := <-errChan:
			return nil, err
		}
	}

	for _, value := range docs {
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

func mapDocs(docs []bson.M) (map[string]*DetailedMessage[bson.M], error) {
	docsMap := make(map[string]*DetailedMessage[bson.M])

	for _, doc := range docs {
		id, err := getDocId(doc)
		if err != nil {
			return nil, err
		}
		docsMap[id] = &DetailedMessage[bson.M]{Body: doc, Timestamp: time.Now()}
	}

	return docsMap, nil
}

func getDocId(doc bson.M) (string, error) {
	objectId, ok := doc["_id"]
	if !ok {
		return "", errors.New("key '_id' does not exist in document")
	}

	id, ok := objectId.(primitive.ObjectID)
	if !ok {
		return "", errors.New("could not infer type of value with key: '_id'")
	}
	return id.Hex(), nil
}

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

func watchForChanges(docChan chan *changeEvent, errChan chan error, collection *mongo.Collection) {
	ctx := context.Background()
	changeStream, err := collection.Watch(ctx, mongo.Pipeline{})
	if err != nil {
		errChan <- err
		return
	}
	defer changeStream.Close(ctx)

	for changeStream.Next(ctx) {
		var event bson.M
		if err = changeStream.Decode(&event); err != nil {
			errChan <- err
			return
		}
		id, ok := ((event["documentKey"]).(bson.M)["_id"]).(primitive.ObjectID)
		if !ok {
			errChan <- errors.New("could not get '_id' of change event")
		}

		doc, ok := event["fullDocument"]
		if ok {
			docChan <- &changeEvent{
				id:  id.Hex(),
				doc: doc.(bson.M),
			}
		} else {
			docChan <- &changeEvent{
				id:  id.Hex(),
				doc: nil,
			}
		}
	}
}

func (m *MongoService) Populate(databaseName, collectionName string) (err error) {
	// Set up MongoDB connection parameters

	// Use a database and collection
	database := m.client.Database(databaseName)
	collection := database.Collection(collectionName)

	// Create the database and collection if they don't exist
	// err := database.CreateCollection(context.Background(), collectionName)
	// if err != nil {
	// 	log.Printf("Database or collection creation error: %v", err)
	// } else {
	// 	fmt.Printf("Database '%s' and collection '%s' created\n", databaseName, collectionName)
	// }

	// Insert documents
	documents := []interface{}{
		map[string]interface{}{"name": "John", "age": 30},
		map[string]interface{}{"name": "Jane", "age": 25},
		map[string]interface{}{"name": "Bob", "age": 35},
		// map[string]interface{}{"name": "Ben", "age": age},
	}

	insertResult, err := collection.InsertMany(context.Background(), documents)
	if err != nil {
		return err
	}

	fmt.Printf("Inserted %v documents\n", len(insertResult.InsertedIDs))
	return nil
}

func (m *MongoService) PupolateBen(databaseName, collectionName string) {
	// Set up MongoDB connection parameters

	collection := m.client.Database(databaseName).Collection(collectionName)

	// Insert documents
	documents := []interface{}{
		map[string]interface{}{"name": "Ben", "age": 23},
	}

	insertResult, err := collection.InsertMany(context.Background(), documents)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Inserted %v documents\n", len(insertResult.InsertedIDs))
}

func (m *MongoService) Purge(databaseName, collectionName string) {
	collection := m.client.Database(databaseName).Collection(collectionName)
	_, err := collection.DeleteMany(context.Background(), map[string]interface{}{})
	if err != nil {
		panic(err)
	}
}
