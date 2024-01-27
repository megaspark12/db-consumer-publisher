package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	username = "admin"
	password = "admin"
)

// type Document struct {
// 	ID   any       `bson:"_id,omitempty"`
// 	Body any       `bson:"name"`
// 	Age  time.Time `bson:"age"`
// }

type MongoService struct {
	client *mongo.Client
}

type DetailedMessage struct {
	Body      []byte
	Timestamp time.Time
}

type Document struct {
	Doc       bson.M
	Timestamp time.Time
}

func NewMongoService(hostname string, port int, username, password string) (*MongoService, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%s@%s:%d", username, password, hostname, port)))
	if err != nil {
		return nil, err
	}

	return &MongoService{client: client}, nil
}

func (m *MongoService) DetailedConsume(databaseName, collectionName string, timeout time.Duration) (result []*DetailedMessage, err error) {
	// ctx, cancel := context.WithTimeout(context.Background(), timeout)
	// defer cancel()

	// Access a MongoDB collection
	collection := m.client.Database(databaseName).Collection(collectionName)

	documents, err := m.fetchAll(context.Background(), collection, bson.D{{}})
	if err != nil {
		return nil, err
	}

	docs, err := mapDocs(documents)
	if err != nil {
		return nil, err
	}

	docChan := make(chan bson.M)

	go watchForChanges(docChan, collection)

	watch := true
	for watch {
		select {
		case <-time.After(timeout):
			fmt.Println("timeout reached")
			watch = false
		case doc := <-docChan:
			id, err := getDocId(doc)
			if err != nil {
				return nil, err
			}
			docs[id] = &Document{Doc: doc, Timestamp: time.Now()}
		}
	}

	for _, value := range docs {
		docJson, err := json.Marshal(value.Doc)
		if err != nil {
			return nil, err
		}
		result = append(result, &DetailedMessage{Body: docJson, Timestamp: value.Timestamp})
	}
	return
}

func mapDocs(docs []bson.M) (map[string]*Document, error) {
	docsMap := make(map[string]*Document)

	for _, doc := range docs {
		id, err := getDocId(doc)
		if err != nil {
			return nil, err
		}
		docsMap[id] = &Document{Doc: doc, Timestamp: time.Now()}
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

func (m *MongoService) fetchAll(ctx context.Context, collection *mongo.Collection, filter bson.D) (docs []bson.M, err error) {
	// Perform a find operation
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

// func findChanges(last, current map[string]bson.M) bool {
// 	if len(current) != len(last) {
// 		return true
// 	}

// 	for key, currentValue := range current {
// 		lastValue, ok := last[key]
// 		if !ok || !reflect.DeepEqual(currentValue, lastValue) {
// 			return true
// 		}
// 	}
// 	return false
// }

func watchForChanges(docChan chan bson.M, collection *mongo.Collection) (err error) {
	ctx := context.Background()
	changeStream, err := collection.Watch(ctx, mongo.Pipeline{})
	if err != nil {
		return err
	}
	defer changeStream.Close(ctx)

	for changeStream.Next(ctx) {
		var doc bson.M
		if err = changeStream.Decode(&doc); err != nil {
			return err
		}
		docChan <- doc
	}
	return nil
}

// func populate() {
// 	// Set up MongoDB connection parameters
// 	clientOptions := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%s@localhost:27017", username, password))
// 	client, err := mongo.Connect(context.Background(), clientOptions)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer func() {
// 		if err := client.Disconnect(context.Background()); err != nil {
// 			log.Fatal(err)
// 		}
// 	}()

// 	// Check the connection
// 	err = client.Ping(context.Background(), nil)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	fmt.Println("Connected to MongoDB!")

// 	// Use a database and collection
// 	database := client.Database(databaseName)
// 	collection := database.Collection(collectionName)

// 	// Create the database and collection if they don't exist
// 	err = database.CreateCollection(context.Background(), collectionName)
// 	if err != nil {
// 		log.Printf("Database or collection creation error: %v", err)
// 	} else {
// 		fmt.Printf("Database '%s' and collection '%s' created\n", databaseName, collectionName)
// 	}

// 	// Insert documents
// 	documents := []interface{}{
// 		map[string]interface{}{"name": "John", "age": 30},
// 		map[string]interface{}{"name": "Jane", "age": 25},
// 		map[string]interface{}{"name": "Bob", "age": 35},
// 	}

// 	insertResult, err := collection.InsertMany(context.Background(), documents)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	fmt.Printf("Inserted %v documents\n", len(insertResult.InsertedIDs))
// }
