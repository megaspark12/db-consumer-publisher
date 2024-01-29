package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoService struct {
	client *mongo.Client
}

type changeEvent struct {
	id  primitive.ObjectID
	doc bson.M
}

func NewMongoService(hostname string, port int, username, password string) (*MongoService, error) {
	clientOptions := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%d", hostname, port))
	if username != "" && password != "" {
		clientOptions.SetAuth(options.Credential{Username: username, Password: password})
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
