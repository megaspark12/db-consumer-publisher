package mongo

import (
	"context"
	"errors"
	"testing"

	gomonkey "github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	mongoService = &MongoService{}
)

func Test_getDocId(t *testing.T) {
	t.Run("doc _id is ok", func(t *testing.T) {
		doc := make(bson.M)
		objectId := primitive.ObjectID{}
		copy(objectId[:], []byte("012345678910"))
		doc["_id"] = objectId
		got, err := getDocId(doc)
		assert.Nil(t, err)
		assert.Equal(t, objectId, got)
	})
	t.Run("key _id does not exist", func(t *testing.T) {
		doc := make(bson.M)
		got, err := getDocId(doc)
		assert.NotNil(t, err)
		assert.Equal(t, "key '_id' does not exist in document", err.Error())
		assert.Empty(t, got)
	})
	t.Run("key _id does not exist", func(t *testing.T) {
		doc := make(bson.M)
		doc["_id"] = "just a string"
		got, err := getDocId(doc)
		assert.NotNil(t, err)
		assert.Equal(t, "could not infer type of value with key: '_id'", err.Error())
		assert.Empty(t, got)
	})
}

func Test_fetchAll(t *testing.T) {
	t.Run("all docs fetched successfully", func(t *testing.T) {
		expectedDocs := make([]bson.M, 0)
		expectedDocs = append(expectedDocs, bson.M{"ben": 1})
		expectedDocs = append(expectedDocs, bson.M{"ben": 2})
		expectedDocs = append(expectedDocs, bson.M{"ben": 3})

		collection := &mongo.Collection{}
		findPatch := gomonkey.ApplyMethodFunc(collection, "Find", func(_ context.Context, _ interface{}, _ ...*options.FindOptions) (cur *mongo.Cursor, err error) {
			return &mongo.Cursor{}, nil
		})
		defer findPatch.Reset()

		closeCursorPatch := gomonkey.ApplyMethodFunc(&mongo.Cursor{}, "Close", func(_ context.Context) error {
			return nil
		})
		defer closeCursorPatch.Reset()

		allPatch := gomonkey.ApplyMethodFunc(&mongo.Cursor{}, "All", func(_ context.Context, results interface{}) error {
			docs := results.(*[]bson.M)
			*docs = append(*docs, expectedDocs[0])
			*docs = append(*docs, expectedDocs[1])
			*docs = append(*docs, expectedDocs[2])

			return nil
		})
		defer allPatch.Reset()

		t.Run("fetched all docs successfully", func(t *testing.T) {
			got, err := mongoService.fetchAll(context.TODO(), &mongo.Collection{}, bson.M{})
			assert.Nil(t, err)
			assert.Equal(t, expectedDocs, got)
		})
		t.Run("failed to get all docs", func(t *testing.T) {
			allPatch = gomonkey.ApplyMethodFunc(&mongo.Cursor{}, "All", func(_ context.Context, results interface{}) error {
				results = nil
				return errors.New("error in cursor.All")
			})
			got, err := mongoService.fetchAll(context.TODO(), &mongo.Collection{}, bson.M{})
			assert.Equal(t, "error in cursor.All", err.Error())
			assert.Nil(t, got)

		})
		t.Run("failed to get cursor", func(t *testing.T) {
			findPatch = gomonkey.ApplyMethodFunc(collection, "Find", func(_ context.Context, _ interface{}, _ ...*options.FindOptions) (cur *mongo.Cursor, err error) {
				return nil, errors.New("error in collection.Find")
			})
			got, err := mongoService.fetchAll(context.TODO(), &mongo.Collection{}, bson.M{})
			assert.Equal(t, "error in collection.Find", err.Error())
			assert.Nil(t, got)
		})
	})
}
