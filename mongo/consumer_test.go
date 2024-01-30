package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	gomonkey "github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	mongoService = &MongoService{}
	docs         = []bson.M{
		{"_id": primitive.ObjectID([12]byte{1}), "ben": 1},
		{"_id": primitive.ObjectID([12]byte{2}), "ben": 2},
		{"_id": primitive.ObjectID([12]byte{3}), "ben": 3},
	}

	times = []time.Time{
		time.Date(2000, time.January, 1, 0, 0, 0, 0, &time.Location{}),
		time.Date(2001, time.January, 1, 0, 0, 0, 0, &time.Location{}),
		time.Date(2002, time.January, 1, 0, 0, 0, 0, &time.Location{}),
	}

	timeOutputs = []gomonkey.OutputCell{
		{Values: gomonkey.Params{times[0]}},
		{Values: gomonkey.Params{times[1]}},
		{Values: gomonkey.Params{times[2]}},
	}

	expectedDetailedMesssages = []*DetailedMessage[[]byte]{
		{Body: []byte("{\"_id\":\"010000000000000000000000\",\"ben\":1}")},
		{Body: []byte("{\"_id\":\"020000000000000000000000\",\"ben\":2}")},
		{Body: []byte("{\"_id\":\"030000000000000000000000\",\"ben\":3}")},
	}
)

func Test_getDocId(t *testing.T) {
	t.Run("doc _id is ok", func(t *testing.T) {
		doc := make(bson.M)
		doc["_id"] = primitive.ObjectID{1}
		got, err := getDocId(doc)
		assert.Nil(t, err)
		assert.Equal(t, primitive.ObjectID{1}, got)
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

func Test_watchForChanges(t *testing.T) {
	expectedDocs := []bson.M{{"ben": 0}, {"ben": 1}, bson.M(nil)}

	expectedChangeEvents := []bson.M{}
	expectedChangeEvents = append(expectedChangeEvents,
		bson.M{
			"documentKey": bson.M{
				"_id": primitive.ObjectID{1},
			},
			"fullDocument": expectedDocs[0],
		},
	)
	expectedChangeEvents = append(expectedChangeEvents,
		bson.M{
			"documentKey": bson.M{
				"_id": primitive.ObjectID{2},
			},
			"fullDocument": expectedDocs[1],
		},
	)
	expectedChangeEvents = append(expectedChangeEvents,
		bson.M{
			"documentKey": bson.M{
				"_id": primitive.ObjectID{3},
			},
		},
	)
	collection := &mongo.Collection{}
	cs := &mongo.ChangeStream{
		Current: bson.Raw{0},
	}
	watchPatch := gomonkey.ApplyMethodFunc(collection, "Watch", func(_ context.Context, _ interface{}, _ ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
		return cs, nil
	})
	defer watchPatch.Reset()

	closeStreamPatch := gomonkey.ApplyMethodReturn(&mongo.ChangeStream{}, "Close", nil)
	defer closeStreamPatch.Reset()

	nextPatch := gomonkey.ApplyMethodFunc(cs, "Next", func(_ context.Context) bool {
		return cs.Current[0] <= 2
	})
	defer nextPatch.Reset()

	t.Run("watch for 3 changes one for each type", func(t *testing.T) {
		decodePatch := gomonkey.ApplyMethodFunc(cs, "Decode", func(val interface{}) error {
			doc := val.(*bson.M)
			*doc = expectedChangeEvents[cs.Current[0]]
			cs.Current[0]++
			return nil
		})
		defer decodePatch.Reset()

		changeEventChan := make(chan *changeEvent)
		errChan := make(chan error)

		go watchForChanges(changeEventChan, errChan, &mongo.Collection{})

		changeEvents, err := WaitUntilDone(changeEventChan, errChan)
		assert.Nil(t, err)
		for _, ce := range changeEvents {
			assert.Contains(t, expectedDocs, ce.doc)
		}
	})

	t.Run("_id is not ObjectID", func(t *testing.T) {
		expectedChangeEvents[0]["documentKey"].(bson.M)["_id"] = "id"
		cs.Current[0] = 0
		decodePatch := gomonkey.ApplyMethodFunc(cs, "Decode", func(val interface{}) error {
			doc := val.(*bson.M)
			*doc = expectedChangeEvents[cs.Current[0]]
			cs.Current[0]++
			return nil
		})
		defer decodePatch.Reset()
		changeEventChan := make(chan *changeEvent)
		errChan := make(chan error)

		go watchForChanges(changeEventChan, errChan, &mongo.Collection{})

		changeEvents, err := WaitUntilDone(changeEventChan, errChan)
		assert.NotNil(t, err)
		assert.Equal(t, "value of key '_id' is not ObjectID", err.Error())
		assert.Empty(t, changeEvents)
	})

	t.Run("key _id does not exist in changeEvent", func(t *testing.T) {
		delete(expectedChangeEvents[0]["documentKey"].(bson.M), "_id")
		cs.Current[0] = 0
		decodePatch := gomonkey.ApplyMethodFunc(cs, "Decode", func(val interface{}) error {
			doc := val.(*bson.M)
			*doc = expectedChangeEvents[cs.Current[0]]
			cs.Current[0]++
			return nil
		})
		defer decodePatch.Reset()

		changeEventChan := make(chan *changeEvent)
		errChan := make(chan error)

		go watchForChanges(changeEventChan, errChan, &mongo.Collection{})

		changeEvents, err := WaitUntilDone(changeEventChan, errChan)
		assert.NotNil(t, err)
		assert.Equal(t, "key '_id' does not exist", err.Error())
		assert.Empty(t, changeEvents)
	})

	t.Run("documentKey is not bson.M", func(t *testing.T) {
		expectedChangeEvents[0] = bson.M{
			"documentKey": "documentKey",
		}
		cs.Current[0] = 0
		decodePatch := gomonkey.ApplyMethodFunc(cs, "Decode", func(val interface{}) error {
			doc := val.(*bson.M)
			*doc = expectedChangeEvents[cs.Current[0]]
			cs.Current[0]++
			return nil
		})
		defer decodePatch.Reset()
		changeEventChan := make(chan *changeEvent)
		errChan := make(chan error)

		go watchForChanges(changeEventChan, errChan, &mongo.Collection{})

		changeEvents, err := WaitUntilDone(changeEventChan, errChan)
		assert.NotNil(t, err)
		assert.Equal(t, "value of key 'documentKey' is not bson.M", err.Error())
		assert.Empty(t, changeEvents)
	})

	t.Run("documentKey does not exist in change event", func(t *testing.T) {
		delete(expectedChangeEvents[0], "documentKey")
		cs.Current[0] = 0
		decodePatch := gomonkey.ApplyMethodFunc(cs, "Decode", func(val interface{}) error {
			doc := val.(*bson.M)
			*doc = expectedChangeEvents[cs.Current[0]]
			cs.Current[0]++
			return nil
		})
		defer decodePatch.Reset()
		changeEventChan := make(chan *changeEvent)
		errChan := make(chan error)

		go watchForChanges(changeEventChan, errChan, &mongo.Collection{})

		changeEvents, err := WaitUntilDone(changeEventChan, errChan)
		assert.NotNil(t, err)
		assert.Equal(t, "key 'documentKey' does not exists in change event", err.Error())
		assert.Empty(t, changeEvents)
	})

	t.Run("failed to decode change event", func(t *testing.T) {
		decodePatch := gomonkey.ApplyMethodFunc(cs, "Decode", func(val interface{}) error {
			return errors.New("error decoding change event")
		})
		defer decodePatch.Reset()

		changeEventChan := make(chan *changeEvent)
		errChan := make(chan error)

		go watchForChanges(changeEventChan, errChan, &mongo.Collection{})

		changeEvents, err := WaitUntilDone(changeEventChan, errChan)
		assert.NotNil(t, err)
		assert.Equal(t, "error decoding change event", err.Error())
		assert.Empty(t, changeEvents)
	})

	t.Run("failed to watch for change events", func(t *testing.T) {
		watchPatch := gomonkey.ApplyMethodFunc(collection, "Watch", func(_ context.Context, _ interface{}, _ ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
			return nil, errors.New("error in watch")
		})
		defer watchPatch.Reset()

		changeEventChan := make(chan *changeEvent)
		errChan := make(chan error)

		go watchForChanges(changeEventChan, errChan, &mongo.Collection{})

		changeEvents, err := WaitUntilDone(changeEventChan, errChan)
		assert.NotNil(t, err)
		assert.Equal(t, "error in watch", err.Error())
		assert.Empty(t, changeEvents)
	})
}

func Test_mapDocs(t *testing.T) {
	t.Run("all docs mapped successfully", func(t *testing.T) {
		nowPatch := gomonkey.ApplyFuncSeq(time.Now, timeOutputs)
		defer nowPatch.Reset()
		docsMap, err := mapDocs(docs)
		assert.Nil(t, err)
		i := 0
		for _, value := range docsMap {
			assert.Len(t, docsMap, len(docs))
			assert.Contains(t, docs, value.Body)
			assert.Contains(t, times, value.Timestamp)
			i++
		}
	})

	t.Run("error getting doc id", func(t *testing.T) {
		docs := []bson.M{
			{"ben": 1},
			{"_id": primitive.ObjectID([12]byte{2}), "ben": 2},
			{"_id": primitive.ObjectID([12]byte{3}), "ben": 3},
		}
		docsMap, err := mapDocs(docs)
		assert.NotNil(t, err)
		assert.Nil(t, docsMap)
	})
}

func Test_Consume(t *testing.T) {
	t.Run("all docs consumed successfully", func(t *testing.T) {
		patchDetailedConsume := gomonkey.ApplyMethodReturn(mongoService, "DetailedConsume", expectedDetailedMesssages, nil)
		defer patchDetailedConsume.Reset()

		got, err := mongoService.Consume("", "", time.Duration(1))
		assert.Nil(t, err)
		for i, doc := range got {
			assert.Equal(t, expectedDetailedMesssages[i].Body, doc)
		}
	})

	t.Run("failed to DetailedConsume", func(t *testing.T) {
		patchDetailedConsume := gomonkey.ApplyMethodReturn(mongoService, "DetailedConsume", nil, errors.New("error in DetailedConsume"))
		defer patchDetailedConsume.Reset()

		got, err := mongoService.Consume("", "", time.Duration(1))
		assert.Nil(t, got)
		assert.NotNil(t, err)
		assert.Equal(t, "error in DetailedConsume", err.Error())
	})
}

func Test_DetailedConsume(t *testing.T) {
	patchDatabase := gomonkey.ApplyMethodReturn(mongoService.client, "Database", &mongo.Database{})
	defer patchDatabase.Reset()

	patchCollection := gomonkey.ApplyMethodReturn(&mongo.Database{}, "Collection", &mongo.Collection{})
	defer patchCollection.Reset()

	patchFetchAll := gomonkey.ApplyPrivateMethod(mongoService, "fetchAll", func(_ context.Context, _ *mongo.Collection, _ primitive.M) ([]primitive.M, error) {
		return docs, nil
	})
	defer patchFetchAll.Reset()

	t.Run("detailed consumed all docs successfully", func(t *testing.T) {
		patchNow := gomonkey.ApplyFuncReturn(time.Now, time.Time{})
		defer patchNow.Reset()

		patchWatchForChanges := gomonkey.ApplyFunc(watchForChanges, func(changeEventChan chan *changeEvent, errChan chan error, _ *mongo.Collection) {
			for _, doc := range docs {
				changeEventChan <- &changeEvent{
					id:  doc["_id"].(primitive.ObjectID),
					doc: doc,
				}
			}
			close(changeEventChan)
		})
		defer patchWatchForChanges.Reset()

		gotDetailedMessages, err := mongoService.DetailedConsume("", "", 10*time.Second)
		assert.Nil(t, err)
		assert.Len(t, gotDetailedMessages, len(expectedDetailedMesssages))
		for _, gotDetailedMessage := range gotDetailedMessages {
			assert.Contains(t, expectedDetailedMesssages, gotDetailedMessage)
		}
	})

	t.Run("timeout reached", func(t *testing.T) {
		patchNow := gomonkey.ApplyFuncReturn(time.Now, time.Time{})
		defer patchNow.Reset()

		patchWatchForChanges := gomonkey.ApplyFunc(watchForChanges, func(changeEventChan chan *changeEvent, errChan chan error, _ *mongo.Collection) {
			time.Sleep(time.Second)
		})
		defer patchWatchForChanges.Reset()

		gotDetailedMessages, err := mongoService.DetailedConsume("", "", 1*time.Nanosecond)
		assert.Nil(t, err)
		for _, gotDetailedMessage := range gotDetailedMessages {
			assert.Contains(t, expectedDetailedMesssages, gotDetailedMessage)
		}
	})

	t.Run("document is removed", func(t *testing.T) {
		patchNow := gomonkey.ApplyFuncReturn(time.Now, time.Time{})
		defer patchNow.Reset()

		patchWatchForChanges := gomonkey.ApplyFunc(watchForChanges, func(changeEventChan chan *changeEvent, errChan chan error, _ *mongo.Collection) {
			for i, doc := range docs {
				if i < 2 {
					changeEventChan <- &changeEvent{
						id:  doc["_id"].(primitive.ObjectID),
						doc: doc,
					}
				} else {
					changeEventChan <- &changeEvent{
						id:  doc["_id"].(primitive.ObjectID),
						doc: nil, // document is deleted
					}
				}

			}
			close(changeEventChan)
		})
		defer patchWatchForChanges.Reset()

		gotDetailedMessages, err := mongoService.DetailedConsume("", "", time.Second)
		assert.Nil(t, err)
		assert.Len(t, gotDetailedMessages, len(expectedDetailedMesssages)-1)
		for i, gotDetailedMessage := range gotDetailedMessages {
			assert.Equal(t, expectedDetailedMesssages[i].Body, gotDetailedMessage.Body)
		}
	})

	t.Run("error marshaling json", func(t *testing.T) {
		patchWatchForChanges := gomonkey.ApplyFunc(watchForChanges, func(changeEventChan chan *changeEvent, errChan chan error, _ *mongo.Collection) {
			for _, doc := range docs {
				changeEventChan <- &changeEvent{
					id:  doc["_id"].(primitive.ObjectID),
					doc: doc,
				}
			}
			close(changeEventChan)
		})
		defer patchWatchForChanges.Reset()
		patchMarshal := gomonkey.ApplyFuncReturn(json.Marshal, nil, errors.New("error in json.Marshal"))
		defer patchMarshal.Reset()

		gotDetailedMessages, err := mongoService.DetailedConsume("", "", 10*time.Second)
		assert.NotNil(t, err)
		assert.Equal(t, "error in json.Marshal", err.Error())
		assert.Nil(t, gotDetailedMessages)
	})

	t.Run("failed to watch for changes", func(t *testing.T) {
		patchNow := gomonkey.ApplyFuncReturn(time.Now, time.Time{})
		defer patchNow.Reset()

		patchWatchForChanges := gomonkey.ApplyFunc(watchForChanges, func(changeEventChan chan *changeEvent, errChan chan error, _ *mongo.Collection) {
			errChan <- errors.New("error in watchForChanges")
		})
		defer patchWatchForChanges.Reset()

		gotDetailedMessages, err := mongoService.DetailedConsume("", "", time.Second)
		assert.NotNil(t, err)
		assert.Equal(t, "error in watchForChanges", err.Error())
		assert.Nil(t, gotDetailedMessages)
	})

	t.Run("error mapping docs", func(t *testing.T) {
		patchFetchAll := gomonkey.ApplyPrivateMethod(mongoService, "fetchAll", func(_ context.Context, _ *mongo.Collection, _ primitive.M) ([]primitive.M, error) {
			delete(docs[0], "_id")
			return docs, nil
		})
		defer patchFetchAll.Reset()

		gotDetailedMessages, err := mongoService.DetailedConsume("", "", 10*time.Second)
		assert.NotNil(t, err)
		assert.Equal(t, "key '_id' does not exist in document", err.Error())
		assert.Nil(t, gotDetailedMessages)
	})

	t.Run("error fetching all docs", func(t *testing.T) {
		patchFetchAll := gomonkey.ApplyPrivateMethod(mongoService, "fetchAll", func(_ context.Context, _ *mongo.Collection, _ primitive.M) ([]primitive.M, error) {
			return nil, errors.New("error in fetchAll")
		})
		defer patchFetchAll.Reset()

		gotDetailedMessages, err := mongoService.DetailedConsume("", "", 10*time.Second)
		assert.NotNil(t, err)
		assert.Equal(t, "error in fetchAll", err.Error())
		assert.Nil(t, gotDetailedMessages)
	})
}

func Test_AsyncConsume(t *testing.T) {
	t.Run("all docs consumed successfully", func(t *testing.T) {
		patchDetailedConsume := gomonkey.ApplyMethodReturn(mongoService, "DetailedConsume", expectedDetailedMesssages, nil)
		defer patchDetailedConsume.Reset()
		got, err := WaitUntilDone(mongoService.AsyncConsume("", "", time.Second))
		assert.Nil(t, err)
		for i, doc := range got {
			assert.Equal(t, expectedDetailedMesssages[i].Body, doc)
		}
	})

	t.Run("failed detailed consuming", func(t *testing.T) {
		patchDetailedConsume := gomonkey.ApplyMethodReturn(mongoService, "DetailedConsume", nil, errors.New("error in DetailedConsume"))
		defer patchDetailedConsume.Reset()
		got, err := WaitUntilDone(mongoService.AsyncConsume("", "", time.Second))
		assert.Nil(t, got)
		assert.NotNil(t, err)
		assert.Equal(t, "error in DetailedConsume", err.Error())
	})
}

func Test_AsyncDetailedConsume(t *testing.T) {
	t.Run("all docs detailed consumed successfully", func(t *testing.T) {
		patchDetailedConsume := gomonkey.ApplyMethodReturn(mongoService, "DetailedConsume", expectedDetailedMesssages, nil)
		defer patchDetailedConsume.Reset()
		got, err := WaitUntilDone(mongoService.AsyncDetailedConsume("", "", time.Second))
		assert.Nil(t, err)
		assert.ElementsMatch(t, expectedDetailedMesssages, got)
	})

	t.Run("failed detailed consuming", func(t *testing.T) {
		patchDetailedConsume := gomonkey.ApplyMethodReturn(mongoService, "DetailedConsume", nil, errors.New("error in DetailedConsume"))
		defer patchDetailedConsume.Reset()
		got, err := WaitUntilDone(mongoService.AsyncDetailedConsume("", "", time.Second))
		assert.Nil(t, got)
		assert.NotNil(t, err)
		assert.Equal(t, "error in DetailedConsume", err.Error())
	})
}

func Test_TimestampConsume(t *testing.T) {
	t.Run("all docs consumed successfully", func(t *testing.T) {
		patchDetailedConsume := gomonkey.ApplyMethodFunc(mongoService, "DetailedConsume", func(_ string, _ string, _ time.Duration) ([]*DetailedMessage[[]byte], error) {
			return []*DetailedMessage[[]byte]{
				{Timestamp: times[0]},
				{Timestamp: times[1]},
				{Timestamp: times[2]},
			}, nil
		})
		defer patchDetailedConsume.Reset()

		got, err := mongoService.TimestampConsume("", "", time.Second)
		assert.Nil(t, err)
		for i, timestamp := range got {
			assert.Equal(t, times[i], timestamp)
		}
	})

	t.Run("failed to DetailedConsume", func(t *testing.T) {
		patchDetailedConsume := gomonkey.ApplyMethodReturn(mongoService, "DetailedConsume", nil, errors.New("error in DetailedConsume"))
		defer patchDetailedConsume.Reset()

		got, err := mongoService.TimestampConsume("", "", time.Duration(1))
		assert.Nil(t, got)
		assert.NotNil(t, err)
		assert.Equal(t, "error in DetailedConsume", err.Error())
	})
}
