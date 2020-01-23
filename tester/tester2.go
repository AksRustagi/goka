package tester

import (
	"fmt"
	"hash"
	"reflect"
	"sync"
	"testing"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/mock"
	"github.com/lovoo/goka/storage"

	"github.com/Shopify/sarama"
)

type client struct {
	clientID      string
	consumerGroup *ConsumerGroup
	consumer      *consumerMock

	expectGroup bool
	// list of topics where we expect consumers for
	expectedConsumers []string
}

type Tester struct {
	t        *testing.T
	producer *mock.Producer
	tmgr     goka.TopicManager

	clients map[string]*client

	codecs      map[string]goka.Codec
	mQueues     sync.RWMutex
	topicQueues map[string]*queue
	storages    map[string]storage.Storage
}

func NewTester(t *testing.T) *Tester {

	return &Tester{
		t:        t,
		tmgr:     NewMockTopicManager(1, 1),
		producer: mock.NewProducer(t),

		clients: make(map[string]*client),

		codecs:      make(map[string]goka.Codec),
		topicQueues: make(map[string]*queue),
		storages:    make(map[string]storage.Storage),
	}
}

func (tt *Tester) nextClient() *client {
	c := &client{
		clientID:      fmt.Sprintf("client-%d", len(tt.clients)),
		consumer:      newConsumerMock(tt),
		consumerGroup: NewConsumerGroup(tt.t),
	}
	tt.clients[c.clientID] = c
	return c
}

func (tt *Tester) ConsumerGroupBuilder() goka.ConsumerGroupBuilder {
	return func(brokers []string, group, clientID string) (sarama.ConsumerGroup, error) {
		client, exists := tt.clients[clientID]
		if !exists {
			return nil, fmt.Errorf("cannot create consumergroup because no client registered with ID: %s", clientID)
		}
		return client.consumerGroup, nil
	}
}

func (tt *Tester) ConsumerBuilder() goka.SaramaConsumerBuilder {
	return func(brokers []string, clientID string) (sarama.Consumer, error) {
		client, exists := tt.clients[clientID]
		if !exists {
			return nil, fmt.Errorf("cannot create sarama consumer because no client registered with ID: %s", clientID)
		}

		return client.consumer, nil
	}
}

type consumerMock struct {
	tester         *Tester
	requiredTopics map[string]bool
	partConsumers  map[string]*partConsumerMock
}

func newConsumerMock(tt *Tester) *consumerMock {
	return &consumerMock{
		tester:         tt,
		requiredTopics: make(map[string]bool),
		partConsumers:  make(map[string]*partConsumerMock),
	}
}

func (cm *consumerMock) Topics() ([]string, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (cm *consumerMock) Partitions(topic string) ([]int32, error) {
	return nil, fmt.Errorf("not implemented")
}

func (cm *consumerMock) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	if _, exists := cm.partConsumers[topic]; exists {
		return nil, fmt.Errorf("Got duplicate consume partition for topic %s", topic)
	}
	cons := &partConsumerMock{
		queue:    cm.tester.getOrCreateQueue(topic),
		messages: make(chan *sarama.ConsumerMessage),
		errors:   make(chan *sarama.ConsumerError),
		closer: func() error {
			if _, exists := cm.partConsumers[topic]; !exists {
				return fmt.Errorf("partition consumer seems already closed")
			}
			delete(cm.partConsumers, topic)
			return nil
		},
	}

	return cons, nil
}
func (cm *consumerMock) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}
func (cm *consumerMock) Close() error {
	return nil
}

type partConsumerMock struct {
	closer   func() error
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
	queue    *queue
}

func (pcm *partConsumerMock) Close() error {
	close(pcm.messages)
	close(pcm.errors)
	return pcm.closer()
}

func (pcm *partConsumerMock) AsyncClose() {
	go pcm.Close()
}

func (pcm *partConsumerMock) Messages() <-chan *sarama.ConsumerMessage {
	return pcm.messages
}

func (pcm *partConsumerMock) Errors() <-chan *sarama.ConsumerError {
	return pcm.errors
}

func (pcm *partConsumerMock) HighWaterMarkOffset() int64 {
	return pcm.queue.Hwm()
}

func (tt *Tester) ProducerBuilder() goka.ProducerBuilder {
	return func(b []string, cid string, hasher func() hash.Hash32) (goka.Producer, error) {
		return tt.producer, nil
	}
}

func (tt *Tester) TopicManagerBuilder() goka.TopicManagerBuilder {
	return func(brokers []string) (goka.TopicManager, error) {
		return tt.tmgr, nil
	}
}

// RegisterGroupGraph is called by a processor when the tester is passed via
// `WithTester(..)`.
// This will setup the tester with the neccessary consumer structure
func (tt *Tester) RegisterGroupGraph(gg *goka.GroupGraph) string {

	client := tt.nextClient()
	if gg.GroupTable() != nil {
		queue := tt.getOrCreateQueue(gg.GroupTable().Topic())
		client.expectSaramaConsumer(queue)
		tt.registerCodec(gg.GroupTable().Topic(), gg.GroupTable().Codec())
	}

	for _, input := range gg.InputStreams() {
		client.expectGroupConsumer(input.Topic())
		tt.registerCodec(input.Topic(), input.Codec())
	}

	for _, output := range gg.OutputStreams() {
		tt.registerCodec(output.Topic(), output.Codec())
		tt.getOrCreateQueue(output.Topic())
	}
	for _, join := range gg.JointTables() {
		client.expectSimpleConsumer()
		// tt.getOrCreateQueue(join.Topic()).expectSimpleConsumer()
		tt.registerCodec(join.Topic(), join.Codec())
	}

	if loop := gg.LoopStream(); loop != nil {
		client.expectGroupConsumer(loop.Topic())
		// tt.getOrCreateQueue(loop.Topic()).expectGroupConsumer()
		tt.registerCodec(loop.Topic(), loop.Codec())
	}

	for _, lookup := range gg.LookupTables() {
		client.expectSimpleConsumer(lookup)
		// tt.getOrCreateQueue(lookup.Topic()).expectSimpleConsumer()
		tt.registerCodec(lookup.Topic(), lookup.Codec())
	}

	return client.clientID
}

func (tt *Tester) RegisterView(table goka.Table, c goka.Codec) string {
	client := tt.nextClient()
	client.expectedConsumers = append(client.expectedConsumers, string(table))
	return client.clientID
}

func (tt *Tester) getOrCreateQueue(topic string) *queue {
	tt.mQueues.RLock()
	_, exists := tt.topicQueues[topic]
	tt.mQueues.RUnlock()
	if !exists {
		tt.mQueues.Lock()
		if _, exists = tt.topicQueues[topic]; !exists {
			tt.topicQueues[topic] = newQueue(topic)
		}
		tt.mQueues.Unlock()
	}

	tt.mQueues.RLock()
	defer tt.mQueues.RUnlock()
	return tt.topicQueues[topic]
}

func (tt *Tester) codecForTopic(topic string) goka.Codec {
	codec, exists := tt.codecs[topic]
	if !exists {
		panic(fmt.Errorf("No codec for topic %s registered.", topic))
	}
	return codec
}

func (tt *Tester) registerCodec(topic string, codec goka.Codec) {
	if existingCodec, exists := tt.codecs[topic]; exists {
		if reflect.TypeOf(codec) != reflect.TypeOf(existingCodec) {
			panic(fmt.Errorf("There are different codecs for the same topic. This is messed up (%#v, %#v)", codec, existingCodec))
		}
	}
	tt.codecs[topic] = codec
}
