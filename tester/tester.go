package tester

import (
	"flag"
	"fmt"
	"hash"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/storage"

	"github.com/Shopify/sarama"
)

// Codec decodes and encodes from and to []byte
type Codec interface {
	Encode(value interface{}) (data []byte, err error)
	Decode(data []byte) (value interface{}, err error)
}

type debugLogger interface {
	Printf(s string, args ...interface{})
}

type nilLogger int

func (*nilLogger) Printf(s string, args ...interface{}) {}

var (
	debug              = flag.Bool("tester-debug", false, "show debug prints of the tester.")
	logger debugLogger = new(nilLogger)
)

// EmitHandler abstracts a function that allows to overwrite kafkamock's Emit function to
// simulate producer errors
type EmitHandler func(topic string, key string, value []byte) *goka.Promise

type client struct {
	clientID      string
	consumerGroup *ConsumerGroup
	consumer      *consumerMock
}

func (c *client) waitStartup() {
	if c.consumerGroup != nil {
		c.consumerGroup.WaitRunning()
	}

	c.consumer.waitRequiredConsumersStartup()
}

func (c *client) requireConsumer(topic string) {
	c.consumer.requirePartConsumer(topic)
}

func (c *client) catchup() int {
	catchup := c.consumerGroup.catchupAndWait()

	catchup += c.consumer.catchup()

	return catchup
}

type queuedMessage struct {
	topic string
	key   string
	value []byte
}

type Tester struct {
	t        *testing.T
	producer *producerMock
	tmgr     goka.TopicManager

	mClients sync.RWMutex
	clients  map[string]*client

	codecs      map[string]goka.Codec
	mQueues     sync.Mutex
	topicQueues map[string]*queue
	storages    map[string]storage.Storage

	queuedMessages []*queuedMessage
}

func New(t *testing.T) *Tester {

	tt := &Tester{
		t: t,

		clients: make(map[string]*client),

		codecs:      make(map[string]goka.Codec),
		topicQueues: make(map[string]*queue),
		storages:    make(map[string]storage.Storage),
	}
	tt.tmgr = NewMockTopicManager(tt, 1, 1)
	tt.producer = newProducerMock(tt.handleEmit)

	return tt
}

func (tt *Tester) nextClient() *client {
	tt.mClients.Lock()
	defer tt.mClients.Unlock()
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
		tt.mClients.RLock()
		defer tt.mClients.RUnlock()
		client, exists := tt.clients[clientID]
		if !exists {
			return nil, fmt.Errorf("cannot create consumergroup because no client registered with ID: %s", clientID)
		}

		if client.consumerGroup == nil {
			return nil, fmt.Errorf("Did not expect a group graph")
		}

		return client.consumerGroup, nil
	}
}

func (tt *Tester) ConsumerBuilder() goka.SaramaConsumerBuilder {
	return func(brokers []string, clientID string) (sarama.Consumer, error) {
		tt.mClients.RLock()
		defer tt.mClients.RUnlock()

		client, exists := tt.clients[clientID]
		if !exists {
			return nil, fmt.Errorf("cannot create sarama consumer because no client registered with ID: %s", clientID)
		}

		return client.consumer, nil
	}
}

// EmitterProducerBuilder creates a producer builder used for Emitters.
// Emitters need to flush when emitting messages.
func (tt *Tester) EmitterProducerBuilder() goka.ProducerBuilder {
	builder := tt.ProducerBuilder()
	return func(b []string, cid string, hasher func() hash.Hash32) (goka.Producer, error) {
		prod, err := builder(b, cid, hasher)
		return &flushingProducer{
			tester:   tt,
			producer: prod,
		}, err
	}
}

// handleEmit handles an Emit-call on the producerMock.
// This takes care of queueing calls
// to handled topics or putting the emitted messages in the emitted-messages-list
func (tt *Tester) handleEmit(topic string, key string, value []byte) *goka.Promise {
	promise := goka.NewPromise()
	tt.pushMessage(topic, key, value)
	return promise.Finish(nil)
}

func (tt *Tester) pushMessage(topic string, key string, data []byte) {

	tt.getOrCreateQueue(topic).push(key, data)
}

type consumerMock struct {
	sync.RWMutex
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

func (cm *consumerMock) catchup() int {
	cm.RLock()
	defer cm.RUnlock()
	var catchup int
	for _, pc := range cm.partConsumers {
		catchup += pc.catchup()
		return catchup
	}
	return catchup
}

func (cm *consumerMock) Topics() ([]string, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (cm *consumerMock) Partitions(topic string) ([]int32, error) {
	return nil, fmt.Errorf("not implemented")
}

func (cm *consumerMock) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	cm.Lock()
	defer cm.Unlock()
	if _, exists := cm.partConsumers[topic]; exists {
		return nil, fmt.Errorf("Got duplicate consume partition for topic %s", topic)
	}
	cons := &partConsumerMock{
		offset:   offset,
		queue:    cm.tester.getOrCreateQueue(topic),
		messages: make(chan *sarama.ConsumerMessage),
		errors:   make(chan *sarama.ConsumerError),
		closer: func() error {
			cm.Lock()
			cm.Unlock()
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

func (cm *consumerMock) waitRequiredConsumersStartup() {
	doCheck := func() bool {
		cm.RLock()
		defer cm.RUnlock()

		for topic := range cm.requiredTopics {
			_, ok := cm.partConsumers[topic]
			if !ok {
				return false
			}
		}
		return true
	}
	for !doCheck() {
		time.Sleep(100 * time.Millisecond)
	}
}

func (cm *consumerMock) requirePartConsumer(topic string) {
	cm.requiredTopics[topic] = true
}

type partConsumerMock struct {
	offset   int64
	closer   func() error
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
	queue    *queue
}

func (pcm *partConsumerMock) catchup() int {
	var numCatchup int
	for _, msg := range pcm.queue.messagesFromOffset(pcm.offset) {
		pcm.messages <- &sarama.ConsumerMessage{
			Key:       []byte(msg.key),
			Value:     msg.value,
			Topic:     pcm.queue.topic,
			Partition: 0,
			Offset:    msg.offset,
		}
		numCatchup++
	}

	return numCatchup
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
	// we need to expect a consumer group so we're creating one in the client
	if gg.GroupTable() != nil || len(gg.InputStreams()) > 0 {
		client.consumerGroup = NewConsumerGroup(tt.t)
	}

	// register codecs
	if gg.GroupTable() != nil {
		tt.registerCodec(gg.GroupTable().Topic(), gg.GroupTable().Codec())
	}

	for _, input := range gg.InputStreams() {
		tt.registerCodec(input.Topic(), input.Codec())
	}

	for _, output := range gg.OutputStreams() {
		tt.registerCodec(output.Topic(), output.Codec())
	}

	for _, join := range gg.JointTables() {
		tt.registerCodec(join.Topic(), join.Codec())
	}

	if loop := gg.LoopStream(); loop != nil {
		tt.registerCodec(loop.Topic(), loop.Codec())
	}

	for _, lookup := range gg.LookupTables() {
		tt.registerCodec(lookup.Topic(), lookup.Codec())
	}

	return client.clientID
}

func (tt *Tester) RegisterView(table goka.Table, c goka.Codec) string {
	client := tt.nextClient()
	client.requireConsumer(string(table))
	return client.clientID
}

// RegisterEmitter registers an emitter to be working with the tester.
func (tt *Tester) RegisterEmitter(topic goka.Stream, codec goka.Codec) {
	tt.registerCodec(string(topic), codec)
	tt.getOrCreateQueue(string(topic))
}

func (tt *Tester) getOrCreateQueue(topic string) *queue {
	tt.mQueues.Lock()
	defer tt.mQueues.Unlock()
	queue, exists := tt.topicQueues[topic]
	if !exists {
		queue = newQueue(topic)
		tt.topicQueues[topic] = queue
	}
	return queue
}

func (tt *Tester) codecForTopic(topic string) goka.Codec {
	codec, exists := tt.codecs[topic]
	if !exists {
		panic(fmt.Errorf("No codec for topic %s registered.", topic))
	}
	return codec
}

func (tt *Tester) registerCodec(topic string, codec goka.Codec) {
	// create a queue, we're going to need it anyway
	tt.getOrCreateQueue(topic)

	if existingCodec, exists := tt.codecs[topic]; exists {
		if reflect.TypeOf(codec) != reflect.TypeOf(existingCodec) {
			panic(fmt.Errorf("There are different codecs for the same topic. This is messed up (%#v, %#v)", codec, existingCodec))
		}
	}
	tt.codecs[topic] = codec
}

func (tt *Tester) TableValue(table goka.Table, key string) interface{} {
	return nil
}
func (tt *Tester) SetTableValue(table goka.Table, key string, value interface{}) {
}

func (tt *Tester) StorageBuilder() storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		return storage.NewMemory(), nil
	}
}

func (tt *Tester) ClearValues() {
}

func (tt *Tester) NewQueueTracker(topic string) *QueueTracker {
	return newQueueTracker(tt, tt.t, topic)
}

func (tt *Tester) waitStartup() {
	tt.mClients.RLock()
	defer tt.mClients.RUnlock()

	for _, client := range tt.clients {
		client.waitStartup()
	}

}
func (tt *Tester) waitForClients() {
	logger.Printf("waiting for consumers")

	tt.mClients.RLock()
	defer tt.mClients.RUnlock()
	for {
		var totalCatchup int
		for _, client := range tt.clients {
			totalCatchup += client.catchup()
		}

		if totalCatchup == 0 {
			break
		}
	}

	logger.Printf("waiting for consumers done")
}

func (tt *Tester) Consume(topic string, key string, msg interface{}) {
	tt.waitStartup()
	value := reflect.ValueOf(msg)
	if msg == nil || (value.Kind() == reflect.Ptr && value.IsNil()) {
		tt.pushMessage(topic, key, nil)
	} else {
		data, err := tt.codecForTopic(topic).Encode(msg)
		if err != nil {
			panic(fmt.Errorf("Error encoding value %v: %v", msg, err))
		}
		tt.pushMessage(topic, key, data)
	}

	tt.waitForClients()
}
