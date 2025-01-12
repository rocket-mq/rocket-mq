package rocketmq

import (
	"time"

	"github.com/apache/rocketmq-clients/golang/v5"
)

type RocketMQ struct {
	debug              bool
	topic              []string
	endpoint           string
	accessKey          string
	secretKey          string
	consumerGroup      string
	nameSpace          string
	awaitDuration      time.Duration
	transactionChecker func(*golang.MessageView) golang.TransactionResolution
}

type Option func(*RocketMQ)

func WithDebug(debug bool) Option {
	return func(rmq *RocketMQ) {
		rmq.debug = debug
	}
}

func WithConsumerGroup(consumerGroup string) Option {
	return func(rmq *RocketMQ) {
		rmq.consumerGroup = consumerGroup
	}
}

func WithNameSpace(nameSpace string) Option {
	return func(rmq *RocketMQ) {
		rmq.nameSpace = nameSpace
	}
}

func WithTransactionChecker(transactionChecker func(*golang.MessageView) golang.TransactionResolution) Option {
	return func(rmq *RocketMQ) {
		rmq.transactionChecker = transactionChecker
	}
}

func WithAwaitDuration(awaitDuration time.Duration) Option {
	return func(rmq *RocketMQ) {
		rmq.awaitDuration = awaitDuration
	}
}

func New(endpoint, accessKey, secretKey string, topic []string, opts ...Option) *RocketMQ {
	rmq := &RocketMQ{
		endpoint:  endpoint,
		accessKey: accessKey,
		secretKey: secretKey,
		topic:     topic,
	}

	for _, opt := range opts {
		opt(rmq)
	}

	return rmq
}
