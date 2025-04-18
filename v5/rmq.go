package v5

import (
	"os"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5"
)

type RocketMQ struct {
	debug              string
	logPath            string
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
		if debug {
			rmq.debug = "true"
			return
		}
		rmq.debug = "false"
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

func WithLogPath(logPath string) Option {
	return func(rmq *RocketMQ) {
		rmq.logPath = logPath
	}
}

func New(endpoint, accessKey, secretKey string, topic []string, opts ...Option) *RocketMQ {
	logPath, _ := os.Getwd()
	rmq := &RocketMQ{
		debug:     "true",
		logPath:   logPath,
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
