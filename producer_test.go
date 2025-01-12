package rocketmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5"

	"github.com/rocket-mq/rocket-mq"
	"github.com/rocket-mq/rocket-mq/producer"
)

var producers *rocketmq.Producer

func getProducer(transactionChecker func(*golang.MessageView) golang.TransactionResolution) {
	var err error
	if producers, err = rocketmq.New(
		config.Endpoint,
		config.AccessKey,
		config.SecretKey,
		config.Topic,
		rocketmq.WithNameSpace(config.NameSpace),
		rocketmq.WithTransactionChecker(transactionChecker),
		rocketmq.WithDebug(true),
	).Producer(); err != nil {
		panic(err)
	}
}

// 发送普通消息
func TestSend(t *testing.T) {
	getProducer(nil)
	defer producers.Close()
	message := producer.NewMessage("test-1", []byte("hello world"),
		producer.WithTag("tag-1"),
		producer.WithKeys("key-1"),
	)
	res, err := producers.Send(context.Background(), message)
	if err != nil {
		t.Fatal(err)
		return
	}

	for _, r := range res {
		t.Log("message id：", r.MessageID)
	}
}

// 发送延时消息
func TestDelaySend(t *testing.T) {
	getProducer(nil)
	defer producers.Close()
	message := producer.NewMessage("test-2", []byte("hello world"),
		producer.WithTag("tag-2"),
		producer.WithKeys("key-2"),
		producer.WithDelayTimestamp(time.Hour),
	)
	res, err := producers.Send(context.Background(), message)
	if err != nil {
		t.Fatal(err)
		return
	}

	for _, r := range res {
		t.Log("message id：", r.MessageID)
	}
}

// 发送异步消息
func TestSendAsync(t *testing.T) {
	getProducer(nil)
	defer producers.Close()

	message := producer.NewMessage("test-1", []byte("hello world"),
		producer.WithTag("tag-1"),
		producer.WithKeys("key-1"),
	)

	producers.SendAsync(context.Background(), message, func(ctx context.Context, resp []*golang.SendReceipt, err error) {
		if err != nil {
			t.Error(err)
			return
		}
		for _, r := range resp {
			t.Log("message id：", r.MessageID)
		}
	})

	time.Sleep(time.Second * 15)
}

// 发送顺序消息
func TestFifoSend(t *testing.T) {
	getProducer(nil)
	defer producers.Close()

	message := producer.NewMessage("test-3", []byte("hello world"),
		producer.WithTag("tag-3"),
		producer.WithKeys("key-3"),
		producer.WithMessageGroup("fifo"),
	)

	res, err := producers.Send(context.Background(), message)
	if err != nil {
		t.Fatal(err)
		return
	}

	for _, r := range res {
		t.Log("message id：", r.MessageID)
	}
}

// 发送事务消息
func TestTransactionSend(t *testing.T) {
	getProducer(func(mv *golang.MessageView) golang.TransactionResolution {
		t.Log("from transaction checker，body：", string(mv.GetBody()))
		t.Log("from transaction checker，message id：", mv.GetMessageId())
		return golang.COMMIT
	})
	defer producers.Close()

	message := producer.NewMessage("test-4", []byte("hello world"),
		producer.WithTag("tag-4"),
		producer.WithKeys("key-4"),
	)

	res, transaction, err := producers.SendWithTransaction(context.Background(), message)
	if err != nil {
		t.Fatal(err)
		return
	}

	for _, r := range res {
		t.Log("message id：", r.MessageID)
	}

	time.Sleep(time.Second * 5000)

	_ = transaction.Commit()
}
