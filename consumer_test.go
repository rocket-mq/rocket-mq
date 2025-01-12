package rocketmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rocket-mq/rocket-mq"
)

var consumer *rocketmq.Consumer

func getConsumer() {
	var err error
	if consumer, err = rocketmq.New(
		config.Endpoint,
		config.AccessKey,
		config.SecretKey,
		config.Topic,
		rocketmq.WithConsumerGroup(config.ConsumeGroup),
		rocketmq.WithAwaitDuration(5*time.Second),
		rocketmq.WithNameSpace(config.NameSpace),
		rocketmq.WithDebug(true),
	).Consumer(); err != nil {
		panic(err)
	}
}

// TestReceive receive message
func TestReceive(t *testing.T) {
	getConsumer()
	defer consumer.Close()
	num := 0

	for {
		num++

		list, err := consumer.Receive(context.Background(), 1, 20*time.Second)
		if err != nil {
			if !rocketmq.IsMessageNotFoundErr(err) {
				t.Error(err)
				return
			}
			continue
		}

		for _, msg := range list {
			fmt.Println("message：", string(msg.GetBody()))
			fmt.Println("message id：", msg.GetMessageId())
			fmt.Println("message keys：", msg.GetKeys())
			fmt.Println("message tag：", msg.GetTag())
			fmt.Println("message topic：", msg.GetTopic())
			_ = consumer.Ack(context.Background(), msg)
		}

		if num >= 10 {
			break
		}

		time.Sleep(time.Second * 3)
	}
}

// TestReceiveAndAutoAck receive message and auto ack
func TestReceiveAndAutoAck(t *testing.T) {
	getConsumer()
	defer consumer.Close()
	num := 0

	for {
		num++

		list, err := consumer.ReceiveAndAutoAck(context.Background(), 1, 20*time.Second)
		if err != nil {
			if !rocketmq.IsMessageNotFoundErr(err) {
				t.Error(err)
				return
			}
			continue
		}

		for _, msg := range list {
			fmt.Println("message：", string(msg.GetBody()))
			fmt.Println("message id：", msg.GetMessageId())
			fmt.Println("message keys：", msg.GetKeys())
			fmt.Println("message tag：", msg.GetTag())
			fmt.Println("message topic：", msg.GetTopic())
		}

		if num >= 10 {
			break
		}

		time.Sleep(time.Second * 3)
	}
}
