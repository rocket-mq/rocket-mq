package v5

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// Consumer 消费者
type Consumer struct {
	consumer golang.SimpleConsumer
}

// Consumer 创建消费者
func (rmq *RocketMQ) Consumer() (*Consumer, error) {
	_ = os.Setenv("mq.consoleAppender.enabled", rmq.debug)
	_ = os.Setenv("user.home", strings.TrimRight(rmq.logPath, "/"))
	golang.ResetLogger()

	se := make(map[string]*golang.FilterExpression, len(rmq.topic))
	for _, topic := range rmq.topic {
		se[topic] = golang.SUB_ALL
	}

	consumer, err := golang.NewSimpleConsumer(&golang.Config{
		Endpoint:      rmq.endpoint,
		NameSpace:     rmq.nameSpace,
		ConsumerGroup: rmq.consumerGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    rmq.accessKey,
			AccessSecret: rmq.secretKey,
		},
	}, golang.WithAwaitDuration(rmq.awaitDuration), golang.WithSubscriptionExpressions(se))
	if err != nil {
		return nil, err
	}

	if err = consumer.Start(); err != nil {
		return nil, err
	}

	return &Consumer{consumer: consumer}, nil
}

// Close 关闭消费者
func (c *Consumer) Close() error {
	return c.consumer.GracefulStop()
}

// Receive 接收消息
func (c *Consumer) Receive(ctx context.Context, maxMessageNum int32, invisibleDuration time.Duration) ([]*golang.MessageView, error) {
	return c.consumer.Receive(ctx, maxMessageNum, invisibleDuration)
}

// Ack 确认消息
func (c *Consumer) Ack(ctx context.Context, messageView *golang.MessageView) error {
	return c.consumer.Ack(ctx, messageView)
}

// ReceiveAndAutoAck 接收消息并自动确认
func (c *Consumer) ReceiveAndAutoAck(ctx context.Context, maxMessageNum int32, invisibleDuration time.Duration) ([]*golang.MessageView, error) {
	messageView, err := c.consumer.Receive(ctx, maxMessageNum, invisibleDuration)
	if err != nil {
		return nil, err
	}

	mvs := make([]*golang.MessageView, 0, len(messageView))

	for _, mv := range messageView {
		if err = c.consumer.Ack(ctx, mv); err != nil {
			continue
		}
		mvs = append(mvs, mv)
	}

	return mvs, nil
}
