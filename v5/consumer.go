package v5

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"
)

// SimpleConsumer 消费者
type SimpleConsumer struct {
	consumer golang.SimpleConsumer
}

// SimpleConsumer 创建消费者
func (rmq *RocketMQ) SimpleConsumer() (*SimpleConsumer, error) {
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
	}, golang.WithSimpleAwaitDuration(rmq.awaitDuration), golang.WithSimpleSubscriptionExpressions(se))
	if err != nil {
		return nil, err
	}

	if err = consumer.Start(); err != nil {
		return nil, err
	}

	return &SimpleConsumer{consumer: consumer}, nil
}

// Close 关闭消费者
func (c *SimpleConsumer) Close() error {
	return c.consumer.GracefulStop()
}

// Receive 接收消息
// maxMessageNum 一次接收的最大消息数
// invisibleDuration 设置消息的不可见时间，当消息被消费者拉取后，该消息在指定的时间内对其他消费者不可见，以确保消息处理的幂等性，需要 > 20s
func (c *SimpleConsumer) Receive(ctx context.Context, maxMessageNum int32, invisibleDuration time.Duration) ([]*golang.MessageView, error) {
	return c.consumer.Receive(ctx, maxMessageNum, invisibleDuration)
}

// Ack 确认消息
func (c *SimpleConsumer) Ack(ctx context.Context, messageView *golang.MessageView) error {
	return c.consumer.Ack(ctx, messageView)
}

// ReceiveAndAutoAck 接收消息并自动确认
func (c *SimpleConsumer) ReceiveAndAutoAck(ctx context.Context, maxMessageNum int32, invisibleDuration time.Duration) ([]*golang.MessageView, error) {
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

// PushConsumer 创建推送型消费者
// pushConsumptionThreadCount 用于设置 Push 消费模式下的消费线程数
// pushMaxCacheMessageCount 用于设置 Push 消费模式下客户端本地缓存的最大消息数量
func (rmq *RocketMQ) PushConsumer(pushConsumptionThreadCount, pushMaxCacheMessageCount int32, consume func(*golang.MessageView) golang.ConsumerResult, closeChan <-chan struct{}) error {
	_ = os.Setenv("mq.consoleAppender.enabled", rmq.debug)
	_ = os.Setenv("user.home", strings.TrimRight(rmq.logPath, "/"))
	golang.ResetLogger()

	se := make(map[string]*golang.FilterExpression, len(rmq.topic))
	for _, topic := range rmq.topic {
		se[topic] = golang.SUB_ALL
	}

	consumer, err := golang.NewPushConsumer(&golang.Config{
		Endpoint:      rmq.endpoint,
		NameSpace:     rmq.nameSpace,
		ConsumerGroup: rmq.consumerGroup,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    rmq.accessKey,
			AccessSecret: rmq.secretKey,
		},
	},
		golang.WithPushAwaitDuration(rmq.awaitDuration),
		golang.WithPushSubscriptionExpressions(se),
		golang.WithPushConsumptionThreadCount(pushConsumptionThreadCount),
		golang.WithPushMaxCacheMessageCount(pushMaxCacheMessageCount),
		golang.WithPushMessageListener(&golang.FuncMessageListener{
			Consume: consume,
		}),
	)
	if err != nil {
		return err
	}

	if err = consumer.Start(); err != nil {
		return err
	}

	defer consumer.GracefulStop()

	<-closeChan

	return nil
}
