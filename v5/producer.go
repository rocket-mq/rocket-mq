package v5

import (
	"context"
	"os"
	"strings"

	"github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"

	"github.com/rocket-mq/rocket-mq/v5/producer"
)

// Producer 生产者
type Producer struct {
	producer golang.Producer
}

// Producer 创建生产者
func (rmq *RocketMQ) Producer() (*Producer, error) {
	_ = os.Setenv("mq.consoleAppender.enabled", rmq.debug)
	_ = os.Setenv("user.home", strings.TrimRight(rmq.logPath, "/"))
	golang.ResetLogger()

	opts := []golang.ProducerOption{
		golang.WithTopics(rmq.topic...),
	}
	if rmq.transactionChecker != nil {
		opts = append(opts, golang.WithTransactionChecker(&golang.TransactionChecker{
			Check: rmq.transactionChecker,
		}))
	}

	p, err := golang.NewProducer(&golang.Config{
		Endpoint:  rmq.endpoint,
		NameSpace: rmq.nameSpace,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    rmq.accessKey,
			AccessSecret: rmq.secretKey,
		},
	}, opts...)
	if err != nil {
		return nil, err
	}

	if err = p.Start(); err != nil {
		return nil, err
	}

	return &Producer{producer: p}, nil
}

// Close 关闭生产者
func (p *Producer) Close() error {
	return p.producer.GracefulStop()
}

// Send 发送消息
func (p *Producer) Send(ctx context.Context, data *producer.Message) ([]*golang.SendReceipt, error) {
	return p.producer.Send(ctx, data.Wrap())
}

// SendAsync 异步发送消息
func (p *Producer) SendAsync(ctx context.Context, data *producer.Message, f func(context.Context, []*golang.SendReceipt, error)) {
	p.producer.SendAsync(ctx, data.Wrap(), f)
}

// SendWithTransaction 发送事务消息
func (p *Producer) SendWithTransaction(ctx context.Context, data *producer.Message) ([]*golang.SendReceipt, golang.Transaction, error) {
	transaction := p.producer.BeginTransaction()
	sendReceipt, err := p.producer.SendWithTransaction(ctx, data.Wrap(), transaction)
	return sendReceipt, transaction, err
}
