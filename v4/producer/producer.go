package producer

import (
	"time"

	"github.com/aliyunmq/mq-http-go-sdk"

	"github.com/rocket-mq/rocket-mq/v4/producer/message"
)

type TransactionResolution int32

const (
	UNKNOWN TransactionResolution = iota // 开始生成枚举值, 默认为0
	COMMIT
	ROLLBACK
)

type Producer struct {
	endpoint   string
	accessKey  string
	secretKey  string
	topic      string
	instanceId string
	groupId    string
	producer   mq_http_sdk.MQProducer
	trans      mq_http_sdk.MQTransProducer
	checker    func(mq_http_sdk.ConsumeMessageEntry) TransactionResolution
}

type Option func(*Producer)

func WithTrans(groupId string, checker func(mq_http_sdk.ConsumeMessageEntry) TransactionResolution) Option {
	return func(p *Producer) {
		p.groupId = groupId
		p.checker = checker
	}
}

// New 实例化生产者
func New(endpoint, accessKey, secretKey, topic, instanceId string, opts ...Option) *Producer {
	p := &Producer{
		endpoint:   endpoint,
		accessKey:  accessKey,
		secretKey:  secretKey,
		topic:      topic,
		instanceId: instanceId,
	}

	for _, opt := range opts {
		opt(p)
	}

	client := mq_http_sdk.NewAliyunMQClient(endpoint, accessKey, secretKey, "")

	if p.checker == nil {
		p.producer = client.GetProducer(instanceId, topic)
		return p
	}

	p.trans = client.GetTransProducer(instanceId, topic, p.groupId)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				return
			}
		}()

		for {
			endChan := make(chan struct{})
			respChan := make(chan mq_http_sdk.ConsumeMessageResponse)
			errChan := make(chan error)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						endChan <- struct{}{}
					}
				}()

				select {
				case resp := <-respChan:
					{
						for _, v := range resp.Messages {
							if tr := p.checker(v); tr == COMMIT {
								_ = p.trans.Commit(v.ReceiptHandle)
							} else if tr == ROLLBACK {
								_ = p.trans.Rollback(v.ReceiptHandle)
							}
						}
						endChan <- struct{}{}
					}
				case <-errChan:
					{
						endChan <- struct{}{}
					}
				case <-time.After(35 * time.Second):
					{
						endChan <- struct{}{}
					}
				}
			}()

			p.trans.ConsumeHalfMessage(respChan, errChan, 3, 3)

			<-endChan
		}
	}()

	return p
}

// PublishMessage 发送消息
func (p *Producer) PublishMessage(data *message.Message) (mq_http_sdk.PublishMessageResponse, error) {
	return p.producer.PublishMessage(data.Wrap())
}

// PublishTransMessage 发送事务消息
func (p *Producer) PublishTransMessage(data *message.Message) (mq_http_sdk.PublishMessageResponse, error) {
	return p.trans.PublishMessage(data.Wrap())
}

// Commit 提交事务消息
func (p *Producer) Commit(receiptHandle string) error {
	return p.trans.Commit(receiptHandle)
}

// Rollback 回滚事务消息
func (p *Producer) Rollback(receiptHandle string) error {
	return p.trans.Rollback(receiptHandle)
}
