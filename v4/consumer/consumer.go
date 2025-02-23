package consumer

import (
	"time"

	"github.com/aliyunmq/mq-http-go-sdk"
)

type Consumer struct {
	endpoint   string
	accessKey  string
	secretKey  string
	topic      string
	instanceId string
	groupId    string
	tag        string
	consumer   mq_http_sdk.MQConsumer
}

type Option func(*Consumer)

func WithTag(tag string) Option {
	return func(c *Consumer) {
		c.tag = tag
	}
}

func New(endpoint, accessKey, secretKey, topic, instanceId, groupId string, opts ...Option) *Consumer {
	c := &Consumer{
		endpoint:   endpoint,
		accessKey:  accessKey,
		secretKey:  secretKey,
		topic:      topic,
		instanceId: instanceId,
		groupId:    groupId,
	}

	for _, opt := range opts {
		opt(c)
	}

	client := mq_http_sdk.NewAliyunMQClient(endpoint, accessKey, secretKey, "")
	c.consumer = client.GetConsumer(instanceId, topic, groupId, c.tag)

	return c
}

func (c *Consumer) Receive(numOfMessage int32, waitSeconds int64, f func(mq_http_sdk.ConsumeMessageEntry)) {
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
					f(v)
				}
				endChan <- struct{}{}
			}
		case <-time.After(35 * time.Second):
			{
				endChan <- struct{}{}
			}
		}
	}()

	c.consumer.ConsumeMessage(respChan, errChan, numOfMessage, waitSeconds)

	<-endChan
}

func (c *Consumer) Ack(cme mq_http_sdk.ConsumeMessageEntry) error {
	return c.consumer.AckMessage([]string{cme.ReceiptHandle})
}
