package consumer

import (
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
