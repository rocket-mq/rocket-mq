package producer

import (
	"github.com/aliyunmq/mq-http-go-sdk"

	"github.com/rocket-mq/rocket-mq/v4/producer/message"
)

type Producer struct {
	endpoint   string
	accessKey  string
	secretKey  string
	topic      string
	instanceId string
	groupId    string
	producer   mq_http_sdk.MQProducer
}

type Option func(*Producer)

func WithGroupId(groupId string) Option {
	return func(p *Producer) {
		p.groupId = groupId
	}
}

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
	p.producer = client.GetProducer(instanceId, topic)

	return p
}

func (p *Producer) PublishMessage(data *message.Message) (mq_http_sdk.PublishMessageResponse, error) {
	return p.producer.PublishMessage(data.Wrap())
}
