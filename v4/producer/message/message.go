package message

import (
	"time"

	"github.com/aliyunmq/mq-http-go-sdk"
)

// Message is a wrapper of rocketmq message
type Message struct {
	body                   string
	tag                    string
	key                    string
	properties             map[string]string
	startDeliverTime       int64
	transCheckImmunityTime int
	shardingKey            string
}

type Option func(*Message)

// WithTag sets the tag of the message
func WithTag(tag string) Option {
	return func(m *Message) {
		m.tag = tag
	}
}

// WithKey sets the key of the message
func WithKey(key string) Option {
	return func(m *Message) {
		m.key = key
	}
}

func WithProperties(properties map[string]string) Option {
	return func(m *Message) {
		m.properties = properties
	}
}

// WithStartDeliverTime sets the delay timestamp of the message
// 延时消息
func WithStartDeliverTime(startDeliverTime int64) Option {
	return func(m *Message) {
		m.startDeliverTime = startDeliverTime
	}
}

func WithDelayTime(t time.Time) Option {
	return func(m *Message) {
		ms := t.Sub(time.Now()).Milliseconds()
		if ms > 0 {
			m.startDeliverTime = t.Sub(time.Now()).Milliseconds()
		}
	}
}

func WithTransCheckImmunityTime(transCheckImmunityTime int) Option {
	return func(m *Message) {
		m.transCheckImmunityTime = transCheckImmunityTime
	}
}

func WithShardingKey(shardingKey string) Option {
	return func(m *Message) {
		m.shardingKey = shardingKey
	}
}

// New creates a new message
func New(body string, opts ...Option) *Message {
	message := &Message{
		body:       body,
		properties: make(map[string]string),
	}

	for _, opt := range opts {
		opt(message)
	}

	return message
}

// Wrap converts the message to a rocketmq message
func (m *Message) Wrap() mq_http_sdk.PublishMessageRequest {
	message := mq_http_sdk.PublishMessageRequest{
		MessageBody: m.body,
		Properties:  make(map[string]string),
	}

	if m.tag != "" {
		message.MessageTag = m.tag
	}

	if len(m.properties) > 0 {
		message.Properties = m.properties
	}

	if m.key != "" {
		message.MessageKey = m.key
	}

	if m.startDeliverTime > 0 {
		message.StartDeliverTime = m.startDeliverTime
	}

	if m.transCheckImmunityTime > 0 {
		message.TransCheckImmunityTime = m.transCheckImmunityTime
	}

	if m.shardingKey != "" {
		message.ShardingKey = m.shardingKey
	}

	return message
}
