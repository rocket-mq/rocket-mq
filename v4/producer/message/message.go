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
// 毫秒时间戳
func WithStartDeliverTime(startDeliverTime int64) Option {
	return func(m *Message) {
		m.startDeliverTime = startDeliverTime
	}
}

// WithDelayTime sets the delay time of the message
// 延时消息
func WithDelayTime(t time.Time) Option {
	return func(m *Message) {
		m.startDeliverTime = t.UnixMilli()
	}
}

// WithTransCheckImmunityTime sets the trans check immunity time of the message
// 事务消息
// 第一次消息回查的最快时间,单位秒
func WithTransCheckImmunityTime(transCheckImmunityTime int) Option {
	return func(m *Message) {
		m.transCheckImmunityTime = transCheckImmunityTime
	}
}

// WithShardingKey sets the sharding key of the message
// 顺序消息
// 分区顺序消息中区分不同分区的关键字段
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

// NewWithBytes creates a new message with bytes
func NewWithBytes(body []byte, opts ...Option) *Message {
	return New(string(body), opts...)
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
