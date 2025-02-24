package producer

import (
	"time"

	"github.com/apache/rocketmq-clients/golang/v5"
)

// Message is a wrapper of rocketmq message
type Message struct {
	topic          string
	body           []byte
	keys           []string
	tag            string
	delayTimestamp time.Time
	messageGroup   string
}

type Option func(*Message)

// WithKeys sets the keys of the message
func WithKeys(keys ...string) Option {
	return func(m *Message) {
		for _, key := range keys {
			if key == "" {
				continue
			}
			m.keys = append(m.keys, key)
		}
	}
}

// WithTag sets the tag of the message
func WithTag(tag string) Option {
	return func(m *Message) {
		m.tag = tag
	}
}

// WithDelayTimestamp sets the delay timestamp of the message
// 延时消息
func WithDelayTimestamp(delayTimestamp time.Duration) Option {
	return func(m *Message) {
		m.delayTimestamp = time.Now().Add(delayTimestamp)
	}
}

func WithDelayTime(t time.Time) Option {
	return func(m *Message) {
		m.delayTimestamp = t
	}
}

// WithMessageGroup sets the message group of the message
// 顺序消息
func WithMessageGroup(group string) Option {
	return func(m *Message) {
		m.messageGroup = group
	}
}

// NewMessage creates a new message
func NewMessage(topic string, body []byte, opts ...Option) *Message {
	message := &Message{
		topic: topic,
		body:  body,
	}

	for _, opt := range opts {
		opt(message)
	}

	return message
}

// Wrap converts the message to a rocketmq message
func (m *Message) Wrap() *golang.Message {
	message := &golang.Message{
		Topic: m.topic,
		Body:  m.body,
	}

	if len(m.keys) > 0 {
		message.SetKeys(m.keys...)
	}

	if m.tag != "" {
		message.SetTag(m.tag)
	}

	if !m.delayTimestamp.IsZero() {
		// 延时消息
		message.SetDelayTimestamp(m.delayTimestamp)
	}

	if m.messageGroup != "" {
		// 顺序消息
		message.SetMessageGroup(m.messageGroup)
	}

	return message
}
