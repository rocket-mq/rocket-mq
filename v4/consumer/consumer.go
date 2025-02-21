package consumer

import (
	"fmt"
	"strings"
	"time"

	"github.com/aliyunmq/mq-http-go-sdk"
	"github.com/gogap/errors"
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

func (c *Consumer) Receive(numOfMessage int32, waitSeconds int64) (<-chan mq_http_sdk.ConsumeMessageResponse, <-chan error) {
	respChan := make(chan mq_http_sdk.ConsumeMessageResponse)
	errChan := make(chan error)
	_errChan := make(chan error)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				errChan <- fmt.Errorf("%s", r)
			}
		}()
		select {
		case err := <-_errChan:
			if !strings.Contains(err.(errors.ErrCode).Error(), "MessageNotExist") {
				errChan <- err
			}
		}
	}()

	c.consumer.ConsumeMessage(respChan, _errChan, numOfMessage, waitSeconds)

	return respChan, errChan
}

func (c *Consumer) Ack(receiptHandles []string) error {
	return c.consumer.AckMessage(receiptHandles)
}

func (c *Consumer) ReceiveAndAutoAck(numOfMessage int32, waitSeconds int64) ([]mq_http_sdk.ConsumeMessageEntry, error) {
	respChan := make(chan mq_http_sdk.ConsumeMessageResponse)
	errChan := make(chan error)
	message := make(chan mq_http_sdk.ConsumeMessageEntry, numOfMessage)
	list := make([]mq_http_sdk.ConsumeMessageEntry, 0, numOfMessage)
	stop := make(chan error)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				stop <- fmt.Errorf("%s", r)
			}
		}()

		select {
		case resp := <-respChan:
			{
				var handles []string
				for _, v := range resp.Messages {
					handles = append(handles, v.ReceiptHandle)
					message <- v
				}
				if err := c.consumer.AckMessage(handles); err != nil {
					stop <- err
				}
				stop <- nil
			}
		case err := <-errChan:
			{
				if !strings.Contains(err.(errors.ErrCode).Error(), "MessageNotExist") {
					stop <- err
				}
			}
		case <-time.After(35 * time.Second):
			{
				stop <- nil
			}
		}

	}()

	c.consumer.ConsumeMessage(respChan, errChan, numOfMessage, waitSeconds)

	err := <-stop

	return list, err
}
