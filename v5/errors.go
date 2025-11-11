package v5

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsMessageNotFoundErr 是否为消费者拉取不到消息的错误
func IsMessageNotFoundErr(err error) bool {
	errStr := err.Error()
	if errStr == "CODE: MESSAGE_NOT_FOUND, MESSAGE: no new message" {
		return true
	}
	if errStr == "CODE: MESSAGE_NOT_FOUND, MESSAGE: no match message" {
		return true
	}
	if errStr == "CODE: INTERNAL_SERVER_ERROR, MESSAGE: null. NullPointerException. org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageActivity.receiveMessage(ReceiveMessageActivity.java:63)" {
		return true
	}
	return false
}

// IsReceiveTimeoutErr 是否为消费者拉取消息超时错误
func IsReceiveTimeoutErr(err error) bool {
	if e, ok := status.FromError(err); ok {
		if e.Code() == codes.DeadlineExceeded {
			return true
		}
	}
	errStr := err.Error()
	if errStr == "[error] CODE=DEADLINE_EXCEEDED" {
		return true
	}
	return false
}
