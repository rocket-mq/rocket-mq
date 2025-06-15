package v5

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

// IsReceiveTimoutErr 是否为消费者拉取消息超时错误
func IsReceiveTimoutErr(err error) bool {
	errStr := err.Error()
	if errStr == "[error] CODE=DEADLINE_EXCEEDED" {
		return true
	}
	return false
}
