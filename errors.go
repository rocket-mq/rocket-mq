package rocketmq

func IsMessageNotFoundErr(err error) bool {
	return err.Error() == "CODE: MESSAGE_NOT_FOUND, MESSAGE: no new message" || err.Error() == "CODE: INTERNAL_SERVER_ERROR, MESSAGE: null. NullPointerException. org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageActivity.receiveMessage(ReceiveMessageActivity.java:63)"
}
