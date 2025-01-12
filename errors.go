package rocketmq

func IsMessageNotFoundErr(err error) bool {
	return err.Error() == "CODE: MESSAGE_NOT_FOUND, MESSAGE: no new message"
}
