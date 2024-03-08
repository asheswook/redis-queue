package redis_queue

import "errors"

var ErrPushFailed = errors.New("redis queue push failed")
var ErrPopFailed = errors.New("redis queue pop failed")
var ErrAckPopFailed = errors.New("redis queue safePop failed")
var ErrAckPopEOF = errors.New("EOF while ackPop")
var ErrUnexpectedType = errors.New("redis queue message is unexpected type")
var ErrAckNotAvailable = errors.New("ack is not available for this message, only safe popped message can be acked")
var ErrAckFailed = errors.New("ack failed")
var ErrTimestampUpdateFailed = errors.New("timestamp update failed")
