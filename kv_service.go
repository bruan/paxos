package main

import (
	"fmt"
	"sync"
)

const (
	Set int = iota + 1
	Del
	Get
)

type kvOpInfo struct {
	opType  int
	key     string
	value   string
	version int32
}

type kvValue struct {
	value   string
	version int32
}

type KVService struct {
	node           *Node
	storageLock    sync.RWMutex
	storage        map[string]*kvValue
	instanceGroups []*InstanceGroup
}

// NewKVService 创建KVService
func NewKVService(nodeID int, listenAddr string, nodeAddrs map[int]string, groupCount int) *KVService {
	kvService := &KVService{}
	kvService.node = newNode(nodeID, listenAddr, nodeAddrs)
	kvService.storage = make(map[string]*kvValue)
	kvService.instanceGroups = make([]*InstanceGroup, groupCount)
	for i := 0; i < groupCount; i++ {
		kvService.instanceGroups[i] = kvService.node.newInstanceGroup(i, kvService)
	}
	return kvService
}

// Set 设置一个值
func (kv *KVService) Set(key string, value string, version int32) (string, int32) {
	hashKey := djbhash(key) % uint64(len(kv.instanceGroups))
	instanceGroup := kv.instanceGroups[hashKey]

	valueBuf := serializeOpInfo(kvOpInfo{opType: Set, key: key, value: value, version: version})
	resultBuf, err := instanceGroup.commit(valueBuf)
	if err != nil {
		return "", 0
	}

	kvOpInfo := unserializeOpInfo(resultBuf)
	if kvOpInfo == nil {
		return "", 0
	}

	return kvOpInfo.value, kvOpInfo.version
}

// Del 删除一个值
func (kv *KVService) Del(key string, version int32) (string, int32) {
	hashKey := djbhash(key) % uint64(len(kv.instanceGroups))
	instanceGroup := kv.instanceGroups[hashKey]

	valueBuf := serializeOpInfo(kvOpInfo{opType: Del, key: key, value: "*", version: version})
	resultBuf, err := instanceGroup.commit(valueBuf)

	if err != nil {
		return "", 0
	}

	kvOpInfo := unserializeOpInfo(resultBuf)
	if kvOpInfo == nil {
		return "", 0
	}

	return kvOpInfo.value, kvOpInfo.version
}

// GetLocal 从本节点获取值，不保证一致性
func (kv *KVService) GetLocal(key string) (string, int32) {
	kv.storageLock.RLock()
	defer kv.storageLock.RUnlock()

	kvValue := kv.storage[key]
	if kvValue == nil {
		return "", 0
	}

	return kvValue.value, kvValue.version
}

// GetGlobal 从全局获取值，保证一致性
func (kv *KVService) GetGlobal(key string) (string, int32) {
	hashKey := djbhash(key) % uint64(len(kv.instanceGroups))
	instanceGroup := kv.instanceGroups[hashKey]

	valueBuf := serializeOpInfo(kvOpInfo{opType: Get, key: key, value: "*", version: 0})
	resultValueBuf, err := instanceGroup.commit(valueBuf)
	if err != nil {
		return "", 0
	}

	kvOpValue := unserializeOpInfo(resultValueBuf)
	if kvOpValue == nil {
		return "", 0
	}

	return kvOpValue.value, kvOpValue.version
}

func (kv *KVService) exec(val string) string {
	kvOpInfo := unserializeOpInfo(val)
	if kvOpInfo == nil {
		return "err"
	}

	switch kvOpInfo.opType {
	case Set:
		{
			kv.storageLock.Lock()
			oldValue := kv.storage[kvOpInfo.key]
			if oldValue != nil && oldValue.version != kvOpInfo.version {
				kvOpInfo.value = oldValue.value
				kvOpInfo.version = oldValue.version
				resultBuf := serializeOpInfo(*kvOpInfo)
				kv.storageLock.Unlock()
				return resultBuf
			}

			newValue := kvValue{value: kvOpInfo.value, version: kvOpInfo.version}
			kv.storage[kvOpInfo.key] = &newValue
			kv.storageLock.Unlock()

			return val
		}

	case Del:
		{
			kv.storageLock.Lock()
			oldValue := kv.storage[kvOpInfo.key]
			if oldValue != nil && oldValue.version != kvOpInfo.version {
				kvOpInfo.value = oldValue.value
				kvOpInfo.version = oldValue.version
				resultBuf := serializeOpInfo(*kvOpInfo)
				kv.storageLock.Unlock()
				return resultBuf
			}

			kvOpInfo.value = oldValue.value
			resultBuf := serializeOpInfo(*kvOpInfo)
			delete(kv.storage, kvOpInfo.key)
			kv.storageLock.Unlock()

			return resultBuf
		}

	case Get:
		{
			kv.storageLock.RLock()

			kvValue := kv.storage[kvOpInfo.key]
			if kvValue == nil {
				resultBuf := serializeOpInfo(*kvOpInfo)
				kv.storageLock.RUnlock()
				return resultBuf
			}

			kvOpInfo.value = kvValue.value
			kvOpInfo.version = kvValue.version
			resultBuf := serializeOpInfo(*kvOpInfo)
			kv.storageLock.RUnlock()
			return resultBuf
		}
	}

	return ""
}

func serializeOpInfo(value kvOpInfo) string {
	return fmt.Sprintf("op=%d key=%s val=%s ver=%d", value.opType, value.key, value.value, value.version)
}

func unserializeOpInfo(value string) *kvOpInfo {
	kvOpInfo := kvOpInfo{}
	_, err := fmt.Sscanf(value, "op=%d key=%s val=%s ver=%d", &kvOpInfo.opType, &kvOpInfo.key, &kvOpInfo.value, &kvOpInfo.version)
	if err != nil {
		return nil
	}

	return &kvOpInfo
}

func djbhash(str string) uint64 {
	hash := uint64(0)
	for i := 0; i < len(str); i++ {
		hash = ((hash << 5) + hash) + uint64(str[i])
	}

	return hash
}
