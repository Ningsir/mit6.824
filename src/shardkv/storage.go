package shardkv

import "fmt"

type ShardStatus string

const (
	// 服务状态，可以为请求提供服务
	Serving ShardStatus = "Serving"
	// 正在拉取远程的shard数据，还不能提供服务, 当拉取到数据时，状态变更为NotifyDeleting
	Pulling ShardStatus = "Pulling"
	// 用于通知远程group删除对应的shard
	NotifyDeleting ShardStatus = "NotifyDeleting"
	// 正在Pushing到远程的shard，Push执行完成之后需要将状态更新为Gcing
	Pushing ShardStatus = "Pushing"
	// 将要删除的shard，也就是需要push到远程的shard，不能提供服务
	Gcing ShardStatus = "Gcing"
)

type ShardKVStorage struct {
	Status ShardStatus
	Data   map[string]string
}

func (kv *ShardKVStorage) Get(key string) (string, bool) {
	value, ok := kv.Data[key]
	return value, ok
}

func (kv *ShardKVStorage) Put(key string, value string) {
	kv.Data[key] = value
}

func (kv *ShardKVStorage) Append(key string, value string) {
	old := kv.Data[key]
	kv.Data[key] = old + value
}

func (kv *ShardKVStorage) DeepCopy() *ShardKVStorage {
	copy_storage := MakeStorage()
	copy_storage.Status = kv.Status
	for key, value := range kv.Data {
		copy_storage.Data[key] = value
	}
	return copy_storage
}

func (kv *ShardKVStorage) String() string {
	return fmt.Sprintf("{Status=%s, Size=%d}", kv.Status, len(kv.Data))
}

func MakeStorage() *ShardKVStorage {
	storage := new(ShardKVStorage)
	storage.Data = make(map[string]string)
	return storage
}
