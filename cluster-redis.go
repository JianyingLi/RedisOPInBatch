package main

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"log"
	"strconv"
	"strings"
)


var ctx = context.TODO()

/* Cluster Redis OP */

func giveClusterRedisConn() *redis.ClusterClient {
	c := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: strings.Split(Configs["redis_config"], ","),
	})
	ping, err := c.Ping(ctx).Result()
	if err != nil {
		log.Println("error", err)
		return nil
	}
	log.Println(ping)

	return c
}

func RedisScanAll(c *redis.ClusterClient, pattern string, batchSize int) []string {
	var result []string

	keys := make(map[string]bool,1024)
	cursor := uint64(0)
	for {
		keyBatch,nextCsr,err := c.Scan(ctx,cursor,pattern, int64(batchSize)).Result()
		if err != nil {
			Log_error(err)
			panic(err)
		}
		cursor = nextCsr
		for _,v := range keyBatch {
			keys[v] = true    // 去重
		}
		if cursor == 0 {
			break
		}
	}

	for k,_ := range keys {
		result = append(result, k)
	}

	return result
}

func RedisDealAllWithBatch(c *redis.ClusterClient, keys []string, batchSize int,
	BatchGetOP func(*redis.ClusterClient, []string) map[string]string) map[string]string {

	result := make(map[string]string,0)
	rKeyNum := len(keys)
	if rKeyNum < 1 {
		return result
	}
	if rKeyNum < batchSize {
		result = BatchGetOP(c,keys)
		return result
	}
	for i:=0; i<rKeyNum; i+=batchSize {
		left := i
		right := i+batchSize
		if right >= rKeyNum-1 {
			right = rKeyNum
		}

		keyBatch := keys[left:right]
		valueBatch := BatchGetOP(c,keyBatch)
		for k,v := range valueBatch {
			result[k] = v
		}

		log.Printf("[RedisDealAllWithBatch] %d / %d, result length %d", right,rKeyNum,len(result))
	}
	return result
}

func RedisPipeLineGet(c *redis.ClusterClient, keys []string) map[string]string {
	dataMap := map[string]string{}
	for _,k := range keys {
		dataMap[k] = ""
	}

	return RedisPipelineOP(c,dataMap,GetOP)
}

func RedisPipeLPushWithWindow(c *redis.ClusterClient, dataMap map[string]string, windowSize int) {
	LPushData := make(map[string]string)
	RPopLPushData := make(map[string]string)

	key2EleNum := RedisPipelineOP(c,dataMap,LLenOP)
	for key,eleNum := range key2EleNum {
		num,err := strconv.Atoi(eleNum)
		if err != nil {
			log.Printf("strconv.Atoi redis key:%s, err=%s",key,err)
			continue
		}
		if num < windowSize {
			LPushData[key] = eleNum
		} else {
			RPopLPushData[key] = eleNum
		}
	}

	RedisPipelineOP(c,LPushData,LPushOP)
	RedisPipelineOP(c,RPopLPushData,RPopLPush)
}

func RedisPipeLineLLen(c *redis.ClusterClient, keys []string) map[string]string {
	dataMap := map[string]string{}
	for _,k := range keys {
		dataMap[k] = ""
	}

	return RedisPipelineOP(c,dataMap,LLenOP)
}

func RedisMgetAllWitchBatch(c *redis.ClusterClient, keys []string, batchSize int) map[string]string{
	result := make(map[string]string,1024)
	rKeyNum := len(keys)
	if rKeyNum < 1 {
		return result
	}
	if rKeyNum < batchSize {
		result = RedisMgetString(c,keys)
		return result
	}
	for i:=0; i<rKeyNum; i+=batchSize {
		left := i
		right := i+batchSize
		if right >= rKeyNum-1 {
			right = rKeyNum
		}

		keyBatch := keys[left:right]
		valueBatch := RedisMgetString(c,keyBatch)
		for k,v := range valueBatch {
			result[k] = v
		}
	}
	return result
}

func RedisMgetString(c *redis.ClusterClient,keys []string) map[string]string {
	results := make(map[string]string,0)

	values,err := c.MGet(ctx,keys...).Result()
	if err != nil {
		Log_error(err)
		panic(err)
	}

	for i,v := range values {
		if v == nil{   // Not exist
			continue
		}
		results[keys[i]] = v.(string)
	}

	return results
}

func RedisPipelineOP(c *redis.ClusterClient, dataMap map[string]string, OP func(redis.Pipeliner,string,string)) map[string]string {
	pipe := c.TxPipeline()
	defer pipe.Close()

	for k,v := range dataMap {
		OP(pipe,k,v)
	}
	cmds,err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		log.Fatal("pipe.Exec first error=",err)
	}

	result := make(map[string]string)
	for _,v := range cmds {  // 结果检查
		if v.Err() != nil && v.Err() != redis.Nil {
			log.Printf("[error] ClusterRedis-Pipeline-Exec-Fail on %+v",v)   // 记录执行出错的命令
		}
		cmdStr := strings.Split(v.String()," ")
		if cmdStr[0] == "get" || cmdStr[0] == "llen" {
			if v.Err() == redis.Nil {
				log.Printf("%+v NOT EXIST",v.Args())
				continue
			}
			k := strings.Split(cmdStr[1],":")[0]
			v := cmdStr[2]
			result[k] = v
		}
	}
	return result
}

func LLenOP(pipe redis.Pipeliner, key string, value string) {
	pipe.LLen(ctx,key)
}

func RPopOP(pipe redis.Pipeliner, key string, value string) {
	pipe.RPop(ctx,key)
}

func LPushOP(pipe redis.Pipeliner, key string, value string) {
	pipe.LPush(ctx,key,value)
}

func RPopLPush(pipe redis.Pipeliner, key string, value string) {
	RPopOP(pipe,key,value)
	LPushOP(pipe,key,value)
}

func SetOP(pipe redis.Pipeliner, key string, value string) {
	pipe.Set(ctx,key,value,0)  // 无过期时间
}

func GetOP(pipe redis.Pipeliner, key string, value string) {
	pipe.Get(ctx,key)
}

func RedisTxPipeLineSet(c redis.ClusterClient, dataMap map[string]string)  error {
	const maxRetries = 100
	keys := make([]string,0)
	// Transactional function.
	txf := func(tx *redis.Tx) error {
		// Operation is commited only if the watched keys remain unchanged.
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for k,v:= range dataMap {
				pipe.Set(ctx,k,v,0)
				keys = append(keys,k)
			}
			return nil
		})
		return err
	}

	for i := 0; i < maxRetries; i++ {
		err := c.Watch(ctx, txf, keys...)
		if err == nil {
			// Success.
			return nil
		}
		if err == redis.TxFailedErr {
			// Optimistic lock lost. Retry.
			continue
		}
		// Return any other error.
		return err
	}

	return errors.New("TxPipe-Get reached maximum number of retries")

}







