package main

import (
	"fmt"
	"sort"
	"sync"
)

var mu = &sync.Mutex{}

type ByBoolThanID []MsgData

func (elems ByBoolThanID) Len() int      { return len(elems) }
func (elems ByBoolThanID) Swap(a, b int) { elems[a], elems[b] = elems[b], elems[a] }
func (elems ByBoolThanID) Less(a, b int) bool {
	if elems[a].HasSpam == elems[b].HasSpam {
		return elems[a].ID < elems[b].ID
	}
	return elems[a].HasSpam
}

func RunPipeline(cmds ...cmd) {
	var wg = &sync.WaitGroup{}
	var channels = make([]chan interface{}, len(cmds)+1)

	for i := 0; i < len(cmds)+1; i++ {
		channels[i] = make(chan interface{})
	}

	for i, command := range cmds {
		wg.Add(1)
		go func(command cmd, wg *sync.WaitGroup, in, out chan interface{}) {
			defer wg.Done()
			command(in, out)
			close(out)
		}(command, wg, channels[i], channels[i+1])
	}
	wg.Wait()

}

func SelectUsers(in, out chan interface{}) {
	var wg = &sync.WaitGroup{}
	var seenIds = &sync.Map{}

	for userEmail := range in {

		emailString, ok := userEmail.(string)
		if !ok {
			fmt.Printf("I don't know about type of %v!\n", userEmail)
		}

		wg.Add(1)
		go func(email string, idsMap *sync.Map, wg *sync.WaitGroup, out chan interface{}) {
			defer wg.Done()
			user := GetUser(email)
			mu.Lock()
			defer mu.Unlock()
			if _, ok := idsMap.Load(user.ID); !ok {
				idsMap.Store(user.ID, true)
				out <- user
			}
		}(emailString, seenIds, wg, out)

	}
	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	var wg = &sync.WaitGroup{}
	var users = make([]User, 0)
	var worker = func(out chan interface{}, wg *sync.WaitGroup, usersBatch ...User) {
		defer wg.Done()
		messagesIds, err := GetMessages(usersBatch...)
		if err != nil {
			fmt.Printf("Some error returned from func GetMessages():%v", err)
			return
		}
		for _, id := range messagesIds {
			out <- id
		}
	}

	for inputFromChan := range in {
		user, ok := inputFromChan.(User)
		if !ok {
			fmt.Printf("I don't know about type of %v\n", inputFromChan)
			break
		}
		mu.Lock()
		users = append(users, user)

		if len(users) == GetMessagesMaxUsersBatch {
			wg.Add(1)
			go worker(out, wg, users...)
			users = make([]User, 0)
		}
		mu.Unlock()
	}

	if len(users) > 0 {
		wg.Add(1)
		go worker(out, wg, users...)
	}
	wg.Wait()

}

func CheckSpam(in, out chan interface{}) {
	var wg = &sync.WaitGroup{}
	var controlChan = make(chan struct{}, HasSpamMaxAsyncRequests)

	for inputFromChan := range in {
		msgID, ok := inputFromChan.(MsgID)
		if !ok {
			fmt.Printf("I don't know about type of %v\n", inputFromChan)
			break
		}
		controlChan <- struct{}{}

		wg.Add(1)
		go func(id MsgID, out chan interface{}, wg *sync.WaitGroup, controlChannel chan struct{}) {
			defer wg.Done()
			ifHasSpam, err := HasSpam(id)
			if err != nil {
				fmt.Printf("Some error returned from func GetMessages():%v", err)
				return
			}
			out <- MsgData{ID: id, HasSpam: ifHasSpam}
			<-controlChannel
		}(msgID, out, wg, controlChan)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var wg = &sync.WaitGroup{}
	var resultSyncMap = &sync.Map{}

	for val := range in {
		msgData, ok := val.(MsgData)
		if !ok {
			fmt.Println("I don't know about type of msgData")
			return
		}
		wg.Add(1)
		go func(data MsgData, resultSyncMap *sync.Map) {
			defer wg.Done()
			resultSyncMap.Store(data.ID, data)
		}(msgData, resultSyncMap)
	}
	wg.Wait()

	var resultSlice []MsgData
	resultSyncMap.Range(func(key, value interface{}) bool {
		msgData, ok := value.(MsgData)
		if ok {
			resultSlice = append(resultSlice, msgData)
		}
		return true
	})

	sort.Sort(ByBoolThanID(resultSlice))

	for _, msgData := range resultSlice {
		out <- fmt.Sprintf("%v %v", msgData.HasSpam, msgData.ID)
	}

}
