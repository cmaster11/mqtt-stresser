package main

import (
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"os"
	"time"
	"math/rand"
	"errors"
)

var hostname string

func init() {
	hostname, _ = os.Hostname()

	localAddr := os.Getenv("LOCAL_ADDRESS")
	localInterface := os.Getenv("LOCAL_INTERFACE")

	if len(localAddr) > 0 && len(localInterface) > 0 {
		hostname = fmt.Sprintf("%s-%s", localInterface, localAddr)
	}
}

func waitTimeoutOrError(token mqtt.Token) bool {
	return token.WaitTimeout(opTimeout) == false || token.Error() != nil
}

var timeoutError error = errors.New("Timeout")
func tokenErrorOrTimeout(token mqtt.Token) error {
	tokenError := token.Error()
	if tokenError == nil {
		return timeoutError
	}
	return tokenError
}

func (w *Worker) Run() {
	verboseLogger.Printf("[%d] initializing\n", w.WorkerId)

	queue := make(chan [2]string)
	cid := w.WorkerId
	t := randomSource.Int31()

	topicName := fmt.Sprintf(topicNameTemplate, hostname, w.WorkerId, t)

	if w.PollingOnly && w.PollingDelay > 0 && w.WaitTestEnd {
		w.startPolling(t, topicName)
		return
	}

	subscriberClientId := fmt.Sprintf(subscriberClientIdTemplate, hostname, w.WorkerId, t,)
	publisherClientId := fmt.Sprintf(publisherClientIdTemplate, hostname, w.WorkerId, t)

	verboseLogger.Printf("[%d] topic=%s subscriberClientId=%s publisherClientId=%s\n", cid, topicName, subscriberClientId, publisherClientId)

	publisherOptions := mqtt.NewClientOptions().SetClientID(publisherClientId).SetUsername(w.Username).SetPassword(w.Password).AddBroker(w.BrokerUrl)
	subscriberOptions := mqtt.NewClientOptions().SetClientID(subscriberClientId).SetUsername(w.Username).SetPassword(w.Password).AddBroker(w.BrokerUrl)

	subscriberOptions.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		queue <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	publisher := mqtt.NewClient(publisherOptions)
	subscriber := mqtt.NewClient(subscriberOptions)

	verboseLogger.Printf("[%d] connecting publisher\n", w.WorkerId)
	if token := publisher.Connect(); waitTimeoutOrError(token) {
		workerConnectionErrorsCount++
		err := tokenErrorOrTimeout(token)
		fmt.Printf("Publisher not connected: %s\n", err)

		resultChan <- Result{
			WorkerId:     w.WorkerId,
			Event:        "ConnectFailed",
			Error:        true,
			ErrorMessage: err,
		}
		return
	}

	verboseLogger.Printf("[%d] connecting subscriber\n", w.WorkerId)
	if token := subscriber.Connect(); waitTimeoutOrError(token) {
		workerConnectionErrorsCount++
		err := tokenErrorOrTimeout(token)
		fmt.Printf("Subscriber not connected: %s\n", err)

		resultChan <- Result{
			WorkerId:     w.WorkerId,
			Event:        "ConnectFailed",
			Error:        true,
			ErrorMessage: err,
		}

		return
	}

	defer func() {
		verboseLogger.Printf("[%d] unsubscribe\n", w.WorkerId)

		if token := subscriber.Unsubscribe(topicName); waitTimeoutOrError(token) {
			fmt.Println(tokenErrorOrTimeout(token))
			//os.Exit(1)
			return
		}

		subscriber.Disconnect(5)
	}()

	verboseLogger.Printf("[%d] subscribing to topic\n", w.WorkerId)
	if token := subscriber.Subscribe(topicName, 0, nil); waitTimeoutOrError(token) {
		resultChan <- Result{
			WorkerId:     w.WorkerId,
			Event:        "SubscribeFailed",
			Error:        true,
			ErrorMessage: tokenErrorOrTimeout(token),
		}

		return
	}

	verboseLogger.Printf("[%d] starting control loop %s\n", w.WorkerId, topicName)

	timeout := make(chan bool, 1)
	stopWorker := false
	receivedCount := 0
	publishedCount := 0

	t0 := time.Now()
	for i := 0; i < w.Nmessages; i++ {
		text := fmt.Sprintf("this is msg #%d!", i)
		token := publisher.Publish(topicName, 0, false, text)
		publishedCount++
		token.Wait()
	}
	publisher.Disconnect(5)

	publishTime := time.Since(t0)
	verboseLogger.Printf("[%d] all messages published\n", w.WorkerId)

	go func() {
		time.Sleep(w.Timeout)
		timeout <- true
	}()

	t0 = time.Now()
	for receivedCount < w.Nmessages && !stopWorker && !shouldTerminate {
		select {
		case <-queue:
			receivedCount++

			verboseLogger.Printf("[%d] %d/%d received\n", w.WorkerId, receivedCount, w.Nmessages)
			if receivedCount == w.Nmessages {
				resultChan <- Result{
					WorkerId:          w.WorkerId,
					Event:             "Completed",
					PublishTime:       publishTime,
					ReceiveTime:       time.Since(t0),
					MessagesReceived:  receivedCount,
					MessagesPublished: publishedCount,
				}
			} else {
				resultChan <- Result{
					WorkerId:          w.WorkerId,
					Event:             "ProgressReport",
					PublishTime:       publishTime,
					ReceiveTime:       time.Since(t0),
					MessagesReceived:  receivedCount,
					MessagesPublished: publishedCount,
				}
			}
		case <-timeout:
			verboseLogger.Printf("[%d] timeout!!\n", cid)
			stopWorker = true

			resultChan <- Result{
				WorkerId:          w.WorkerId,
				Event:             "TimeoutExceeded",
				PublishTime:       publishTime,
				MessagesReceived:  receivedCount,
				MessagesPublished: publishedCount,
				Error:             true,
			}
		case <-abortChan:
			verboseLogger.Printf("[%d] received abort signal", w.WorkerId)
			stopWorker = true

			resultChan <- Result{
				WorkerId:          w.WorkerId,
				Event:             "Aborted",
				PublishTime:       publishTime,
				MessagesReceived:  receivedCount,
				MessagesPublished: publishedCount,
				Error:             false,
			}
		}
	}

	verboseLogger.Printf("[%d] worker finished\n", w.WorkerId)

	if !shouldTerminate {
		if w.PollingDelay > 0 && w.WaitTestEnd {
			w.startPolling(t, topicName)
		} else {
			if w.WaitTestEnd {
				<-w.TestEndChan
			}
		}
	}
}

func (w *Worker) startPolling(t int32, topicName string) {
	executePolling := true

	go func() {
		<-w.TestEndChan
		executePolling = false
	}()
	
	// Starts a polling worker, will automatically trigger deferred disconnection of subscriber
	go func() {
		pollingPublisherClientId := fmt.Sprintf(publisherClientIdTemplate, hostname, w.WorkerId, t)
		pollingPublisherOptions := mqtt.NewClientOptions().SetClientID(pollingPublisherClientId).SetUsername(w.Username).SetPassword(w.Password).AddBroker(w.BrokerUrl)
		pollingPublisher := mqtt.NewClient(pollingPublisherOptions)

		verboseLogger.Printf("[%d] connecting polling publisher\n", w.WorkerId)
		if token := pollingPublisher.Connect(); waitTimeoutOrError(token) {
			workerConnectionErrorsCount++
			verboseLogger.Printf("[%d] failed to connect polling publisher\n", w.WorkerId)
			
			err := tokenErrorOrTimeout(token)
			fmt.Printf("Polling not connected: %s\n", err)

			if w.PollingOnly {
				resultChan <- Result{
					WorkerId:     w.WorkerId,
					Event:        "ConnectFailed",
					Error:        true,
					ErrorMessage: err,
				}
			}
			return
		}

		if w.PollingOnly {
			resultChan <- Result{
				WorkerId:          w.WorkerId,
				Event:             "Completed",
			}
		}

		i := 0
		for executePolling && !shouldTerminate {
			text := fmt.Sprintf("this is polling msg #%d!", i)
			token := pollingPublisher.Publish(topicName, 0, false, text)
			token.Wait()
			verboseLogger.Printf("[%d] sent polling message\n", w.WorkerId)

			// Creates a slight deviation from standard delay
			var deviationPercentage int64 = 20 // n%
			maxDeviation := int64(w.PollingDelay) / 100 * deviationPercentage * 2
			randDeviation := rand.Int63n(maxDeviation)
			deviatedDelay := time.Duration(int64(w.PollingDelay) + randDeviation)

			time.Sleep(deviatedDelay)
		}

		pollingPublisher.Disconnect(5)
	}()
}