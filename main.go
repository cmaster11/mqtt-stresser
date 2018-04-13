package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
	"errors"
)

var (
	resultChan         = make(chan Result)
	abortChan          = make(chan bool)
	shouldTerminate    = false
	randomSource       = rand.New(rand.NewSource(time.Now().UnixNano()))
	stopGeneratingWorkers = false

	subscriberClientIdTemplate = "mqtt-stresser-sub-%s-worker%d-%d"
	publisherClientIdTemplate  = "mqtt-stresser-pub-%s-worker%d-%d"
	pollingPublisherClientIdTemplate  = "mqtt-stresser-pol-pub-%s-worker%d-%d"
	topicNameTemplate          = "internal/mqtt-stresser/%s/worker%d-%d"

	opTimeout = 5 * time.Second
	workerConnectionErrorsCount = 0

	errorLogger   = log.New(os.Stderr, "ERROR: ", log.Lmicroseconds|log.Ltime|log.Lshortfile)
	verboseLogger = log.New(os.Stderr, "DEBUG: ", log.Lmicroseconds|log.Ltime|log.Lshortfile)

	argNumClients    = flag.Int("num-clients", 10, "Number of concurrent clients")
	argNumMessages   = flag.Int("num-messages", 10, "Number of messages shipped by client")
	argTimeout       = flag.String("timeout", "5s", "Timeout for pub/sub loop")
	argGlobalTimeout = flag.String("global-timeout", "60s", "Timeout spanning all operations")
	argRampUpSize    = flag.Int("rampup-size", 100, "Size of rampup batch")
	argRampUpDelay   = flag.String("rampup-delay", "500ms", "Time between batch rampups")
	argBrokerUrl     = flag.String("broker", "", "Broker URL")
	argUsername      = flag.String("username", "", "Username")
	argPassword      = flag.String("password", "", "Password")
	argLogLevel      = flag.Int("log-level", 0, "Log level (0=nothing, 1=errors, 2=debug, 3=error+debug)")
	argProfileCpu    = flag.String("profile-cpu", "", "write cpu profile `file`")
	argProfileMem    = flag.String("profile-mem", "", "write memory profile to `file`")
	argHideProgress  = flag.Bool("no-progress", false, "Hide progress indicator")
	argHelp          = flag.Bool("help", false, "Show help")

	// Custom
	argCumulateConnections   = flag.Bool("cumulate-connections", false, "If used, clients will NOT disconnect until the end of the test")
	argWaitForIntSignal   = flag.Bool("wait-for-int-signal", false, "If used, clients will NOT disconnect until an interrupt signal is received (invalidates global timeout)")
	argPollingDelay   = flag.String("polling-delay", "1s", "If workers are kept alive using -cumulate-connections or -wait-for-int-signal, they'll poll the broker with this delay")
	argPollingOnly          = flag.Bool("polling-only", false, "Disables message pub/sub test and executes pure polling only")
	argStopAfterErrors          = flag.Int("stop-after-errors", 0, "Stops generating workers if n worker fails connecting")
)

type Worker struct {
	WorkerId  int
	BrokerUrl string
	Username  string
	Password  string
	Nmessages int
	Timeout   time.Duration

	WaitTestEnd bool
	TestEndChan chan bool
	PollingDelay time.Duration
	PollingOnly bool
}

type Result struct {
	WorkerId          int
	Event             string
	PublishTime       time.Duration
	ReceiveTime       time.Duration
	MessagesReceived  int
	MessagesPublished int
	Error             bool
	ErrorMessage      error
}

var signalChan chan os.Signal
var tooManyErrorsError error = errors.New("Too many errors")

func main() {
	flag.Parse()

	if flag.NFlag() < 1 || *argHelp {
		flag.Usage()
		os.Exit(1)
	}

	if *argProfileCpu != "" {
		f, err := os.Create(*argProfileCpu)

		if err != nil {
			fmt.Printf("Could not create CPU profile: %s\n", err)
		}

		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Printf("Could not start CPU profile: %s\n", err)
		}
	}

	pollingOnly := *argPollingOnly

	num := *argNumMessages

	if pollingOnly {
		num = 0
	}

	brokerUrl := *argBrokerUrl
	username := *argUsername
	password := *argPassword
	testTimeout, _ := time.ParseDuration(*argTimeout)
	pollingDelay, _ := time.ParseDuration(*argPollingDelay)

	verboseLogger.SetOutput(ioutil.Discard)
	errorLogger.SetOutput(ioutil.Discard)

	if *argLogLevel == 1 || *argLogLevel == 3 {
		errorLogger.SetOutput(os.Stderr)
	}

	if *argLogLevel == 2 || *argLogLevel == 3 {
		verboseLogger.SetOutput(os.Stderr)
	}

	if brokerUrl == "" {
		os.Exit(1)
	}

	signalChan = make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for !shouldTerminate {
			select {
				case signal := <-signalChan:
					shouldTerminate = true

					fmt.Println()
					fmt.Printf("Received %s. Aborting.\n", signal)
			}
		}
	}()

	rampUpDelay, _ := time.ParseDuration(*argRampUpDelay)
	rampUpSize := *argRampUpSize

	if rampUpSize < 0 {
		rampUpSize = 100
	}

	resultChan = make(chan Result, *argNumClients**argNumMessages)

	testEndChan := make(chan bool, *argNumClients)

	cid := 0
	for cid = 0; cid < *argNumClients && !shouldTerminate && (*argStopAfterErrors == 0 || (*argStopAfterErrors > 0 && workerConnectionErrorsCount < *argStopAfterErrors)); cid++ {
		if cid%rampUpSize == 0 && cid > 0 {
			fmt.Printf("%d worker started - waiting %s\n", cid, rampUpDelay)
			time.Sleep(rampUpDelay)
		}

		go (&Worker{
			WorkerId:  cid,
			BrokerUrl: brokerUrl,
			Username:  username,
			Password:  password,
			Nmessages: num,
			Timeout:   testTimeout,
			
			WaitTestEnd: *argCumulateConnections,
			TestEndChan: testEndChan,
			PollingDelay: pollingDelay,
			PollingOnly: pollingOnly,
		}).Run()
	}

	if *argStopAfterErrors > 0 && workerConnectionErrorsCount >= *argStopAfterErrors {
		fmt.Printf("Workers generation stopped (%d client created). Reached %d connection errors\n", cid, workerConnectionErrorsCount)
		for idx := cid; idx < *argNumClients; idx ++ {
			resultChan <- Result{
				WorkerId:     idx,
				Event:        "ConnectFailed",
				Error:        true,
				ErrorMessage: tooManyErrorsError,
			}
		}
	}

	finEvents := 0

	timeout := make(chan bool, 1)
	globalTimeout, _ := time.ParseDuration(*argGlobalTimeout)
	results := make([]Result, *argNumClients)

	if !*argWaitForIntSignal {
		go func() {
			time.Sleep(globalTimeout)
			timeout <- true
		}()
	}

	testEndChanSignalSent := false
	sendTestEndChanSignal := func () {
		if *argCumulateConnections && !testEndChanSignalSent {
			testEndChanSignalSent = true
			// Tell workers it's time to go home
			count := 0
			for count < *argNumClients {
				testEndChan<-true
				count++
			}
		}
	}

	for finEvents < *argNumClients && !shouldTerminate {
		select {
		case msg := <-resultChan:
			results[msg.WorkerId] = msg

			if msg.Event == "Completed" || msg.Error {
				finEvents++
				verboseLogger.Printf("%d/%d events received\n", finEvents, *argNumClients)
			}

			if msg.Error {
				errorLogger.Println(msg)
			}

			if *argHideProgress == false {
				if msg.Event == "Completed" {
					fmt.Print(".")
				}

				if msg.Error {
					fmt.Print("E")
				}
			}

		case <-timeout:
			fmt.Println()
			fmt.Printf("Aborted because global timeout (%s) was reached.\n", *argGlobalTimeout)

			shouldTerminate = true
		}
	}

	if *argWaitForIntSignal && !shouldTerminate {
		fmt.Println()
		fmt.Printf("Waiting for interrupt signal.\n")
		for !shouldTerminate {
			time.Sleep(250 * time.Millisecond)
		}
	}

	sendTestEndChanSignal()

	summary, err := buildSummary(*argNumClients, num, results)
	exitCode := 0

	if err != nil {
		exitCode = 1
	} else {
		printSummary(summary)
	}

	if *argProfileMem != "" {
		f, err := os.Create(*argProfileMem)

		if err != nil {
			fmt.Printf("Could not create memory profile: %s\n", err)
		}

		runtime.GC() // get up-to-date statistics

		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Printf("Could not write memory profile: %s\n", err)
		}
		f.Close()
	}

	pprof.StopCPUProfile()

	os.Exit(exitCode)
}
