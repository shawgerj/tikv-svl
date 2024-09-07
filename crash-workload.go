package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"github.com/pingcap/tidb/config"
	tikvRaw "github.com/pingcap/tidb/store/tikv"
	tikverr "github.com/tikv/client-go/v2/error"
	tikv "github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const (
	clientAddr    = "127.0.0.1:2379"
	defaultKey    = "test-key-"
	defaultValue  = "value-"
	kill          = "pkill"
	serverProcess = "tikv-server"
	failPoints    = "FAILPOINTS"
	pdProcess     = "pd-server"
)

type kvPair struct {
	key   string
	value string
}

func isTiKVServerRunning() bool {
	cmd := exec.Command("ps", "-eo", "comm")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	processes := strings.Split(string(output), "\n")
	for _, process := range processes {
		if strings.TrimSpace(process) == serverProcess {
			return true
		}
	}
	return false
}

func readFailConfig(path string) []string {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("%s", err.Error())
	}
	defer file.Close()
	reader := bufio.NewScanner(file)

	var failpoints []string
	//todo regex checking
	for reader.Scan() {
		failpoints = append(failpoints, reader.Text())
	}
	return failpoints
}

func doInitialTransaction() (*transaction.KVTxn, [1000]kvPair) {
	client, err := tikv.NewClient([]string{clientAddr})
	if err != nil {
		log.Fatalf("Unable to initialize TiKV client on %s", clientAddr)
	}

	defer client.Close()

	//initial data
	var initial_kv_items [1000]kvPair
	for i, _ := range initial_kv_items {
		initial_kv_items[i] = kvPair{
			key:   defaultKey + strconv.Itoa(i),
			value: defaultValue + strconv.Itoa(i),
		}
	}

	t, err := client.Begin()
	if err != nil {
		log.Fatalf("Unable to start TiKV write transaction on %s", clientAddr)
	}

	for _, item := range initial_kv_items {
		err = t.Set([]byte(item.key), []byte(item.value))
		if err != nil {
			log.Fatalf("Failed to write key-value %s:%s", item.key, item.value)
		}
	}

	err = t.Commit(context.TODO())
	log.Println("COMMIT DONE")
	if err != nil {
		log.Fatalf("Failed to commit initial transaction")
	}

	readBackTransaction(client, initial_kv_items)
	//shutdown tikv server running with no failures
	return t, initial_kv_items
}

func readBackTransaction(client *tikv.Client, items [1000]kvPair) {

	t, err := client.Begin()
	if err != nil {
		log.Fatalf("Unable to start read TiKV transaction on %s", clientAddr)
	}

	for _, item := range items {
		v, err := t.Get(context.TODO(), []byte(item.key))
		if tikverr.IsErrNotFound(err) {
			log.Println(err)
			log.Fatalf("FAILURE TO READ COMMITTED DATA")
		}
		if errors.Is(err, tikverr.ErrRegionUnavailable) {
			log.Println(err)
			log.Println("TIKV CRASHED")
			return
		} else if string(v) != item.value {
			log.Fatalf("FAILURE TO READ COMMITTED DATA CORRECTLY")
		}
	}
}

func failingTransaction(client *tikv.Client, i int) {
	var newItems [1000]kvPair
	for j, _ := range newItems {
		newItems[j] = kvPair{
			key:   defaultKey + strconv.Itoa(1000*(i+1)),
			value: defaultValue + strconv.Itoa(1000*(i+1)),
		}
	}

	nextTransaction, err := client.Begin()
	if err != nil {
		log.Fatalf("Unable to start TiKV write transaction on %s", clientAddr)
	}

	var potentialFailure bool
	for _, item := range newItems {
		err = nextTransaction.Set([]byte(item.key), []byte(item.value))
		if err != nil {
			potentialFailure = true
			break
		}
	}

	err = nextTransaction.Commit(context.TODO())
	if err != nil {
		potentialFailure = true
	}
	if !isTiKVServerRunning() {
		return
	}

	if potentialFailure == false {
		readBackTransaction(client, newItems)
		log.Println("WROTE AGAIN")
	} else {
		t, err := client.Begin()
		if err != nil {
			log.Fatalf("Unable to start read TiKV transaction on %s", clientAddr)
		}

		for _, item := range newItems {
			v, err := t.Get(context.TODO(), []byte(item.key))
			if tikverr.IsErrNotFound(err) {
				continue
			}
			if err != nil {
				continue
			} else if string(v) == item.value {
				log.Fatalf("ABLE TO READ DATA FROM UNCOMITTED TRANSACTION")
			}
		}
	}
}

func failingRawKV(i int) {
	client, err := tikvRaw.NewRawKVClient([]string{"192.168.199.113:2379"}, config.Security{})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	var newItems [1000]kvPair
	for j, _ := range newItems {
		newItems[j] = kvPair{
			key:   defaultKey + strconv.Itoa(1000*(i+1)),
			value: defaultValue + strconv.Itoa(1000*(i+1)),
		}
	}

	var writtenItems [1000]kvPair
	var lastWritten int
	for i, item := range newItems {
		err = client.Put([]byte(item.key), []byte(item.value))
		if err != nil {
			lastWritten = i
			break
		}
		writtenItems[i] = item
	}

	if !isTiKVServerRunning() {
		launchServers := exec.Command("./launch-server.sh")
		err = launchServers.Start()
		if err != nil {
			log.Println(err.Error())
			log.Fatalf("Unable to launch servers")

		}
	}

	for i, item := range writtenItems {
		v, err := client.Get([]byte(item.key))
		if i < lastWritten {
			if tikverr.IsErrNotFound(err) {
				log.Fatalf("UNABLE TO READ SUCCESSFULLY WRITTEN KEY")
			}
			if string(v) != item.value {
				log.Fatalf("VALUE READ DOES NOT MATCH WRITTEN VALUE")
			}
			if err != nil {
				log.Fatalf("OTHER ERROR DURING READ")
			}
		} else {
			if tikverr.IsErrNotFound(err) {
				continue
			}
			if err != nil {
				continue
			} else if string(v) == item.value {
				log.Fatalf("ABLE TO READ DATA FROM FAILED PUT")
			}
		}
	}
}

func main() {
	path := flag.String("cfg", "fail.cfg", "a string")
	flag.Parse()
	failpoints := readFailConfig(*path)

	launchPD := exec.Command("./launch-pd.sh")
	err := launchPD.Start()
	if err != nil {
		log.Println(err.Error())
		log.Fatalf("Unable to launch pd")

	}

	launchServers := exec.Command("./launch-server.sh")
	err = launchServers.Start()
	if err != nil {
		log.Println(err.Error())
		log.Fatalf("Unable to launch servers")

	}

	log.Println("LAUNCHED")
	_, initialItems := doInitialTransaction()
	killServer := exec.Command(kill, serverProcess)
	err = killServer.Run()
	if err != nil {
		log.Fatalf("Unable to kill tikv server")
	}

	log.Println("INITIAL TRANSACTION COMPLETED. RESTARTING SERVER")
	for i, failpoint := range failpoints {
		log.Println("EVALUATING FAILPOINT: " + failpoint)
		envString := failpoint + "=panic"
		err = os.Setenv(failPoints, envString)
		if err != nil {
			log.Fatalf("Unable to set failpoint")
		}

		launchServers = exec.Command("./launch-server.sh")
		err = launchServers.Start()
		if err != nil {
			log.Println(err.Error())
			log.Fatalf("Unable to launch servers")

		}

		client, err := tikv.NewClient([]string{clientAddr})
		if err != nil {
			log.Fatalf("Unable to initialize TiKV client on %s", clientAddr)
		}

		if isTiKVServerRunning() {
			readBackTransaction(client, initialItems)
		}

		if isTiKVServerRunning() {
			failingRawKV(i)
		} else {
			launchServers = exec.Command("./launch-server.sh")
			err = launchServers.Start()
			if err != nil {
				log.Println(err.Error())
				log.Fatalf("Unable to launch servers")

			}
			failingRawKV(i)
		}

		client.Close()

		if isTiKVServerRunning() {
			killServer = exec.Command(kill, serverProcess)
			err = killServer.Run()
			if err != nil {
				log.Fatalf("Unable to kill tikv server")
			}
		}
	}
	log.Println("Crash Consistent for all configured fail points!")
	killServer = exec.Command(kill, pdProcess)
	err = killServer.Run()
	if err != nil {
		log.Fatalf("Unable to kill pd server")
	}
}
