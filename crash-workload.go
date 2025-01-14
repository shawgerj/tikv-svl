package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/rawkv"
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

func doInitialTransaction() (*transaction.KVTxn, [100000]kvPair) {
	client, err := tikv.NewClient([]string{clientAddr})
	if err != nil {
		log.Fatalf("Unable to initialize TiKV client on %s", clientAddr)
	}

	defer client.Close()

	//initial data
	var initial_kv_items [100000]kvPair
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

func initKeys(client *rawkv.Client) [100000]kvPair {
	var newItems [100000]kvPair
	for j, _ := range newItems {
		newItems[j] = kvPair{
			key:   defaultKey + strconv.Itoa(100000+(j+1)),
			value: defaultValue + strconv.Itoa(100000+(j+1)),
		}
	}

	log.Println("STARTING TO WRITE 100000 KV PAIRS")
	var writtenItems [100000]kvPair
	for i, item := range newItems {
		err := client.Put(context.TODO(), []byte(item.key), []byte(item.value))
		if err != nil {
			log.Fatalf("UNEXPECTED ERROR WRITING PUT")
		}
		log.Println("WROTE ITEM " + item.key + ": " + item.value)
		writtenItems[i] = item
	}
	log.Println("WRITTEN 100000 INITIAL KEYS")

	log.Println("STARTING TO READ INITIAL WRITTEN PAIRS")
	for _, item := range writtenItems {
		v, err := client.Get(context.TODO(), []byte(item.key))
		if tikverr.IsErrNotFound(err) {
			log.Fatalf("UNABLE TO READ SUCCESSFULLY WRITTEN KEY")
		}
		if err != nil {
			log.Println(err)
			log.Fatalf("OTHER ERROR DURING READ")
		}
		if string(v) != item.value {
			log.Fatalf("VALUE READ DOES NOT MATCH WRITTEN VALUE %s %s", string(v), item.value)
		} else {
			log.Println("READ ITEM " + item.key + ": " + string(v))
		}
	}
	return writtenItems
}

func readInitialKeys(client *rawkv.Client, items [100000]kvPair) {
	log.Println("STARTING TO READ INITIAL WRITTEN PAIRS")
	for _, item := range items {
		log.Println("TRYING TO READ ITEM " + item.key + ": " + item.value)
		v, err := client.Get(context.TODO(), []byte(item.key))
		if tikverr.IsErrNotFound(err) {
			log.Fatalf("UNABLE TO READ SUCCESSFULLY WRITTEN KEY")
		}
		if errors.Is(err, tikverr.ErrRegionUnavailable) {
			log.Println(err)
			log.Println("TIKV CRASHED")
			return
		}
		if err != nil {
			log.Println(err)
			log.Fatalf("OTHER ERROR DURING READ")
		}
		if string(v) != item.value {
			log.Fatalf("VALUE READ DOES NOT MATCH WRITTEN VALUE %s %s", string(v), item.value)
		} else {
			log.Println("READ ITEM " + item.key + ": " + string(v))
		}
	}
}

func readBackTransaction(client *tikv.Client, items [100000]kvPair) {
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
	var newItems [100000]kvPair
	for j, _ := range newItems {
		newItems[j] = kvPair{
			key:   defaultKey + strconv.Itoa(100000+(i+1)),
			value: defaultValue + strconv.Itoa(100000+(i+1)),
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

func failingRawKV(client *rawkv.Client, i int) {
	var newItems [100000]kvPair
	for j, _ := range newItems {
		newItems[j] = kvPair{
			key:   defaultKey + strconv.Itoa(100000*(i+2)+(j+1)),
			value: defaultValue + strconv.Itoa(100000*(i+2)+(j+1)),
		}
	}

	log.Println("STARTING TO WRITE 100000 KV PAIRS")
	var writtenItems [100000]kvPair
	var lastWritten = 0
	for j, item := range newItems {
		if !isTiKVServerRunning() {
			log.Println("TiKV has crashed")
			break
		}
		err := client.Put(context.TODO(), []byte(item.key), []byte(item.value))
		if err != nil {
			lastWritten = j
			log.Println("POTENTIALLY WROTE ITEM " + item.key + ": " + item.value)
		} else {
			log.Println("WROTE ITEM " + item.key + ": " + item.value)
			writtenItems[j] = item
		}
	}
	log.Println("WRITTEN SOME KEYS")

	if !isTiKVServerRunning() {
		log.Println("RELAUNCHING SERVERS")
		launchServers := exec.Command("./launch-server.sh")
		launchServers.Stdout = os.Stdout
		err := launchServers.Run()
		if err != nil {
			log.Println(err.Error())
			log.Fatalf("Unable to launch servers")

		}
	}

	log.Println("STARTING TO READ WRITTEN PAIRS")
	for j, item := range newItems {
		if !isTiKVServerRunning() {
			log.Println("TiKV has crashed")
			break
		}
		v, err := client.Get(context.TODO(), []byte(item.key))
		if j < lastWritten {
			if tikverr.IsErrNotFound(err) {
				log.Fatalf("UNABLE TO READ SUCCESSFULLY WRITTEN KEY")
			}
			if errors.Is(err, tikverr.ErrRegionUnavailable) {
				log.Println(err)
				log.Println("TIKV CRASHED")
			}
			if err != nil {
				log.Println(err)
				log.Fatalf("OTHER ERROR DURING READ")
			}
			if string(v) != item.value {
				log.Fatalf("VALUE READ DOES NOT MATCH WRITTEN VALUE %s %s", string(v), item.value)
			} else {
				log.Println("READ ITEM " + item.key + ": " + string(v))
			}
		} else {
			if errors.Is(err, tikverr.ErrRegionUnavailable) {
				log.Println(err)
				log.Println("TIKV CRASHED")
				return
			}
			if err != nil {
				log.Println(err)
				log.Fatalf("OTHER ERROR DURING READ")
			}
			if string(v) != item.value || len(v) == 0 {
				continue
			} else {
				log.Println("READ ITEM " + item.key + ": " + string(v))
			}
		}
	}
}

func main() {
	path := flag.String("cfg", "fail.cfg", "a string")
	flag.Parse()
	failpoints := readFailConfig(*path)

	launchPD := exec.Command("./launch-pd.sh")
	launchPD.Stdout = os.Stdout
	err := launchPD.Run()
	if err != nil {
		log.Println(err.Error())
		log.Fatalf("Unable to launch pd")

	}

	launchServers := exec.Command("./launch-server.sh")
	launchServers.Stdout = os.Stdout
	err = launchServers.Run()
	if err != nil {
		log.Println(err.Error())
		log.Fatalf("Unable to launch servers")

	}

	log.Println("LAUNCHED")
	client, err := rawkv.NewClientWithOpts(context.TODO(), []string{clientAddr})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	initialItems := initKeys(client)
	killServer := exec.Command(kill, serverProcess)
	err = killServer.Run()
	if err != nil {
		log.Fatalf("Unable to kill tikv server")
	}

	log.Println("INITIAL KEYS WRITTEN")
	for i, failpoint := range failpoints {
		log.Println("EVALUATING FAILPOINT: " + failpoint)
		envString := failpoint + "=panic"
		err = os.Setenv(failPoints, envString)
		if err != nil {
			log.Fatalf("Unable to set failpoint")
		}

		launchServers = exec.Command("./launch-server.sh")
		launchServers.Stdout = os.Stdout
		err = launchServers.Run()
		if err != nil {
			log.Println(err.Error())
			log.Fatalf("Unable to launch servers")

		}

		log.Println("READING INITIAL KEYS")
		if isTiKVServerRunning() {
			readInitialKeys(client, initialItems)
		}

		log.Println("WRITING NEW KEYS")
		if isTiKVServerRunning() {
			failingRawKV(client, i)
		} else {
			launchServers = exec.Command("./launch-server.sh")
			launchServers.Stdout = os.Stdout
			err = launchServers.Run()
			if err != nil {
				log.Println(err.Error())
				log.Fatalf("Unable to launch servers")

			}
			failingRawKV(client, i)
		}

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
