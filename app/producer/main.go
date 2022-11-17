package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/linqcod/transaction-system/app/database"
	"github.com/linqcod/transaction-system/app/kafka"
	"github.com/rs/zerolog/log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var logger = log.With().Str("pkg", "main").Logger()

var (
	listenAddrApi string
	//kafka
	kafkaBrokerUrl string
	kafkaVerbose   bool
	kafkaClientId  string
	kafkaTopic     string

	// postgres
	host     string
	port     string
	user     string
	password string
	dbname   string
)

var db *sql.DB

func main() {
	flag.StringVar(&listenAddrApi, "listen-address", "0.0.0.0:9000", "Listen address for api")
	flag.StringVar(&kafkaBrokerUrl, "kafka-brokers", "localhost:19092,localhost:29092,localhost:39092", "Kafka brokers in comma separated value")
	flag.BoolVar(&kafkaVerbose, "kafka-verbose", true, "Kafka verbose logging")
	flag.StringVar(&kafkaClientId, "kafka-client-id", "my-kafka-client", "Kafka client id to connect")
	flag.StringVar(&kafkaTopic, "kafka-topic", "transactions-queue", "Kafka topic to push")

	//postgres config
	flag.StringVar(&host, "host", "localhost", "postgres host")
	flag.StringVar(&port, "port", "5432", "postgres port")
	flag.StringVar(&user, "user", "root", "postgres username")
	flag.StringVar(&password, "password", "root", "postgres password")
	flag.StringVar(&dbname, "dbname", "transaction-system-pg", "postgres db name")

	flag.Parse()

	//connect to kafka
	kafkaProducer, err := kafka.Configure(strings.Split(kafkaBrokerUrl, ","), kafkaClientId, kafkaTopic)
	if err != nil {
		logger.Error().Str("error", err.Error()).Msg("unable to configure kafka")
		return
	}
	defer kafkaProducer.Close()

	// connect to db
	db, err = database.NewClient(host, port, user, password, dbname)
	if err != nil {
		logger.Error().Msgf("error while connecting to database: %s", err.Error())
	}
	defer db.Close()

	var errChan = make(chan error, 1)
	go func() {
		logger.Info().Msgf("starting app at %s", listenAddrApi)
		errChan <- server(listenAddrApi)
	}()

	var signalChan = make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	select {
	case <-signalChan:
		logger.Info().Msg("got an interrupt, exiting...")
	case err := <-errChan:
		if err != nil {
			logger.Error().Err(err).Msg("error while running api, exiting...")
		}
	}
}

func server(listenAddr string) error {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.POST("/api/v1/account", ChangeAccountBalance)

	//for debugging purpose
	for _, routeInfo := range router.Routes() {
		logger.Debug().
			Str("path", routeInfo.Path).
			Str("handler", routeInfo.Handler).
			Str("method", routeInfo.Method).
			Msg("registered routes")
	}

	return router.Run(listenAddr)
}

func ChangeAccountBalance(ctx *gin.Context) {
	parent := context.Background()
	defer parent.Done()

	var transaction database.Transaction
	err := json.NewDecoder(ctx.Request.Body).Decode(&transaction)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": map[string]interface{}{
				"message": fmt.Sprintf("error while binding json: %s", err.Error()),
			},
		})

		ctx.Abort()
		return
	}

	isTransactionValid, err := IsTransactionValid(context.Background(), transaction)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": map[string]interface{}{
				"message": fmt.Sprintf("error no account found: %s", err.Error()),
			},
		})

		ctx.Abort()
		return
	}

	if !isTransactionValid {
		ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": map[string]interface{}{
				"message": "invalid transaction",
			},
		})

		ctx.Abort()
		return
	}

	if transaction.Amount < 0 {
		transaction.TransactionType = "WITHDRAW_TRANSACTION"
	} else {
		transaction.TransactionType = "ADDITIONAL_TRANSACTION"
	}

	transactionInBytes, err := json.Marshal(transaction)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": map[string]interface{}{
				"message": fmt.Sprintf("error while marshaling json: %s", err.Error()),
			},
		})

		ctx.Abort()
		return
	}

	err = kafka.Push(parent, []byte(transaction.AccountID), transactionInBytes)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": map[string]interface{}{
				"message": fmt.Sprintf("error while push message into kafka: %s", err.Error()),
			},
		})

		ctx.Abort()
		return
	}

	ctx.JSON(http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "success push data into kafka",
		"data":    transaction,
	})
}

func IsTransactionValid(ctx context.Context, transaction database.Transaction) (bool, error) {
	var account database.Account

	res := db.QueryRowContext(ctx, "SELECT * FROM account WHERE account_id = $1 LIMIT 1", transaction.AccountID)
	if err := res.Scan(&account.AccountID, &account.Balance); err != nil {
		return false, err
	}

	if account.Balance+transaction.Amount < 0 {
		return false, nil
	} else {
		return true, nil
	}
}
