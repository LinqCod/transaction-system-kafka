package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	_ "github.com/lib/pq"
	"github.com/linqcod/transaction-system/app/database"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	// kafka
	kafkaBrokerUrl     string
	kafkaVerbose       bool
	kafkaTopic         string
	kafkaConsumerGroup string
	kafkaClientId      string

	// postgres
	host     string
	port     string
	user     string
	password string
	dbname   string
)

var logger = log.With().Str("pkg", "main").Logger()

func main() {
	// kafka config
	flag.StringVar(&kafkaBrokerUrl, "kafka-brokers", "localhost:19092,localhost:29092,localhost:39092", "Kafka brokers in comma separated value")
	flag.BoolVar(&kafkaVerbose, "kafka-verbose", true, "Kafka verbose logging")
	flag.StringVar(&kafkaTopic, "kafka-topic", "transactions-queue", "Kafka topic. Only one topic per worker.")
	flag.StringVar(&kafkaConsumerGroup, "kafka-consumer-group", "consumer-group", "Kafka consumer group")
	flag.StringVar(&kafkaClientId, "kafka-client-id", "my-client-id", "Kafka client id")

	//postgres config
	flag.StringVar(&host, "host", "localhost", "postgres host")
	flag.StringVar(&port, "port", "5432", "postgres port")
	flag.StringVar(&user, "user", "root", "postgres username")
	flag.StringVar(&password, "password", "root", "postgres password")
	flag.StringVar(&dbname, "dbname", "transaction-system-pg", "postgres db name")

	flag.Parse()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	brokers := strings.Split(kafkaBrokerUrl, ",")

	config := kafka.ReaderConfig{
		Brokers:         brokers,
		GroupID:         kafkaClientId,
		Topic:           kafkaTopic,
		MinBytes:        10e3,
		MaxBytes:        10e6,
		MaxWait:         1 * time.Second,
		ReadLagInterval: -1,
	}

	reader := kafka.NewReader(config)
	defer reader.Close()

	db, err := database.NewClient(host, port, user, password, dbname)
	if err != nil {
		logger.Error().Msgf("error while connecting to database: %s", err.Error())
	}
	defer db.Close()

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			logger.Error().Msgf("error while receiving message: %s", err.Error())
			continue
		}

		var transaction database.Transaction
		if err = json.Unmarshal(m.Value, &transaction); err != nil {
			logger.Error().Msgf("error while unmarshalling transaction: %s", err.Error())
			continue
		}

		if err = processTransaction(context.Background(), db, transaction); err != nil {
			logger.Error().Msgf("error while processing transaction: %s", err.Error())
			continue
		}
	}
}

func processTransaction(ctx context.Context, db *sql.DB, transaction database.Transaction) error {
	var account database.Account

	res := db.QueryRowContext(ctx, "SELECT * FROM account WHERE account_id = $1 LIMIT 1", transaction.AccountID)
	if err := res.Scan(&account.AccountID, &account.Balance); err != nil {
		return err
	}

	db.QueryRowContext(ctx, "UPDATE account SET balance = $1 WHERE account_id = $2", account.Balance+transaction.Amount, account.AccountID)
	return nil
}
