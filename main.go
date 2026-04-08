package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	pb "paxos_distributed_banking_system/paxos"

	"google.golang.org/grpc"
)

type ServerClient struct {
	ID     string
	client pb.PaxosServiceClient
}

type TransactionBatch struct {
	BatchID       int
	Operations    [][]string
	ActiveServers []string
}

func loadCSVTransactions(file string) ([]TransactionBatch, error) {
	fp, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	csvReader := csv.NewReader(fp)
	csvReader.FieldsPerRecord = -1

	var batches []TransactionBatch
	var currentBatch TransactionBatch

	for {
		line, err := csvReader.Read()
		if err != nil {
			break
		}

		if line[0] != "" { // New batch
			if currentBatch.Operations != nil {
				batches = append(batches, currentBatch)
			}
			batchID, _ := strconv.Atoi(line[0])
			currentBatch = TransactionBatch{BatchID: batchID}
		}

		if len(line) >= 2 && strings.HasPrefix(line[1], "(") {
			operation := strings.Trim(line[1], "()")
			transactionFields := strings.Split(operation, ",")
			for i := range transactionFields {
				transactionFields[i] = strings.TrimSpace(transactionFields[i])
			}
			currentBatch.Operations = append(currentBatch.Operations, transactionFields)
		}

		if len(line) >= 3 && strings.HasPrefix(line[2], "[") {
			activeServerStr := strings.Trim(line[2], "[]")
			currentBatch.ActiveServers = strings.Split(activeServerStr, ",")
			for i := range currentBatch.ActiveServers {
				currentBatch.ActiveServers[i] = strings.TrimSpace(currentBatch.ActiveServers[i])
			}
		}
	}

	if currentBatch.Operations != nil {
		batches = append(batches, currentBatch)
	}

	return batches, nil
}

func InitializeClient(id string, port int) *ServerClient {
	connection, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Connection error %d: %v", port, err)
	}

	return &ServerClient{ID: id, client: pb.NewPaxosServiceClient(connection)}
}

func (s *ServerClient) PerformTransaction(sender, receiver string, value int) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	request := &pb.TransactionRequest{
		Sender:   sender,
		Receiver: receiver,
		Amount:   int32(value),
	}

	response, err := s.client.ProcessTransaction(ctx, request)
	if err != nil {
		return false, fmt.Errorf("Client %s: Transaction Failure: %v", s.ID, err)
	}

	if response.Success {
		fmt.Printf("Client %s: Transaction from %s to %s, amount: %d. Message: %s\n", s.ID, sender, receiver, value, response.Message)
		return true, nil
	}

	fmt.Printf("Client %s: Transaction from %s to %s, amount: %d failed. Message: %s\n", s.ID, sender, receiver, value, response.Message)
	return false, nil
}

func (s *ServerClient) RetrieveBalance(clientID string) (int32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	request := &pb.PrintBalanceRequest{ClientId: clientID}
	result, err := s.client.PrintBalance(ctx, request)
	if err != nil {
		return 0, err
	}
	return result.Balance, nil
}

func handleTransactionBatch(clients []*ServerClient, batch TransactionBatch) {
	for i, op := range batch.Operations {
		sender := op[0]
		receiver := op[1]
		value, _ := strconv.Atoi(op[2])

		clientIndex := int(sender[1] - '1')
		if clientIndex < 0 || clientIndex >= len(clients) {
			log.Printf("Invalid sender: %s", sender)
			continue
		}

		fmt.Printf("Executing operation %d: %s to %s, value: %d\n", i+1, sender, receiver, value)
		success, err := clients[clientIndex].PerformTransaction(sender, receiver, value)
		if err != nil {
			log.Printf("Error while processing operation: %v", err)
		} else if !success {
			log.Printf("Operation failed: %s to %s, value: %d", sender, receiver, value)
		}

		displayBalances(clients)
		time.Sleep(200 * time.Millisecond)
	}

	for _, client := range clients {
		err := client.ReportPerformance()
		if err != nil {
			log.Printf("Client performance error %s: %v", client.ID, err)
		}
	}
}

func displayBalances(clients []*ServerClient) {
	fmt.Println("Current Balances:")
	for _, client := range clients {
		balance, err := client.RetrieveBalance(client.ID)
		if err != nil {
			log.Printf("Unable to retrieve balance %s: %v", client.ID, err)
		} else {
			fmt.Printf("%s: %d\n", client.ID, balance)
		}
	}
	fmt.Println()
}

func refreshActiveServers(clients []*ServerClient, activeServers []string) {
	for _, client := range clients {
		_, err := client.client.UpdateLiveServers(context.Background(), &pb.UpdateLiveServersRequest{LiveServers: activeServers})
		if err != nil {
			log.Printf("Error updating active servers for client %s: %v", client.ID, err)
		}
	}
}

func (s *ServerClient) ReportPerformance() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	request := &pb.PerformanceRequest{}
	result, err := s.client.Performance(ctx, request)
	if err != nil {
		return fmt.Errorf("client %s: error retrieving performance metrics: %v", s.ID, err)
	}

	fmt.Printf("Client %s Performance Data:\n", s.ID)
	fmt.Printf("  Transactions per second: %.2f\n", result.Throughput)
	fmt.Printf("  Latency: %.2f seconds per transaction\n", result.Latency)

	return nil
}

func main() {
	clients := make([]*ServerClient, 5)
	for i := 0; i < 5; i++ {
		clients[i] = InitializeClient(fmt.Sprintf("S%d", i+1), 50051+i)
	}

	transactionBatches, err := loadCSVTransactions("sample_transactions.csv")
	if err != nil {
		log.Fatalf("Error loading transaction CSV: %v", err)
	}

	for _, batch := range transactionBatches {
		println()
		fmt.Printf("Processing Batch %d: Active servers: %v\n", batch.BatchID, batch.ActiveServers)
		fmt.Println("Press Enter to continue...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')

		refreshActiveServers(clients, batch.ActiveServers)

		handleTransactionBatch(clients, batch)
	}
}
