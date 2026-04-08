package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "cse535lab1/paxos"

	"google.golang.org/grpc"
)

var servers []*Server

type Transaction struct {
	Sender   string
	Receiver string
	Amount   int
}

type QueuedTransaction struct {
	Transaction Transaction
	Attempts    int
}

type Server struct {
	pb.UnimplementedPaxosServiceServer
	ID               int
	Balance          map[string]int
	LocalLog         []Transaction
	GlobalLog        []Transaction
	mu               sync.RWMutex
	clients          []pb.PaxosServiceClient
	lastBallot       int
	acceptedLog      []Transaction
	transactionQueue []QueuedTransaction
	liveServers      map[string]bool

	transactionCount    int
	totalLatency        time.Duration
	startTime           time.Time
	totalTransactions   int
	totalProcessingTime float64

	performanceCheckTicker *time.Ticker
	performanceCheckDone   chan bool
}

func NewServer(id int, serverAddresses []string) *Server {
	balance := map[string]int{
		"S1": 100, "S2": 100, "S3": 100, "S4": 100, "S5": 100,
	}
	server := &Server{
		ID:               id,
		Balance:          balance,
		LocalLog:         []Transaction{},
		GlobalLog:        []Transaction{},
		lastBallot:       0,
		liveServers:      make(map[string]bool),
		transactionQueue: []QueuedTransaction{},
		acceptedLog:      []Transaction{},
	}

	for i, address := range serverAddresses {
		if i != id-1 {
			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Connection to server %d failed: %v", i+1, err)
			}
			server.clients = append(server.clients, pb.NewPaxosServiceClient(conn))
		}
	}

	return server
}

func (s *Server) ProcessTransaction(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	startTime := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	sender, receiver, amount := req.Sender, req.Receiver, int(req.Amount)

	log.Printf("Server %d: Handling transaction: %s to %s, value: %d", s.ID, sender, receiver, amount)

	if _, exists := s.Balance[sender]; !exists {
		return &pb.TransactionResponse{Success: false, Message: fmt.Sprintf("Sender %s not in system", sender)}, nil
	}
	if _, exists := s.Balance[receiver]; !exists {
		return &pb.TransactionResponse{Success: false, Message: fmt.Sprintf("Receiver %s not in system", receiver)}, nil
	}

	if s.ID == getServerIDFromSender(sender) {
		if s.Balance[sender] >= amount {
			s.Balance[sender] -= amount
			s.Balance[receiver] += amount

			newTx := Transaction{Sender: sender, Receiver: receiver, Amount: amount}
			s.LocalLog = append(s.LocalLog, newTx)
			log.Printf("Server %d: New transaction added to local log: %v", s.ID, newTx)

			endTime := time.Now()
			processingTime := endTime.Sub(startTime).Seconds()
			s.totalTransactions++
			s.totalProcessingTime += processingTime

			return &pb.TransactionResponse{Success: true, Message: "Transaction completed successfully."}, nil
		} else {
			if s.countActivePeers() > len(s.clients)/2 {
				success, err := s.executePaxos()

				if err != nil {
					log.Printf("Server %d: Paxos execution error: %v", s.ID, err)
					return &pb.TransactionResponse{Success: false, Message: fmt.Sprintf("Paxos error occurred: %v", err)}, nil
				}
				if success {
					log.Printf("Server %d: Paxos consensus achieved!", s.ID)

					if s.Balance[sender] >= amount {
						s.updateBalances()
						s.Balance[sender] -= amount
						s.Balance[receiver] += amount

						newTx := Transaction{Sender: sender, Receiver: receiver, Amount: amount}
						s.LocalLog = append(s.LocalLog, newTx)
						log.Printf("Server %d: Transaction added to local log after Paxos: %v", s.ID, newTx)

						endTime := time.Now()
						latency := endTime.Sub(startTime)
						s.totalLatency += latency
						s.transactionCount++

						return &pb.TransactionResponse{Success: true, Message: "Transaction processed after Paxos consensus."}, nil
					} else {
						s.transactionQueue = append(s.transactionQueue, QueuedTransaction{
							Transaction: Transaction{Sender: sender, Receiver: receiver, Amount: amount},
							Attempts:    0,
						})
						return &pb.TransactionResponse{Success: false, Message: "Insufficient funds even after Paxos."}, nil
					}
				}
			} else {
				s.transactionQueue = append(s.transactionQueue, QueuedTransaction{
					Transaction: Transaction{Sender: sender, Receiver: receiver, Amount: amount},
					Attempts:    0,
				})
				log.Printf("Current transaction queue: %v", s.transactionQueue)
				return &pb.TransactionResponse{Success: false, Message: "No majority, Paxos failed, transaction queued"}, nil
			}
		}
	}

	endTime := time.Now()
	processingTime := endTime.Sub(startTime).Seconds()
	s.totalTransactions++
	s.totalProcessingTime += processingTime

	return &pb.TransactionResponse{Success: true, Message: "Transaction recorded on receiving server."}, nil
}

func (s *Server) countActivePeers() int {
	activeCount := 0
	for _, isActive := range s.liveServers {
		if isActive {
			activeCount++
		}
	}
	return activeCount
}

func (s *Server) executePaxos() (bool, error) {
	ballot := s.generateNewBallot()

	promises := s.broadcastPrepare(ballot)
	if len(promises) < len(s.clients)/2+1 {
		return false, fmt.Errorf("majority promises not received")
	}

	consolidatedLog := s.LocalLog
	for _, promise := range promises {
		consolidatedLog = append(consolidatedLog, convertPbTransactionsToLocal(promise.Log)...)
	}

	accepted := s.broadcastAccept(ballot, consolidatedLog)
	if len(accepted) < len(s.clients)/2+1 {
		return false, fmt.Errorf("majority accepts not received")
	}

	s.applyLog(consolidatedLog)
	s.broadcastCommit(ballot, consolidatedLog)

	go s.handleQueuedTransactions()

	return true, nil
}

func (s *Server) generateNewBallot() int {
	s.lastBallot++
	return s.lastBallot
}

func (s *Server) UpdateLiveServers(ctx context.Context, req *pb.UpdateLiveServersRequest) (*pb.UpdateLiveServersResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.liveServers = make(map[string]bool)
	for _, server := range req.LiveServers {
		s.liveServers[server] = true
	}

	go s.handleQueuedTransactions()

	return &pb.UpdateLiveServersResponse{}, nil
}

func (s *Server) handleQueuedTransactions() {
	s.mu.Lock()
	defer s.mu.Unlock()

	var updatedQueue []QueuedTransaction
	for _, queuedTx := range s.transactionQueue {
		sender, receiver := queuedTx.Transaction.Sender, queuedTx.Transaction.Receiver

		if s.liveServers[sender] && s.liveServers[receiver] {
			log.Printf("Server %d: Attempting to process queued tx: %s to %s, amount: %d", s.ID, sender, receiver, queuedTx.Transaction.Amount)
			success, err := s.executePaxos()
			if err != nil || !success {
				log.Printf("Server %d: Paxos failed for queued tx: %v. Queue unchanged.", s.ID, queuedTx.Transaction)
				queuedTx.Attempts++
				updatedQueue = append(updatedQueue, queuedTx)
			} else {
				if s.Balance[sender] >= queuedTx.Transaction.Amount {
					s.Balance[sender] -= queuedTx.Transaction.Amount
					s.Balance[receiver] += queuedTx.Transaction.Amount
					s.LocalLog = append(s.LocalLog, queuedTx.Transaction)

					log.Printf("Server %d: Queued tx processed: %s to %s, amount: %d", s.ID, sender, receiver, queuedTx.Transaction.Amount)
				} else {
					log.Printf("Server %d: Insufficient balance after Paxos for queued tx: %v. Queue unchanged.", s.ID, queuedTx.Transaction)
					updatedQueue = append(updatedQueue, queuedTx)
				}
			}
		} else {
			updatedQueue = append(updatedQueue, queuedTx)
		}
	}

	s.transactionQueue = updatedQueue
}

func (s *Server) broadcastPrepare(ballot int) []*pb.PromiseResponse {
	req := &pb.PrepareRequest{BallotNumber: int32(ballot)}
	responses := make(chan *pb.PromiseResponse, len(s.liveServers))

	var wg sync.WaitGroup
	for _, client := range s.clients {
		wg.Add(1)
		go func(c pb.PaxosServiceClient) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := c.Prepare(ctx, req)
			if err == nil {
				responses <- resp
			} else {
				log.Printf("Server %d: Prepare request failed: %v", s.ID, err)
			}
		}(client)
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	var promises []*pb.PromiseResponse
	for resp := range responses {
		promises = append(promises, resp)
	}

	return promises
}

func (s *Server) broadcastAccept(ballot int, log []Transaction) []*pb.AcceptedResponse {
	req := &pb.AcceptRequest{
		BallotNumber: int32(ballot),
		Log:          convertLocalTransactionsToPb(log),
	}
	responses := make(chan *pb.AcceptedResponse, len(s.clients))

	var wg sync.WaitGroup
	for _, client := range s.clients {
		wg.Add(1)
		go func(c pb.PaxosServiceClient) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := c.Accept(ctx, req)
			if err == nil {
				responses <- resp
			} else {
				// Handle error
			}
		}(client)
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	var accepted []*pb.AcceptedResponse
	for resp := range responses {
		accepted = append(accepted, resp)
	}

	return accepted
}

func (s *Server) broadcastCommit(ballot int, log []Transaction) {
	req := &pb.CommitRequest{
		BallotNumber: int32(ballot),
		Log:          convertLocalTransactionsToPb(log),
	}

	var wg sync.WaitGroup
	for _, client := range s.clients {
		wg.Add(1)
		go func(c pb.PaxosServiceClient) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := c.Commit(ctx, req)
			if err != nil {
				// Handle error
			}
		}(client)
	}
	wg.Wait()
}

func (s *Server) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PromiseResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.BallotNumber > int32(s.lastBallot) {
		s.lastBallot = int(req.BallotNumber)
		return &pb.PromiseResponse{
			Accepted: true,
			Log:      convertLocalTransactionsToPb(s.LocalLog),
		}, nil
	}

	return &pb.PromiseResponse{Accepted: false}, nil
}

func (s *Server) Accept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptedResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.BallotNumber >= int32(s.lastBallot) {
		s.lastBallot = int(req.BallotNumber)
		s.acceptedLog = convertPbTransactionsToLocal(req.Log)
		return &pb.AcceptedResponse{Accepted: true, ServerId: int32(s.ID)}, nil
	}

	return &pb.AcceptedResponse{Accepted: false, ServerId: int32(s.ID)}, nil
}

func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	s.applyLog(convertPbTransactionsToLocal(req.Log))
	return &pb.CommitResponse{}, nil
}

func (s *Server) applyLog(transactions []Transaction) {
	s.GlobalLog = transactions
	s.updateBalances()

	log.Printf("Server %d: Applied log from leader. Updated balances: %v", s.ID, s.Balance)
}

func (s *Server) updateBalances() {
	startingBalance := 100
	tempBalances := make(map[string]int)

	for i := 1; i <= 5; i++ {
		serverID := fmt.Sprintf("S%d", i)
		tempBalances[serverID] = startingBalance
	}

	for _, tx := range s.GlobalLog {
		tempBalances[tx.Sender] -= tx.Amount

		if s.liveServers[tx.Receiver] {
			tempBalances[tx.Receiver] += tx.Amount
		}
	}

	for serverID := range tempBalances {
		if _, isActive := s.liveServers[serverID]; !isActive {
			if lastKnownBalance, exists := s.Balance[serverID]; exists {
				tempBalances[serverID] = lastKnownBalance
			}
		}
	}

	s.Balance = tempBalances
}

func (s *Server) PrintBalance(ctx context.Context, req *pb.PrintBalanceRequest) (*pb.PrintBalanceResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	balance, found := s.Balance[req.ClientId]
	if !found {
		return nil, fmt.Errorf("client %s not found in system", req.ClientId)
	}
	return &pb.PrintBalanceResponse{Balance: int32(balance)}, nil
}

func getServerIDFromSender(sender string) int {
	return int(sender[1] - '0')
}

func convertLocalTransactionsToPb(transactions []Transaction) []*pb.Transaction {
	pbTxs := make([]*pb.Transaction, len(transactions))
	for i, tx := range transactions {
		pbTxs[i] = &pb.Transaction{
			Sender:   tx.Sender,
			Receiver: tx.Receiver,
			Amount:   int32(tx.Amount),
		}
	}
	return pbTxs
}

func (s *Server) Performance(ctx context.Context, req *pb.PerformanceRequest) (*pb.PerformanceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.totalTransactions == 0 || s.totalProcessingTime == 0 {
		return &pb.PerformanceResponse{
			Throughput: 0,
			Latency:    0,
		}, nil
	}

	throughput := float64(s.totalTransactions) / s.totalProcessingTime

	avgLatency := s.totalProcessingTime / float64(s.totalTransactions)

	return &pb.PerformanceResponse{
		Throughput: throughput,
		Latency:    avgLatency,
	}, nil
}

func convertPbTransactionsToLocal(pbTransactions []*pb.Transaction) []Transaction {
	transactions := make([]Transaction, len(pbTransactions))
	for i, pbTx := range pbTransactions {
		transactions[i] = Transaction{
			Sender:   pbTx.Sender,
			Receiver: pbTx.Receiver,
			Amount:   int(pbTx.Amount),
		}
	}
	return transactions
}

func displayTransactionQueues(servers []*Server) {
	for _, server := range servers {
		log.Printf("Server %d Transaction Queue Status: %v", server.ID, server.transactionQueue)
	}
}

func main() {
	serverAddresses := []string{
		"localhost:50051",
		"localhost:50052",
		"localhost:50053",
		"localhost:50054",
		"localhost:50055",
	}

	servers = make([]*Server, 5)
	for i := 0; i < 5; i++ {
		servers[i] = NewServer(i+1, serverAddresses)
	}

	var wg sync.WaitGroup
	for i, server := range servers {
		wg.Add(1)
		go func(s *Server, port int) {
			defer wg.Done()
			listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
			if err != nil {
				log.Fatalf("Failed to start listener on port %d: %v", port, err)
			}
			grpcServer := grpc.NewServer()
			pb.RegisterPaxosServiceServer(grpcServer, s)
			fmt.Printf("Server %d is now listening on port %d...\n", s.ID, port)
			if err := grpcServer.Serve(listener); err != nil {
				log.Fatalf("Failed to serve on port %d: %v", port, err)
			}
		}(server, 50051+i)
	}
	wg.Wait()
}

// Uncomment and modify this function if you want to periodically print performance metrics
/*
func monitorServerPerformance(s *Server) {
	s.performanceCheckTicker = time.NewTicker(10 * time.Second)
	s.performanceCheckDone = make(chan bool)

	go func() {
		for {
			select {
			case <-s.performanceCheckTicker.C:
				ctx := context.Background()
				perf, err := s.Performance(ctx, &pb.PerformanceRequest{})
				if err != nil {
					log.Printf("Error retrieving performance data: %v", err)
				} else {
					log.Printf("Server %d Metrics - Throughput: %.2f tx/s, Avg Latency: %.6f s/tx",
						s.ID, perf.Throughput, perf.Latency)
				}
			case <-s.performanceCheckDone:
				return
			}
		}
	}()
}
*/
