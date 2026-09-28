package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	checks   int
	failures []string
)

func check(name string, got, want interface{}) {
	checks++
	g := fmt.Sprintf("%v", got)
	w := fmt.Sprintf("%v", want)
	if g != w {
		failures = append(failures, fmt.Sprintf("%s: got %s, want %s", name, g, w))
	}
}

func main() {
	port := os.Getenv("NOIDA_KAFKA_PORT")
	if port == "" {
		fmt.Fprintln(os.Stderr, "NOIDA_KAFKA_PORT not set")
		os.Exit(1)
	}
	addr := fmt.Sprintf("127.0.0.1:%s", port)

	// 1. Topic creation
	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	topic := "go-topic"
	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create topic: %v\n", err)
		os.Exit(1)
	}

	partitions, err := conn.ReadPartitions()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read partitions: %v\n", err)
		os.Exit(1)
	}
	var goTopicParts []kafka.Partition
	for _, p := range partitions {
		if p.Topic == topic {
			goTopicParts = append(goTopicParts, p)
		}
	}
	check("topic has 3 partitions", len(goTopicParts), 3)

	// 2. Produce messages
	writer := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = writer.WriteMessages(ctx,
		kafka.Message{Key: []byte("k1"), Value: []byte("v1")},
		kafka.Message{Key: []byte("k2"), Value: []byte("v2")},
		kafka.Message{Key: []byte("k3"), Value: []byte("v3")},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to write messages: %v\n", err)
		os.Exit(1)
	}

	// 3. Consume via kafka.Reader with GroupID
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{addr},
		GroupID:        "go-group",
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})

	var received []string
	var msgsToCommit []kafka.Message
	readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readCancel()

	for len(received) < 3 {
		m, err := reader.FetchMessage(readCtx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error fetching message: %v\n", err)
			break
		}
		received = append(received, string(m.Value))
		msgsToCommit = append(msgsToCommit, m)
	}

	sort.Strings(received)
	check("received 3 messages", fmt.Sprintf("%v", received), "[v1 v2 v3]")

	err = reader.CommitMessages(readCtx, msgsToCommit...)
	if err != nil {
		failures = append(failures, fmt.Sprintf("commit messages failed: %v", err))
	}
	reader.Close()

	// Wait briefly for consumer 1 leave group to register
	time.Sleep(300 * time.Millisecond)

	// 4. Second consumer in same group resumes past committed offsets
	err = writer.WriteMessages(ctx,
		kafka.Message{Key: []byte("k4"), Value: []byte("v4")},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to write resume message: %v\n", err)
		os.Exit(1)
	}

	reader2 := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{addr},
		GroupID:        "go-group",
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})

	readCtx2, readCancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer readCancel2()

	m2, err := reader2.FetchMessage(readCtx2)
	if err != nil {
		failures = append(failures, fmt.Sprintf("consumer2 fetch error: %v", err))
	} else {
		check("consumer2 resumes with v4", string(m2.Value), "v4")
		reader2.CommitMessages(readCtx2, m2)
	}
	reader2.Close()

	fmt.Printf("kafka-go: %d checks, %d failed\n", checks, len(failures))
	for _, f := range failures {
		fmt.Printf("  FAIL: %s\n", f)
	}
	if len(failures) > 0 {
		os.Exit(1)
	}
}
