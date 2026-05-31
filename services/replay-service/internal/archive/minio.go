package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/thorOdinson16/multiplayer-infra/services/replay-service/internal/consumer"
)

// Archiver archives completed replays to MinIO (FR-RP-04)
type Archiver struct {
	client     *minio.Client
	bucketName string
}

// NewArchiver creates a new MinIO archiver
func NewArchiver(endpoint, accessKey, secretKey, bucket string) (*Archiver, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %w", err)
	}

	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to check bucket: %w", err)
	}

	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
		log.Printf("Created bucket: %s", bucket)
	}

	return &Archiver{
		client:     client,
		bucketName: bucket,
	}, nil
}

// ArchiveMatch archives match events to MinIO (FR-RP-04, FR-RP-06)
func (a *Archiver) ArchiveMatch(matchID string, events []*consumer.MatchEvent) error {
	ctx := context.Background()

	data, err := json.Marshal(events)
	if err != nil {
		return fmt.Errorf("failed to marshal events: %w", err)
	}

	objectName := fmt.Sprintf("replays/%s.json", matchID)
	reader := bytes.NewReader(data)

	_, err = a.client.PutObject(ctx, a.bucketName, objectName, reader, int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/json",
	})

	if err != nil {
		return fmt.Errorf("failed to upload replay: %w", err)
	}

	log.Printf("Replay archived: %s (%d events, %d bytes)", matchID, len(events), len(data))
	return nil
}

// GetReplay retrieves a replay from MinIO
func (a *Archiver) GetReplay(ctx context.Context, matchID string) ([]*consumer.MatchEvent, error) {
	objectName := fmt.Sprintf("replays/%s.json", matchID)

	obj, err := a.client.GetObject(ctx, a.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get replay: %w", err)
	}
	defer obj.Close()

	var events []*consumer.MatchEvent
	if err := json.NewDecoder(obj).Decode(&events); err != nil {
		return nil, fmt.Errorf("failed to decode replay: %w", err)
	}

	return events, nil
}