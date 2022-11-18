package awskit_test

import (
	"context"
	"os"
	"testing"
	"time"

	"code.olapie.com/awskit"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func setupS3Bucket(t *testing.T) *awskit.S3Bucket {
	profile := os.Getenv("AWS_TEST_PROFILE")
	if profile == "" {
		profile = "test"
	}
	bucket := os.Getenv("S3_TEST_BUCKET")
	require.NotEmpty(t, bucket)
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithSharedConfigProfile(profile))
	require.NoError(t, err)
	t.Log(cfg.Region)
	cfg.Region = "us-west-1"
	return awskit.NewS3Bucket(cfg, bucket)
}

func TestS3_Put(t *testing.T) {
	bucket := setupS3Bucket(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	id := uuid.NewString()
	content := []byte("content" + uuid.NewString())
	metadata := map[string]string{"test-key": "test value"}
	err := bucket.Put(ctx, id, content, metadata)
	require.NoError(t, err)

	readContent, err := bucket.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, content, readContent)

	readMetadata, err := bucket.GetMetadata(ctx, id)
	require.NoError(t, err)
	require.Equal(t, metadata, readMetadata)

	err = bucket.Delete(ctx, id)
	require.NoError(t, err)

	exists, err := bucket.Exists(ctx, id)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestS3_BatchDelete(t *testing.T) {
	r := setupS3Bucket(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	var ids []string
	for i := 0; i < 3; i++ {
		id := uuid.NewString()
		content := []byte("content" + uuid.NewString())
		metadata := map[string]string{"test-key": "test value"}
		err := r.Put(ctx, id, content, metadata)
		require.NoError(t, err)
		ids = append(ids, id)
	}

	err := r.BatchDelete(ctx, ids)
	require.NoError(t, err)
}
