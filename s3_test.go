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

func setupS3(t *testing.T) *awskit.S3Bucket {
	bucket := os.Getenv("S3_TEST_BUCKET")
	require.NotEmpty(t, bucket)
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithSharedConfigProfile("default"))
	require.NoError(t, err)
	return awskit.NewS3Bucket(cfg, bucket)
}

func TestS3_Upload(t *testing.T) {
	r := setupS3(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	id := uuid.NewString()
	content := []byte("content" + uuid.NewString())
	metadata := map[string]string{"test-key": "test value"}
	err := r.Put(ctx, id, content, metadata)
	require.NoError(t, err)

	readContent, err := r.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, content, readContent)

	readMetadata, err := r.GetMetadata(ctx, id)
	require.NoError(t, err)
	require.Equal(t, metadata, readMetadata)

	err = r.Delete(ctx, id)
	require.NoError(t, err)

	exists, err := r.Exists(ctx, id)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestS3_BatchDelete(t *testing.T) {
	r := setupS3(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	var ids []string
	for i := 0; i < 3; i++ {
		id := uuid.NewString()
		content := []byte("content" + uuid.NewString())
		metadata := map[string]string{"Testkey": "test value"}
		err := r.Put(ctx, id, content, metadata)
		require.NoError(t, err)
		ids = append(ids, id)
	}

	err := r.BatchDelete(ctx, ids)
	require.NoError(t, err)
}
