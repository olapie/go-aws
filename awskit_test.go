package awskit_test

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func loadConfig(t *testing.T) aws.Config {
	profile := os.Getenv("AWS_TEST_PROFILE")
	if profile == "" {
		profile = "test"
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithSharedConfigProfile(profile))
	require.NoError(t, err)
	if region := os.Getenv("AWS_TEST_REGION"); region != "" {
		cfg.Region = region
	}
	return cfg
}
