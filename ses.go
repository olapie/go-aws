package awskit

import (
	"context"

	"code.olapie.com/errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
)

type Email struct {
	From     string
	To       []string
	Cc       []string
	Subject  string
	TextBody string
	HTMLBody string
	Charset  string
}

type SES struct {
	ses *ses.Client
}

func NewSES(cfg aws.Config) *SES {
	return &SES{
		ses: ses.NewFromConfig(cfg),
	}
}

func (s *SES) Send(ctx context.Context, email *Email) (string, error) {
	if email.From == "" {
		return "", errors.New("missing From")
	}

	if len(email.To) == 0 {
		return "", errors.New("missing To")
	}

	if email.Subject == "" {
		return "", errors.New("missing Subject")
	}

	body := new(types.Body)
	var charset *string
	if email.Charset != "" {
		charset = aws.String(email.Charset)
	} else {
		charset = aws.String("UTF-8")
	}

	if email.HTMLBody != "" {
		body.Html = &types.Content{
			Charset: charset,
			Data:    aws.String(email.HTMLBody),
		}
	} else {
		body.Text = &types.Content{
			Charset: charset,
			Data:    aws.String(email.TextBody),
		}
	}

	input := &ses.SendEmailInput{
		Destination: &types.Destination{
			CcAddresses: email.Cc,
			ToAddresses: email.To,
		},
		Message: &types.Message{
			Body: body,
			Subject: &types.Content{
				Charset: charset,
				Data:    aws.String(email.Subject),
			},
		},
		Source: aws.String(email.From),
	}

	result, err := s.ses.SendEmail(ctx, input)
	if err != nil {
		return "", err
	}

	return *result.MessageId, nil
}
