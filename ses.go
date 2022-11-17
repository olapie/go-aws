package awskit

import (
	"code.olapie.com/errors"
	"context"

	"code.olapie.com/conv"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ses"
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
	ses *ses.SES
}

func NewSES(sess *session.Session) *SES {
	return &SES{
		ses: ses.New(sess),
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

	body := new(ses.Body)
	var charset *string
	if email.Charset != "" {
		charset = aws.String(email.Charset)
	} else {
		charset = aws.String("UTF-8")
	}

	if email.HTMLBody != "" {
		body.Html = &ses.Content{
			Charset: charset,
			Data:    aws.String(email.HTMLBody),
		}
	} else {
		body.Text = &ses.Content{
			Charset: charset,
			Data:    aws.String(email.TextBody),
		}
	}

	input := &ses.SendEmailInput{
		Destination: &ses.Destination{
			CcAddresses: conv.MustSlice(email.Cc, aws.String),
			ToAddresses: conv.MustSlice(email.To, aws.String),
		},
		Message: &ses.Message{
			Body: body,
			Subject: &ses.Content{
				Charset: charset,
				Data:    aws.String(email.Subject),
			},
		},
		Source: aws.String(email.From),
	}

	result, err := s.ses.SendEmailWithContext(ctx, input)
	if err != nil {
		return "", err
	}

	return *result.MessageId, nil
}
