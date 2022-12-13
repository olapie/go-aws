Don't use this package if SQS events are processed by AWS Lambda.  
Lambda will delete sqs messages based on the returned result.  Don't need application do it explicitly.  