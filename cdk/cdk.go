package cdk

import (
	"fmt"
	"strings"

	"code.olapie.com/sugar/v2/xname"
	"code.olapie.com/sugar/v2/xruntime"
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslogs"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53targets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awssqs"
	apigatewayv2alpha "github.com/aws/aws-cdk-go/awscdkapigatewayv2alpha/v2"
	apigatewayv2integrationsalpha "github.com/aws/aws-cdk-go/awscdkapigatewayv2integrationsalpha/v2"
	"github.com/aws/constructs-go/constructs/v10"
)

const (
	USEast1 string = "us-east-1"
	USWest1 string = "us-west-1"
	EUWest1 string = "eu-west-1"
)

const (
	IAMServiceLambda         = "lambda.amazonaws.com"
	IAMActionCreateLogStream = "logs:CreateLogStream"
	IAMActionPutLogEvents    = "logs:PutLogEvents"
	IAMActionCreateLogGroup  = "logs:CreateLogGroup"
	IAMActionSNSPublish      = "sns:Publish"
)

type ARecord = awsroute53.ARecord
type DomainName = apigatewayv2alpha.DomainName
type DomainNameProps = apigatewayv2alpha.DomainNameProps
type HttpApi = apigatewayv2alpha.HttpApi
type HttpMethod = apigatewayv2alpha.HttpMethod
type QueueProps = awssqs.QueueProps
type Queue = awssqs.Queue
type Bucket = awss3.Bucket
type Function = awslambda.Function
type HttpLambdaIntegration = apigatewayv2integrationsalpha.HttpLambdaIntegration

type Env struct {
	Account    string
	Region     string
	Service    string
	Stage      string
	HostedZone string
}

func (e *Env) BucketARN(name string) string {
	return fmt.Sprintf("arn:aws:s3:::%s", name)
}

func (e *Env) SNSTopicARN(topic *string) string {
	if topic == nil {
		return fmt.Sprintf("arn:aws:sns:*:%s:*", e.Account)
	}
	return fmt.Sprintf("arn:aws:sns:*:%s:%s", e.Account, *topic)
}

func (e *Env) CertificateARN(certificateID string) string {
	return fmt.Sprintf("arn:aws:acm:%s:%s:certificate/%s", e.Region, e.Account, certificateID)
}

func (e *Env) DynamodbTableARN(tableName string) string {
	return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", e.Region, e.Account, tableName)
}

func (e *Env) GetFullName(baseName string) string {
	return strings.Join([]string{e.Stage, e.Service, baseName}, "-")
}

func (e *Env) GetResourceName(typ, baseName string) string {
	return xname.ToClassName(e.GetFullName(baseName)) + typ
}

type DomainConfig struct {
	CertificateArn string
	HostedZoneName string
}

type FunctionProps = awslambda.FunctionProps

func NewARecord(scope constructs.Construct, hostedZone string, certificateArn, subDomain string) (ARecord, DomainName) {
	domainName := newDomainName(scope, hostedZone, certificateArn, subDomain)
	zoneCDKName := xname.ToClassName(hostedZone) + xname.ToClassName(subDomain) + "Zone"
	zone := awsroute53.HostedZone_FromLookup(scope, xruntime.Addr(zoneCDKName), &awsroute53.HostedZoneProviderProps{
		DomainName: xruntime.Addr(hostedZone),
	})

	domainProperties := awsroute53targets.NewApiGatewayv2DomainProperties(
		domainName.RegionalDomainName(),
		domainName.RegionalHostedZoneId())

	record := awsroute53.NewARecord(scope, xruntime.Addr(xname.ToClassName(subDomain)+"ARecord"), &awsroute53.ARecordProps{
		Zone:           zone,
		Comment:        nil,
		DeleteExisting: xruntime.Addr(true),
		RecordName:     xruntime.Addr(subDomain + "." + hostedZone),
		Ttl:            nil,
		Target:         awsroute53.RecordTarget_FromAlias(domainProperties),
	})
	return record, domainName
}

func NewFunction(scope constructs.Construct, env *Env, name string, props *FunctionProps) awslambda.Function {
	funcName := env.GetFullName(name)
	handlerName := env.Service + "-" + name
	cdkName := xname.ToClassName(funcName) + "LambdaFunction"
	if props == nil {
		props = &FunctionProps{}
	}

	if props.FunctionName == nil {
		props.FunctionName = xruntime.Addr(funcName)
	}

	if props.LogRetention == "" {
		props.LogRetention = awslogs.RetentionDays_ONE_WEEK
	}

	if props.MemorySize == nil {
		props.MemorySize = xruntime.Addr(400.0)
	}

	if props.Timeout == nil {
		props.Timeout = awscdk.Duration_Seconds(xruntime.Addr(30.0))
	}

	if props.Handler == nil {
		props.Handler = xruntime.Addr(handlerName)
	}

	if props.Role == nil {
		props.Role = newFunctionRole(scope, env, funcName)
	}

	props.Runtime = awslambda.Runtime_GO_1_X()
	return awslambda.NewFunction(scope, xruntime.Addr(cdkName), props)
}

type HttpApiEndpoint struct {
	FunctionName string `json:"function_name"`
	Function     Function

	Path    string       `json:"path"`
	Methods []HttpMethod `json:"methods"`

	Default bool `json:"default"`
}

func NewHttpApi(scope constructs.Construct, name string, domainName DomainName, endpoints []HttpApiEndpoint) HttpApi {
	cdkName := xname.ToClassName(name)
	var routes []*apigatewayv2alpha.AddRoutesOptions
	funcToIntegration := make(map[Function]HttpLambdaIntegration)
	var defaultIntegration HttpLambdaIntegration
	for _, e := range endpoints {
		integration := funcToIntegration[e.Function]
		if integration == nil {
			integration = apigatewayv2integrationsalpha.NewHttpLambdaIntegration(xruntime.Addr(
				e.FunctionName+cdkName+"HttpLambdaIntegration"),
				e.Function,
				&apigatewayv2integrationsalpha.HttpLambdaIntegrationProps{})
			funcToIntegration[e.Function] = integration
		}

		if e.Default {
			defaultIntegration = integration
			continue
		}
		routes = append(routes, &apigatewayv2alpha.AddRoutesOptions{
			Integration: integration,
			Path:        xruntime.Addr(e.Path),
			Methods:     xruntime.Addr(e.Methods),
		})
	}

	httpApi := apigatewayv2alpha.NewHttpApi(scope, xruntime.Addr(cdkName+"HttpApi"), &apigatewayv2alpha.HttpApiProps{
		ApiName: xruntime.Addr(name),
		DefaultDomainMapping: &apigatewayv2alpha.DomainMappingOptions{
			DomainName: domainName,
		},
		DefaultIntegration: defaultIntegration,
	})

	for _, route := range routes {
		httpApi.AddRoutes(route)
	}
	return httpApi
}

func NewQueue(scope constructs.Construct, env *Env, name string, props *QueueProps) Queue {
	name = env.GetFullName(name)
	cdkName := xname.ToClassName(name)
	if props == nil {
		props = new(QueueProps)
	}
	if props.MaxMessageSizeBytes == nil {
		props.MaxMessageSizeBytes = xruntime.Addr(float64(64 * 1024))
	}
	if props.QueueName == nil {
		props.QueueName = xruntime.Addr(name)
	}
	if props.RetentionPeriod == nil {
		props.RetentionPeriod = awscdk.Duration_Hours(xruntime.Addr(2.0))
	}

	if props.VisibilityTimeout == nil {
		props.VisibilityTimeout = awscdk.Duration_Seconds(xruntime.Addr(30.0))
	}

	dlq := awssqs.NewQueue(scope, xruntime.Addr(cdkName+"DeadLetterQueue"), &QueueProps{
		MaxMessageSizeBytes:    props.MaxMessageSizeBytes,
		QueueName:              xruntime.Addr(*props.QueueName + "-dlq"),
		ReceiveMessageWaitTime: nil,
		RetentionPeriod:        awscdk.Duration_Days(xruntime.Addr(3.0)),
		VisibilityTimeout:      props.VisibilityTimeout,
	})
	props.DeadLetterQueue = &awssqs.DeadLetterQueue{
		MaxReceiveCount: xruntime.Addr(3.0),
		Queue:           dlq,
	}
	return awssqs.NewQueue(scope, xruntime.Addr(cdkName+"Queue"), props)
}

func NewBucket(scope constructs.Construct, name string) Bucket {
	cdkName := xname.ToClassName(name) + "Bucket"
	return awss3.NewBucket(scope, xruntime.Addr(cdkName), &awss3.BucketProps{
		AutoDeleteObjects: xruntime.Addr(false),
		BucketName:        xruntime.Addr(name),
		Versioned:         xruntime.Addr(true),
		RemovalPolicy:     awscdk.RemovalPolicy_RETAIN,
	})
}

func newFunctionRole(scope constructs.Construct, env *Env, funcFullName string) awsiam.Role {
	cdkName := xname.ToClassName(funcFullName) + "Role"
	role := awsiam.NewRole(scope, xruntime.Addr(cdkName), &awsiam.RoleProps{
		RoleName:  xruntime.Addr(cdkName),
		AssumedBy: awsiam.NewServicePrincipal(xruntime.Addr(IAMServiceLambda), nil),
	})
	role.AddToPolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Actions: xruntime.Addr([]*string{xruntime.Addr(IAMActionCreateLogStream), xruntime.Addr(IAMActionPutLogEvents)}),
		Effect:  awsiam.Effect_ALLOW,
		Resources: xruntime.Addr([]*string{xruntime.Addr(fmt.Sprintf("arn:aws:logs:%s:%s:log-group:/aws/lambda/%s:*",
			env.Region, env.Account, funcFullName))}),
	}))

	role.AddToPolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Actions:   xruntime.Addr([]*string{xruntime.Addr(IAMActionCreateLogGroup)}),
		Effect:    awsiam.Effect_ALLOW,
		Resources: xruntime.Addr([]*string{xruntime.Addr(fmt.Sprintf("arn:aws:logs:%s:%s:*", env.Region, env.Account))}),
	}))
	return role
}

func newDomainName(scope constructs.Construct, hostedZone, certificateArn, subDomain string) DomainName {
	cdkName := xname.ToClassName(subDomain)
	certificate := awscertificatemanager.Certificate_FromCertificateArn(scope,
		xruntime.Addr(cdkName+"Certificate"),
		xruntime.Addr(certificateArn),
	)

	return apigatewayv2alpha.NewDomainName(scope, xruntime.Addr(cdkName+"DomainName"), &DomainNameProps{
		Certificate: certificate,
		DomainName:  xruntime.Addr(subDomain + "." + hostedZone),
	})
}
