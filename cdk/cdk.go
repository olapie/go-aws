package cdk

import (
	"fmt"
	"strings"

	"code.olapie.com/sugar/naming"
	"code.olapie.com/sugar/rtx"
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
	Account string
	Region  string
	Service string
	Stage   string
}

func (e *Env) GetFullName(baseName string) string {
	return strings.Join([]string{e.Stage, e.Service, baseName}, "-")
}

func (e *Env) GetResourceName(typ, baseName string) string {
	return naming.ToClassName(e.GetFullName(baseName)) + typ
}

type DomainConfig struct {
	CertificateArn string
	HostedZoneName string
}

type FunctionProps = awslambda.FunctionProps

func NewARecord(scope constructs.Construct, hostedZone string, certificateArn, subDomain string) (ARecord, DomainName) {
	domainName := newDomainName(scope, hostedZone, certificateArn, subDomain)
	zoneCDKName := naming.ToClassName(hostedZone) + naming.ToClassName(subDomain) + "Zone"
	zone := awsroute53.HostedZone_FromLookup(scope, rtx.Addr(zoneCDKName), &awsroute53.HostedZoneProviderProps{
		DomainName: rtx.Addr(hostedZone),
	})

	domainProperties := awsroute53targets.NewApiGatewayv2DomainProperties(
		domainName.RegionalDomainName(),
		domainName.RegionalHostedZoneId())

	record := awsroute53.NewARecord(scope, rtx.Addr(naming.ToClassName(subDomain)+"ARecord"), &awsroute53.ARecordProps{
		Zone:           zone,
		Comment:        nil,
		DeleteExisting: rtx.Addr(true),
		RecordName:     rtx.Addr(subDomain + "." + hostedZone),
		Ttl:            nil,
		Target:         awsroute53.RecordTarget_FromAlias(domainProperties),
	})
	return record, domainName
}

func NewFunction(scope constructs.Construct, env *Env, name string, props *FunctionProps) awslambda.Function {
	funcName := env.GetFullName(name)
	handlerName := env.Service + "-" + name
	cdkName := naming.ToClassName(funcName) + "LambdaFunction"
	if props == nil {
		props = &FunctionProps{}
	}

	if props.FunctionName == nil {
		props.FunctionName = rtx.Addr(funcName)
	}

	if props.LogRetention == "" {
		props.LogRetention = awslogs.RetentionDays_ONE_WEEK
	}

	if props.MemorySize == nil {
		props.MemorySize = rtx.Addr(400.0)
	}

	if props.Timeout == nil {
		props.Timeout = awscdk.Duration_Seconds(rtx.Addr(30.0))
	}

	if props.Handler == nil {
		props.Handler = rtx.Addr(handlerName)
	}

	if props.Role == nil {
		props.Role = newFunctionRole(scope, env, funcName)
	}

	props.Runtime = awslambda.Runtime_GO_1_X()
	return awslambda.NewFunction(scope, rtx.Addr(cdkName), props)
}

type HttpApiEndpoint struct {
	FunctionName string
	Function     Function

	Path    string
	Methods []HttpMethod

	Default bool
}

func NewHttpApi(scope constructs.Construct, name string, domainName DomainName, endpoints []HttpApiEndpoint) HttpApi {
	cdkName := naming.ToClassName(name)
	var routes []*apigatewayv2alpha.AddRoutesOptions
	funcToIntegration := make(map[Function]HttpLambdaIntegration)
	var defaultIntegration HttpLambdaIntegration
	for _, e := range endpoints {
		integration := funcToIntegration[e.Function]
		if integration == nil {
			integration = apigatewayv2integrationsalpha.NewHttpLambdaIntegration(rtx.Addr(
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
			Path:        rtx.Addr(e.Path),
			Methods:     rtx.Addr(e.Methods),
		})
	}

	httpApi := apigatewayv2alpha.NewHttpApi(scope, rtx.Addr(cdkName+"HttpApi"), &apigatewayv2alpha.HttpApiProps{
		ApiName: rtx.Addr(name),
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
	cdkName := naming.ToClassName(name)
	if props == nil {
		props = new(QueueProps)
	}
	if props.MaxMessageSizeBytes == nil {
		props.MaxMessageSizeBytes = rtx.Addr(float64(64 * 1024))
	}
	if props.QueueName == nil {
		props.QueueName = rtx.Addr(name)
	}
	if props.RetentionPeriod == nil {
		props.RetentionPeriod = awscdk.Duration_Hours(rtx.Addr(2.0))
	}

	if props.VisibilityTimeout == nil {
		props.VisibilityTimeout = awscdk.Duration_Seconds(rtx.Addr(30.0))
	}

	dlq := awssqs.NewQueue(scope, rtx.Addr(cdkName+"DeadLetterQueue"), &QueueProps{
		MaxMessageSizeBytes:    props.MaxMessageSizeBytes,
		QueueName:              rtx.Addr(*props.QueueName + "-dlq"),
		ReceiveMessageWaitTime: nil,
		RetentionPeriod:        awscdk.Duration_Days(rtx.Addr(3.0)),
		VisibilityTimeout:      props.VisibilityTimeout,
	})
	props.DeadLetterQueue = &awssqs.DeadLetterQueue{
		MaxReceiveCount: rtx.Addr(3.0),
		Queue:           dlq,
	}
	return awssqs.NewQueue(scope, rtx.Addr(cdkName+"Queue"), props)
}

func NewBucket(scope constructs.Construct, name string) Bucket {
	cdkName := naming.ToClassName(name) + "Bucket"
	return awss3.NewBucket(scope, rtx.Addr(cdkName), &awss3.BucketProps{
		AutoDeleteObjects: rtx.Addr(false),
		BucketName:        rtx.Addr(name),
		Versioned:         rtx.Addr(true),
		RemovalPolicy:     awscdk.RemovalPolicy_RETAIN,
	})
}

func newFunctionRole(scope constructs.Construct, env *Env, funcFullName string) awsiam.Role {
	cdkName := naming.ToClassName(funcFullName) + "Role"
	role := awsiam.NewRole(scope, rtx.Addr(cdkName), &awsiam.RoleProps{
		RoleName:  rtx.Addr(cdkName),
		AssumedBy: awsiam.NewServicePrincipal(rtx.Addr(IAMServiceLambda), nil),
	})
	role.AddToPolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Actions: rtx.Addr([]*string{rtx.Addr(IAMActionCreateLogStream), rtx.Addr(IAMActionPutLogEvents)}),
		Effect:  awsiam.Effect_ALLOW,
		Resources: rtx.Addr([]*string{rtx.Addr(fmt.Sprintf("arn:aws:logs:%s:%s:log-group:/aws/lambda/%s:*",
			env.Region, env.Account, funcFullName))}),
	}))

	role.AddToPolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Actions:   rtx.Addr([]*string{rtx.Addr(IAMActionCreateLogGroup)}),
		Effect:    awsiam.Effect_ALLOW,
		Resources: rtx.Addr([]*string{rtx.Addr(fmt.Sprintf("arn:aws:logs:%s:%s:*", env.Region, env.Account))}),
	}))
	return role
}

func newDomainName(scope constructs.Construct, hostedZone, certificateArn, subDomain string) DomainName {
	cdkName := naming.ToClassName(subDomain)
	certificate := awscertificatemanager.Certificate_FromCertificateArn(scope,
		rtx.Addr(cdkName+"Certificate"),
		rtx.Addr(certificateArn),
	)

	return apigatewayv2alpha.NewDomainName(scope, rtx.Addr(cdkName+"DomainName"), &DomainNameProps{
		Certificate: certificate,
		DomainName:  rtx.Addr(subDomain + "." + hostedZone),
	})
}
