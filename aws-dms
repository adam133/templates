import { Construct } from 'constructs';
import * as TaskSettings from './config/dms-replication-settings.json';
import * as cdk from 'aws-cdk-lib';
import * as dms from 'aws-cdk-lib/aws-dms';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sm from 'aws-cdk-lib/aws-secretsmanager';

type StackProps = cdk.StackProps;

export class DmsStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props: StackProps) {
        super(scope, id, props);

        const datalakebucketname = cdk.Fn.importValue('DataLakeBucketName');
        const datalakebucket = s3.Bucket.fromBucketName(this, 'DatalakeBucket', datalakebucketname);
        // import subnet group ids from vpc stack
        const subnet1 = cdk.Fn.importValue('PrivateWithEgressSubnetId1');
        const subnet2 = cdk.Fn.importValue('PrivateWithEgressSubnetId2');
        const subnet3 = cdk.Fn.importValue('PrivateWithEgressSubnetId3');
        const vpcId = cdk.Fn.importValue('VpcId');
        const vpc = ec2.Vpc.fromVpcAttributes(this, 'Vpc', {
            availabilityZones: ['us-east-1c'],
            vpcId: vpcId,
        });

        const dmsInitialSecret = new cdk.SecretValue(
            '{"username": "dms_user","password": "dms_user_password","port": "3306","host": "db.dev.com"}'
        );
        // create kms key for dms
        const dmsKmsKey = new kms.Key(this, 'DmsKmsKey', {
            alias: 'dms-kms-key',
            description: 'KMS key for DMS',
            enableKeyRotation: true,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
        });

        const dmsRole = new iam.Role(this, 'DmsRole', {
            assumedBy: new iam.ServicePrincipal('dms.us-east-1.amazonaws.com'),
            description: 'Role for DMS to access secrets manager',
            roleName: 'dms-role',
        });

        // cloudwatch dms role
        const dmsCloudwatchRole = new iam.Role(this, 'DmsCloudwatchRole', {
            assumedBy: new iam.ServicePrincipal('dms.amazonaws.com'),
            description: 'Role for DMS to access cloudwatch',
            roleName: 'dms-cloudwatch-logs-role',
        });

        dmsCloudwatchRole.addManagedPolicy(
            iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonDMSCloudWatchLogsRole')
        );

        dmsKmsKey.addToResourcePolicy(
            new iam.PolicyStatement({
                actions: ['kms:*'],
                conditions: {
                    StringEquals: {
                        'kms:ViaService': 'dms.us-east-1.amazonaws.com',
                    },
                },
                effect: iam.Effect.ALLOW,
                principals: [new iam.AccountRootPrincipal()],
                resources: ['*'],
            })
        );
        dmsKmsKey.addToResourcePolicy(
            new iam.PolicyStatement({
                actions: ['kms:*'],
                conditions: {
                    StringEquals: {
                        'kms:ViaService': 'secretsmanager.amazonaws.com',
                    },
                },
                effect: iam.Effect.ALLOW,
                principals: [new iam.AccountRootPrincipal()],
                resources: ['*'],
            })
        );
        dmsKmsKey.addToResourcePolicy(
            new iam.PolicyStatement({
                actions: ['kms:*'],
                effect: iam.Effect.ALLOW,
                principals: [new iam.ArnPrincipal(dmsRole.roleArn)],
                resources: ['*'],
            })
        );
        // create secret for dms to connect to mysql
        const dmsSecret = new sm.Secret(this, 'DmsSecret', {
            description: 'Secret for DMS to connect to mysql',
            encryptionKey: dmsKmsKey,
            secretName: 'dms-secret',
            secretStringValue: dmsInitialSecret,
        });

        // create dms vpc role for vpc access to create subnet groups
        const dmsVpcRole = new iam.Role(this, 'DmsVpcRole', {
            assumedBy: new iam.ServicePrincipal('dms.us-east-1.amazonaws.com'),
            description: 'Role for DMS to access vpc',
            roleName: 'dms-vpc-role',
        });

        // add required policy to dms vpc role
        dmsVpcRole.addToPolicy(
            new iam.PolicyStatement({
                actions: [
                    'ec2:CreateNetworkInterface',
                    'ec2:DescribeAvailabilityZones',
                    'ec2:DescribeInternetGateways',
                    'ec2:DescribeSecurityGroups',
                    'ec2:DescribeSubnets',
                    'ec2:DescribeVpcs',
                    'ec2:DeleteNetworkInterface',
                    'ec2:ModifyNetworkInterfaceAttribute',
                ],
                effect: iam.Effect.ALLOW,
                resources: ['*'],
            })
        );

        // create dms role for accessing secrets manager
        dmsRole.addToPolicy(
            new iam.PolicyStatement({
                actions: ['secretsmanager:GetSecretValue'],
                effect: iam.Effect.ALLOW,
                resources: [dmsSecret.secretArn],
            })
        );
        dmsRole.addToPolicy(
            new iam.PolicyStatement({
                actions: ['kms:*'],
                effect: iam.Effect.ALLOW,
                resources: ['*'],
            })
        );
        dmsRole.addToPolicy(
            new iam.PolicyStatement({
                actions: ['iam:PassRole'],
                effect: iam.Effect.ALLOW,
                resources: [dmsRole.roleArn],
            })
        );

        // add policies to dms role to allow validation
        dmsRole.addToPolicy(
            new iam.PolicyStatement({
                actions: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:CreateWorkGroup'],
                effect: iam.Effect.ALLOW,
                resources: [
                    `arn:aws:athena:us-east-1:${cdk.Aws.ACCOUNT_ID}:workgroup/dms_validation_workgroup_for_task_*`,
                ],
            })
        );
        dmsRole.addToPolicy(
            new iam.PolicyStatement({
                actions: [
                    'glue:CreateDatabase',
                    'glue:DeleteDatabase',
                    'glue:GetDatabase',
                    'glue:GetTables',
                    'glue:CreateTable',
                    'glue:DeleteTable',
                    'glue:GetTable',
                ],
                effect: iam.Effect.ALLOW,
                resources: [
                    `arn:aws:glue:us-east-1:${cdk.Aws.ACCOUNT_ID}:catalog`,
                    `arn:aws:glue:us-east-1:${cdk.Aws.ACCOUNT_ID}:database/aws_dms_s3_validation_*`,
                    `arn:aws:glue:us-east-1:${cdk.Aws.ACCOUNT_ID}:table/aws_dms_s3_validation_*/*`,
                    `arn:aws:glue:us-east-1:${cdk.Aws.ACCOUNT_ID}:userDefinedFunction/aws_dms_s3_validation_*/*`,
                ],
            })
        );

        // allow dmsRole to access datalake bucket
        dmsRole.addToPolicy(
            new iam.PolicyStatement({
                actions: ['s3:*'],
                effect: iam.Effect.ALLOW,
                resources: [datalakebucket.bucketArn, datalakebucket.bucketArn + '/*'],
            })
        );

        // functions to create a mapping rule for a list of tables
        type TableMapping = {
            'schema-name': string;
            tables: string[];
        };

        const getRule = (schemaName: string, table: string, ruleNumber: string) => {
            return {
                'object-locator': {
                    'schema-name': schemaName,
                    'table-name': table,
                },
                'rule-action': 'include',
                'rule-id': ruleNumber,
                'rule-name': table,
                'rule-type': 'selection',
            };
        };

        const getMappingRules = (tableMappings: TableMapping[]) => {
            const rules: any[] = [];
            let index = 1;

            tableMappings.forEach((mapping) => {
                const { 'schema-name': schemaName, tables } = mapping;
                const schemaRules = tables.map((table, table_number) =>
                    getRule(schemaName, table, (index + table_number).toString())
                );

                rules.push(...schemaRules);
                index += tables.length;
            });

            return rules;
        };
        const tableMappings: TableMapping[] = [
            {
                'schema-name': 'your_schema',
                tables: [
                    
                ],
            },
            {
                'schema-name': 'my_schema',
                tables: [
                    // list all your tables 
                ],
            },
        ];

        const tableMappingRules = {
            rules: { rules: getMappingRules(tableMappings) },
        };

        const sourceMysqlEndpoint = new dms.CfnEndpoint(this, 'MySourceMysqlEndpoint', {
            endpointIdentifier: 'sourceMysqlEndpoint',
            endpointType: 'source',
            engineName: 'mysql',
            mySqlSettings: {
                eventsPollInterval: 15,
                secretsManagerAccessRoleArn: dmsRole.roleArn,
                secretsManagerSecretId: dmsSecret.secretName,
            },
        });

        const targetS3EndpointFullLoad = new dms.CfnEndpoint(this, 'MyTargetS3EndpointFullLoad', {
            endpointIdentifier: 'targetS3EndpointFullLoad',
            endpointType: 'target',
            engineName: 's3',
            s3Settings: {
                bucketFolder: 'mysql',
                bucketName: datalakebucket.bucketName,
                cannedAclForObjects: 'bucket-owner-full-control',
                cdcMaxBatchInterval: 1800,
                compressionType: 'gzip',
                dataFormat: 'parquet',
                includeOpForFullLoad: true,
                parquetVersion: 'parquet-2-0',
                serviceAccessRoleArn: dmsRole.roleArn,
                timestampColumnName: 'dms_last_modified_timestamp',
                useTaskStartTimeForFullLoadTimestamp: false,
            },
        });
        const targetS3EndpointCDC = new dms.CfnEndpoint(this, 'MyTargetS3EndpointCDC', {
            endpointIdentifier: 'targetS3EndpointCDC',
            endpointType: 'target',
            engineName: 's3',
            s3Settings: {
                bucketFolder: 'mysql',
                bucketName: datalakebucket.bucketName,
                cannedAclForObjects: 'bucket-owner-full-control',
                cdcMaxBatchInterval: 1800,
                compressionType: 'gzip',
                dataFormat: 'parquet',
                datePartitionDelimiter: 'SLASH',
                datePartitionEnabled: true,
                datePartitionSequence: 'YYYYMMDD',
                includeOpForFullLoad: true,
                parquetVersion: 'parquet-2-0',
                serviceAccessRoleArn: dmsRole.roleArn,
                timestampColumnName: 'dms_last_modified_timestamp',
                useTaskStartTimeForFullLoadTimestamp: false,
            },
        });

        // create replication subnet group
        const replicationSubnetGroup = new dms.CfnReplicationSubnetGroup(this, 'ReplicationSubnetGroup', {
            replicationSubnetGroupDescription: 'subnet group for mysql replication',
            replicationSubnetGroupIdentifier: 'mysql-replication-subnet-group',
            subnetIds: [subnet1, subnet2, subnet3],
            tags: [
                {
                    key: 'repository',
                    value: 'templates',
                },
                {
                    key: 'cost-allocation:Team',
                    value: 'Engineering',
                },
                {
                    key: 'cost-allocation:Org',
                    value: 'Data',
                },
            ],
        });

        // create security group for dms
        const dmsSecurityGroup = new ec2.SecurityGroup(this, 'DmsSecurityGroup', {
            allowAllOutbound: true,
            description: 'dms security group',
            securityGroupName: 'dms-security-group',
            vpc: vpc,
        });

        const fullLoadReplicationConfig = new dms.CfnReplicationConfig(this, 'MysqlReplicationConfigFullLoad', {
            computeConfig: {
                maxCapacityUnits: 16,
                minCapacityUnits: 16,
                multiAz: false,
                preferredMaintenanceWindow: 'mon:09:00-mon:10:00',
                replicationSubnetGroupId: replicationSubnetGroup.replicationSubnetGroupIdentifier,
                vpcSecurityGroupIds: [dmsSecurityGroup.securityGroupId],
            },
            replicationConfigIdentifier: 'rds-mysql-to-s3-full-load',
            replicationSettings: TaskSettings,
            replicationType: 'full-load',
            resourceIdentifier: 'rds-mysql-to-s3-full-load',
            sourceEndpointArn: sourceMysqlEndpoint.ref,
            tableMappings: tableMappingRules.rules,
            tags: [
                {
                    key: 'repository',
                    value: 'aws-data-infra',
                },
                {
                    key: 'cost-allocation:Team',
                    value: 'Engineering',
                },
                {
                    key: 'cost-allocation:Org',
                    value: 'Data',
                },
            ],
            targetEndpointArn: targetS3EndpointFullLoad.ref,
        });

        const CDCReplicationConfig = new dms.CfnReplicationConfig(this, 'MysqlReplicationConfigCDC', {
            computeConfig: {
                maxCapacityUnits: 8,
                minCapacityUnits: 2,
                multiAz: false,
                preferredMaintenanceWindow: 'mon:09:00-mon:10:00',
                replicationSubnetGroupId: replicationSubnetGroup.replicationSubnetGroupIdentifier,
                vpcSecurityGroupIds: [dmsSecurityGroup.securityGroupId],
            },
            replicationConfigIdentifier: 'rds-mysql-to-s3-cdc',
            replicationSettings: TaskSettings,
            replicationType: 'cdc',
            resourceIdentifier: 'rds-mysql-to-s3-cdc',
            sourceEndpointArn: sourceMysqlEndpoint.ref,
            tableMappings: tableMappingRules.rules,
            tags: [
                {
                    key: 'repository',
                    value: 'aws-data-infra',
                },
                {
                    key: 'cost-allocation:Team',
                    value: 'Engineering',
                },
                {
                    key: 'cost-allocation:Org',
                    value: 'Data',
                },
            ],
            targetEndpointArn: targetS3EndpointCDC.ref,
        });

        // add dependencies
        replicationSubnetGroup.node.addDependency(dmsVpcRole);
        sourceMysqlEndpoint.node.addDependency(dmsRole);
        targetS3EndpointCDC.node.addDependency(dmsRole);
        CDCReplicationConfig.node.addDependency(dmsSecret);
        CDCReplicationConfig.node.addDependency(sourceMysqlEndpoint);
        CDCReplicationConfig.node.addDependency(targetS3EndpointCDC);
        CDCReplicationConfig.node.addDependency(replicationSubnetGroup);
        CDCReplicationConfig.node.addDependency(dmsSecurityGroup);
        fullLoadReplicationConfig.node.addDependency(sourceMysqlEndpoint);
        fullLoadReplicationConfig.node.addDependency(targetS3EndpointFullLoad);
        fullLoadReplicationConfig.node.addDependency(replicationSubnetGroup);
        fullLoadReplicationConfig.node.addDependency(dmsSecurityGroup);

        // add tags to all resources
        cdk.Tags.of(this).add('repository', 'templates');
        cdk.Tags.of(this).add('cost-allocation:Team', 'Engineering');
        cdk.Tags.of(this).add('cost-allocation:Org', 'Data');

        // export security group id, need to allow for ingress from dms
        new cdk.CfnOutput(this, 'DmsSecurityGroupId', {
            exportName: 'DmsSecurityGroupId',
            value: dmsSecurityGroup.securityGroupId,
        });
    }
}
