@description('Azure location for all resources.')
param location string = resourceGroup().location

@allowed([
  'try'
  'small'
  'production'
])
@description('Deployment tier.')
param deploymentTier string = 'try'

@description('Name prefix used for resources.')
@minLength(3)
param namePrefix string = 'acaaf'

@description('Container image used by the sample ACA Job.')
param acaJobImage string = 'mcr.microsoft.com/k8se/quickstart-jobs:latest'

@secure()
@description('Required only for production tier. Existing Postgres connection string managed outside this template.')
param existingPostgresConnectionString string = ''

var suffix = uniqueString(subscription().subscriptionId, resourceGroup().id)
var workspaceName = '${namePrefix}-law-${suffix}'
var managedEnvName = '${namePrefix}-env-${suffix}'
var jobName = '${namePrefix}-job-${suffix}'
var storageAccountName = toLower('st${take(suffix, 22)}')
var fileShareName = 'airflow'
var createStorage = deploymentTier == 'small' || deploymentTier == 'production'
var isProduction = deploymentTier == 'production'

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: workspaceName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

resource managedEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: managedEnvName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
    workloadProfiles: [
      {
        name: 'Consumption'
        workloadProfileType: 'Consumption'
      }
    ]
  }
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = if (createStorage) {
  name: storageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource fileService 'Microsoft.Storage/storageAccounts/fileServices@2023-05-01' = if (createStorage) {
  parent: storage
  name: 'default'
}

resource fileShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-05-01' = if (createStorage) {
  parent: fileService
  name: fileShareName
  properties: {
    accessTier: 'TransactionOptimized'
    enabledProtocols: 'SMB'
  }
}

resource acaJob 'Microsoft.App/jobs@2024-03-01' = {
  name: jobName
  location: location
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 1
      replicaTimeout: 1800
      secrets: isProduction ? [
        {
          name: 'postgres-connection-string'
          value: existingPostgresConnectionString
        }
      ] : []
      registries: []
    }
    template: {
      containers: [
        {
          name: 'worker'
          image: acaJobImage
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          env: concat(
            createStorage ? [
              {
                name: 'AIRFLOW_FILES_SHARE'
                value: fileShareName
              }
            ] : [],
            isProduction ? [
              {
                name: 'POSTGRES_CONNECTION_STRING'
                secretRef: 'postgres-connection-string'
              }
            ] : []
          )
        }
      ]
    }
  }
}

output acaJobResourceId string = acaJob.id
output acaJobName string = acaJob.name
output acaManagedEnvironmentName string = managedEnvironment.name
output selectedTier string = deploymentTier
