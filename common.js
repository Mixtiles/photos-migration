// Code copied from mixtiles-common:

const awsStorage = require('@aws-sdk/lib-storage')
const s3Client = require('@aws-sdk/client-s3')
const Upload = awsStorage.Upload
const S3 = s3Client.S3

const DEFAULT_REGION = 'us-west-2'

const BUCKET_TO_REGION = {
  'mixtiles-assets': 'us-east-1',
  'mixtiles-lexi': 'us-east-2',
  'easyplant-tv-rain-reports': 'us-west-2',
  'aws-cloudtrail-logs-627068100163-27b7cdb8': 'us-east-1',
  'cf-templates-1u8v43a8nxsj2-us-east-1': 'us-east-1',
  'mixtiles-jen-dev': 'us-west-2',
  'mixtiles-data-export': 'us-east-1',
  'mixtiles-data-export-staging': 'us-east-1',
  'easyplant-order-admin': 'us-east-1',
  easyplant: 'us-west-2',
  'filestack-mixtiles-stg-uploads': 'us-west-2',
  'filestack-mixtiles-uploads': 'us-west-2',
  'around-upload': 'us-west-2',
  'around-upload-dev': 'us-west-2',
  'jumpcloud-apps': 'us-east-1',
  'mixtiles-backup': 'us-east-1',
  'around-public': 'us-west-2',
  'mixtiles-tilebooth': 'us-east-1',
  'af-datalocker-data-feb2022': 'us-east-1',
  'mixtiles-app-json': 'us-west-1',
  'staging-env-dags': 'us-west-2',
  'ctv-vibe-report': 'us-west-2',
  'easyplant-public': 'us-west-2',
  'test-env-dags': 'us-west-2',
  'tv-rain-report': 'us-west-2',
  'mixtiles-art-backup-catalog': 'us-west-2',
  'mixtiles-ios-dependency-cache': 'us-west-2',
  'mixtiles-uploads-local': 'us-west-2',
  'campaign.mixtiles.com': 'us-west-2',
  'campaign.mixtiles-staging.com-logs': 'us-west-2',
  'campaign.mixtiles-staging.com': 'us-west-2',
  'data-image-db-backup': 'us-east-1',
  'manufacturing-cober': 'us-west-2',
  'manufacturing-dev-cober': 'us-west-2',
  'manufacturing-vegas': 'us-west-1',
  'manufacturing-dev-vegas': 'us-west-1',
  'manufacturing-vegas-tgi': 'us-west-1',
  'manufacturing-dev-vegas-tgi': 'us-west-1',
  'manufacturing-tgi': 'us-east-1',
  'manufacturing-dev-tgi': 'us-east-1',
  'manufacturing-anstadt': 'us-east-1',
  'manufacturing-dev-anstadt': 'us-east-1',
  'manufacturing-traffic': 'eu-central-1',
  'manufacturing-dev-traffic': 'eu-central-1',
  'manufacturing-pureprint': 'eu-west-2',
  'manufacturing-dev-pureprint': 'eu-west-2',
  'manufacturing-jondo': 'us-west-1',
  'manufacturing-dev-jondo': 'us-west-1',
  'mixtiles-finance-reports': 'us-east-1',
  'mixtiles-uploads-external': 'us-west-2',
  'mixtiles-appsflyer-data': 'us-east-1',
  'mixtiles-public-assets': 'us-east-1',
  'mixtiles-marketing': 'us-east-1',
  mixtiles: 'us-west-2',
  'mixtiles-factory': 'us-west-2',
  'mixtiles-uploads-dev': 'us-west-2',
  'mixtiles-uploads': 'us-west-2',
  'mixtiles-uploads-transformed-dev': 'us-west-2',
  'mixtiles-uploads-transformed': 'us-west-2',
  'mixtiles-json': 'us-west-2',
  'mixtiles-csvs': 'us-west-2',
  'mixtiles-debug': 'us-west-2',
  'mixtiles-jen': 'us-east-1',
  'elasticbeanstalk-eu-central-1-627068100163': 'eu-central-1',
  'mixtiles-debug-europe': 'eu-central-1',
  'mixtiles-jen-europe-prod': 'eu-central-1',
  'test-mixtiles-jen-europe': 'eu-west-1',
  'mixtiles-airflow-logs': 'eu-central-1',
  'mixtiles-airflow-staging-logs': 'eu-central-1',
  'mixtiles-invoices': 'us-east-1',
  'mixtiles-labels': 'us-east-1',
}

const getRegionFromBucket = (bucket) => BUCKET_TO_REGION[bucket] ?? DEFAULT_REGION

async function uploadStreamToS3({ stream: Body, filename: Key, bucketName: Bucket }) {
    let data
    try {
      const s3Client = new S3({ region: getRegionFromBucket(Bucket) })
      const upload = new Upload({
        client: s3Client,
        params: { Bucket, Key, Body },
      })
  
      let prevPart = 0
      upload.on('httpUploadProgress', (progress) => {
        const { loaded, total, part } = progress
        const totalParts = total ? Math.floor(total / 1024 / 1024 / 5) : 'unknown'
        if (part > prevPart) {
          prevPart = part
        }
      })
  
      data = await upload.done()
    } catch (error) {
      throw error
    }
  
    if (isUploadComplete(data)) {
      return { url: data.Location }
    } else {
      throw new Error('uploadStreamTos3 upload aborted')
    }
  }
  
  function isUploadComplete(output) {
    return output.ETag !== undefined
  }
  
  module.exports = {
    uploadStreamToS3,
  }