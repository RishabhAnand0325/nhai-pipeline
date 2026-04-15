"""
Code is used in 'codeBuildTrigger' lambda function
Added here just for documentation / backup
"""

import boto3

def lambda_handler( event, context ):

  cb = boto3.client( 'codebuild' )
  build = {
    'projectName': 'videoModelBuild',
    'sourceVersion': 'master',
    'environmentVariablesOverride' : [{
      'name':'S3_KEY',
      'value' :event['Records'][0]['s3']['object']['key'],
      'type' : 'PLAINTEXT'
    }]
  }

  print( 'Starting build for project {0} from commit ID {1}...'.format( build['projectName'], build['sourceVersion'] ) )
  cb.start_build( **build )
  print( 'Successfully launched build.' )

  return 'Success.'