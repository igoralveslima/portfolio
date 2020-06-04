# Author: Igor Lima
# Objective: Wrapper function for boto3 client
#            Allows shared caching between CLI and boto3
#            Enables use of MFA-enforced roles


import os

import boto3
import botocore.session
from botocore import credentials

# By default the cache path is ~/.aws/boto/cache
cli_cache = os.path.join(os.path.expanduser('~'), '.aws/cli/cache')

# Construct botocore session with cache
session = botocore.session.get_session()
session.get_component('credential_provider').get_provider('assume-role').cache = credentials.JSONFileCache(cli_cache)


# Create boto3 client from session
def boto3_client(service_name):
    """
    Set up a boto3 session with custom botocore credential provider.
    Boto3 will read from AWS CLI cache and use any cached STS sessions.
    Enables the use of IAM roles that have been authenticated with MFA via CLI

    :type service_name: string
    :param service_name: A valid boto3 service name (eg. ec2, ssm, sqs)
    :return: Returns an initialized boto3 client for the given service
    """
    return boto3.Session(botocore_session=session).client(service_name)
