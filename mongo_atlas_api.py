# Script Information
# Author: Igor Lima
# Objective: Library of functions used to interface with MongoDB Atlas API

# load necessary packages
import json
import os

import requests
from requests.auth import HTTPDigestAuth


def parse_response(r):
    """
    Check response for HTTP errors and retrieve decoded contents

    :param r: A requests response object
    :type r: str
    :return: The UTF8 decoded content of the provided response object
             or Exception if HTTP error is found
    """

    # before parsing, ensure there are no errors
    r.raise_for_status()

    # decode from binary, return results item
    content = json.loads(r.content.decode('utf-8'))
    if 'results' in content:
        result = content['results']
        return result
    else:
        return content


def get_users(group_id):
    """
    Retrieves a listing of database users from the provided MongoDB Atlas project (group)

    :param group_id: A valid MongoDB Atlas group id
    :type group_id: str
    """
    # create necessay variables for API call
    endpoint = os.environ['mongoBaseUrl'] + group_id + '/databaseUsers'
    auth_digest = HTTPDigestAuth(
        os.environ['mongoUser'], os.environ['mongoApiKey'])

    # make call and check for errors
    r = requests.get(url=endpoint, auth=auth_digest)

    # parse content and return result
    result = parse_response(r)
    return result


def update_user(group_id,
                user_name,
                user_password=None,
                roles=None):
    """
    Update an existing database user with a new password and permissions

    :param group_id: A valid MongoDB Atlas project (group) id
    :type group_id: str
    :param user_name: The username of an existing database user
    :type user_name: str
    :param user_password: The new password to be associated with this user
    :type user_password: str
    :param roles: A list containing ALL database roles for the user
    :type roles: list
    """
    # create necessary variables for API call
    endpoint = os.environ['mongoBaseUrl'] + \
               group_id + '/databaseUsers/admin/' + user_name
    auth_digest = HTTPDigestAuth(
        os.environ['mongoUser'], os.environ['mongoApiKey'])

    # create payload
    if user_password is None and roles is None:
        raise ValueError(
            'Either user_password or roles argument must be provided')
    else:
        payload = {}

    if user_password:
        payload['password'] = user_password

    if roles:
        payload['roles'] = roles

    # make call and check for errors
    r = requests.patch(url=endpoint, auth=auth_digest, json=payload)

    # parse content and return result
    result = parse_response(r)
    return result


def create_user(group_id,
                user_name,
                user_password,
                roles):
    """
    Create a new database user

    :param group_id: A valid MongoDB Atlas project (group) id
    :type group_id: str
    :param user_name: The username of the database user
    :type user_name: str
    :param user_password: The password to be associated with this user
    :type user_password: str
    :param roles: A list containing ALL database roles for the user
    :type roles: list
    """
    # create necessary variables for API call
    endpoint = os.environ['mongoBaseUrl'] + \
               group_id + '/databaseUsers'
    auth_digest = HTTPDigestAuth(
        os.environ['mongoUser'], os.environ['mongoApiKey'])

    # create payload
    payload = {'databaseName': 'admin',
               'groupId': group_id,
               'username': user_name,
               'roles': roles,
               'password': user_password}

    # make call and check for errors
    r = requests.post(url=endpoint, auth=auth_digest, json=payload)

    # parse content and return result
    result = parse_response(r)
    return result


def delete_user(group_id,
                user_name):
    """
    Delete an existing database user

    :param group_id: A valid MongoDB Atlas project (group) id
    :type group_id: str
    :param user_name: The username of the database user
    :type user_name: str
    """
    # create necessary variables for API call
    endpoint = os.environ['mongoBaseUrl'] + \
               group_id + '/databaseUsers/admin/' + user_name
    auth_digest = HTTPDigestAuth(
        os.environ['mongoUser'], os.environ['mongoApiKey'])

    # make call and check for errors
    r = requests.delete(url=endpoint, auth=auth_digest)

    # on successful delete, nothing is returned from API
    # no need to use parsing function
    # manually checking for errors and forcing an expected result
    r.raise_for_status()
    return True


def get_clusters(group_id):
    """
    Retrieve a list of cluster names active in the MongoDB Atlas project (group)

    :param group_id: A valid MongoDB Atlas project (group) id
    :type group_id: str
    """
    # create necessay variables for API call
    endpoint = os.environ['mongoBaseUrl'] + group_id + '/clusters'
    auth_digest = HTTPDigestAuth(
        os.environ['mongoUser'], os.environ['mongoApiKey'])

    # make call and check for errors
    r = requests.get(url=endpoint, auth=auth_digest)

    # parse content and return result
    result = parse_response(r)
    return result


def get_cluster(group_id, cluster_name):
    """
    Retrieve the cluster information (such as node hostnames) for a given MongoDB Atlas cluster

    :param group_id: A valid MongoDB Atlas project (group) id
    :type group_id: str
    :param cluster_name: The name of an active cluster within the project (group) id
    :type cluster_name: str
    """
    # create necessay variables for API call
    endpoint = os.environ['mongoBaseUrl'] + group_id + '/clusters/' + cluster_name
    auth_digest = HTTPDigestAuth(
        os.environ['mongoUser'], os.environ['mongoApiKey'])

    # make call and check for errors
    r = requests.get(url=endpoint, auth=auth_digest)

    # parse content and return result
    result = parse_response(r)
    return result


def download_log_file(group_id, host_name, download_file_path):
    """
    Retrieve a tar.gz file for a given MongoDB Atlas cluster node.
    Note, the logs for all nodes within a cluster must be retrieved in order
    to have a complete view of actions within a cluster

    :param group_id: A valid MongoDB Atlas project (group) id
    :type group_id: str
    :param host_name: The name of an active cluster node within the project (group) id
    :type host_name: str
    :param download_file_path: The absolute file path on the local machine where the
                               file will be saved
    :type download_file_path: str
    """
    # create necessary variables for API call
    endpoint = os.environ['mongoBaseUrl'] + group_id + \
               '/clusters/' + host_name + '/logs/mongodb.gz'
    auth_digest = HTTPDigestAuth(
        os.environ['mongoUser'], os.environ['mongoApiKey'])

    # make call and check for errors
    r = requests.get(url=endpoint, auth=auth_digest)
    r.raise_for_status()

    # open file and write contents into it
    with open(download_file_path, 'wb') as f:
        f.write(r.content)

    return True
