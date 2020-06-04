# Author: Igor Lima
# Objective: Wrapper class for SSM Parameter Store, allows easier list, get, and put methods

from aws_session_cache import boto3_client

ssm = boto3_client('ssm')


class SSM:
    """
    The SSM object provides an easier interface for the SSM Parameter Store.
    With limited functionality needed to get, put, and list items
    """

    @staticmethod
    def get_param(path):
        """
        Returns the decrypted value for a single parameter.

        :param path: The absolute path (key) for a SSM parameter
        :type path: str
        :return: A string representation of the parameter value
        """
        response = ssm.get_parameter(Name=path,
                                     WithDecryption=True)
        return response["Parameter"]["Value"]

    @staticmethod
    def put_param(name, value):
        """
        Stores the provided key and encrypted value into the SSM Parameter Store service.

        :param name: The absolute path (key) for a SSM parameter
        :type name: str
        :param value: The contents to be encrypted and stored
        :type value: str
        :return: True (boolean)
        """
        ssm.put_parameter(Name=name,
                          Value=value,
                          Type='SecureString',
                          Overwrite=True)
        return True

    @staticmethod
    def _get_paginated_results(ssm_method, options):
        """
        Executes the provided SSM method using a paginator and retrieves all results.
        Useful when listing potentially large number of SSM parameter keys

        :param ssm_method: A valid SSM API method, see boto3 SSM documentation for details
        :type ssm_method: str
        :param options: A dictionary containing configurations for the chosen method
        :type options: dict
        :return: A list containing all results of the SSM method
        """
        # define paginator for ssm param results
        paginator = ssm.get_paginator(ssm_method)
        page_iterator = paginator.paginate(**options, PaginationConfig={'PageSize': 10})

        # paginate through API request and save results
        results = []
        for page in page_iterator:
            for parameter in page["Parameters"]:
                results.append(parameter)

        return results

    def get_params(self, prefix):
        """
        Recursively retrieves the decrypted values of all parameters matching the
        provided prefix. Wrapper for the SSM client 'get_parameters_by_path' method.

        :param prefix: A prefix pattern of existing items
        :type prefix: str
        :return: A list where each element is a dict containing
                 the key and value of each matched parameter
        """
        results = []

        options = {
            'Path': prefix,
            'Recursive': True,
            'WithDecryption': True
        }

        params_raw = self._get_paginated_results(ssm_method='get_parameters_by_path',
                                                 options=options)

        for param in params_raw:
            results.append({'name': param['Name'],
                            'value': param['Value']})
        return results

    def list_params(self, prefix):
        """
        Recursively retrieves the keys of all parameters matching the
        provided prefix. Wrapper for the SSM client 'describe_parameters' method.
        Does NOT retrieve the parameter values.

        :param prefix: A prefix pattern of existing items
        :type prefix: str
        :return: A list with all matches keys
        """
        results = []

        options = {
            'ParameterFilters': [
                {
                    'Key': 'Name',
                    'Option': 'BeginsWith',
                    'Values': [prefix]
                }
            ]
        }

        params_raw = self._get_paginated_results(ssm_method='describe_parameters',
                                                 options=options)

        for param in params_raw:
            results.append(param['Name'])
        return results
