#!/usr/bin/env python
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This file contains some utilities used for processing tweet data and writing
data to BigQuery
"""

import collections
import datetime
import time

from apiclient import discovery
import dateutil.parser
import httplib2
from oauth2client.client import GoogleCredentials

SCOPES = ['https://www.googleapis.com/auth/pubsub']

def get_credentials():
    """Get the Google credentials needed to access our services."""
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
            credentials = credentials.create_scoped(SCOPES)
    return credentials

def create_pubsub_client(credentials):
    """Build the pubsub client."""
    http = httplib2.Http()
    credentials.authorize(http)
    return discovery.build('pubsub', 'v1beta2', http=http)


def flatten(lst):
    """Helper function used to massage the raw tweet data."""
    for el in lst:
        if (isinstance(el, collections.Iterable) and
                not isinstance(el, basestring)):
            for sub in flatten(el):
                yield sub
        else:
            yield el
