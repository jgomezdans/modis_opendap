import cookielib
import urllib
import urllib2
from urlparse import urlparse
import re
import os

from bs4 import BeautifulSoup

import pydap.lib
from pydap.exceptions import ClientError


def install_cas_client(username_field='username', password_field='password'):
    # Create special opener with support for Cookies.
    cj = cookielib.CookieJar()
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    opener.addheaders = [('User-agent', pydap.lib.USER_AGENT)]
    urllib2.install_opener(opener)

    def new_request(url):
        # Remove username/password from url.
        netloc = '%s:%s' % (url.hostname, url.port or 80)
        url = urlunsplit((
                url.scheme, netloc, url.path, url.query, url.fragment
                )).rstrip('?&')

        log = logging.getLogger('pydap')
        log.INFO('Opening %s' % url)
        r = urllib2.urlopen(url)

        # Detect redirection.
        if r.url != url:
            data = r.read()
            code = BeautifulSoup(data)

            # Check if we need to authenticate:
            if code.find('form'):
                # Ok, we need to authenticate. Let's get the location
                # where we need to POST the information.
                post_location = code.find('form').get('action', r.url)

                # Do a post, passing our credentials.
                inputs = code.find('form').findAll('input')
                params = dict([(el['name'], el['value']) for el in inputs
                                 if el['type']=='hidden'])
                params[username_field] = url.username
                params[password_field] = url.password
                params = urllib.urlencode(params)
                req = urllib2.Request(post_location, params)
                r = urllib2.urlopen(req)

                # Parse the response.
                data = r.read()
                code = BeautifulSoup(data)

            # Get the location from the Javascript code. Depending on the
            # CAS this code has to be changed. Ideally, the server would
            # redirect with HTTP headers and this wouldn't be necessary.
            script = code.find('script').string
            redirect = re.search('window.location.href="(.*)"', script).group(1)
            r = urllib2.urlopen(redirect)

        resp = r.headers.dict
        resp['status'] = str(r.code)
        data = r.read()

        # When an error is returned, we parse the error message from the
        # server and return it in a ``ClientError`` exception.
        if resp.get("content-description") == "dods_error":
            m = re.search('code = (?P<code>\d+);\s*message = "(?P<msg>.*)"',
                    data, re.DOTALL | re.MULTILINE)
            msg = 'Server error %(code)s: "%(msg)s"' % m.groupdict()
            raise ClientError(msg)

        return resp, data

    from pydap.util import http
    http.request = new_request
