import logging, time, base64
import httpx
from urllib.parse import urlencode, urlunparse, urlparse

try:
    from xml.etree import ElementTree
    from xml.etree.ElementTree import Element
except ImportError:
    from elementtree import ElementTree
    from elementtree.ElementTree import Element

# AUTH_HANDLERS and NTLM support removed for httpx migration. If NTLM is needed, use httpx-ntlm or similar.

class V1Error(Exception): pass

class V1AssetNotFoundError(V1Error): pass

class V1Server(object):
  "Accesses a V1 HTTP server as a client of the XML API protocol"

  def __init__(self, address="localhost", instance="VersionOne.Web", username='', password='', token=None, scheme="http", instance_url=None, logparent=None, loglevel=logging.ERROR, verify=True):
    """
    If *instance_url* is set its value will override address, instance,
    scheme and object's instance_url attributes.
    If *token* is not None a HTTP header will be added to each request.
    :param address: target hostname
    :param instance: instance
    :param username: credentials (username)
    :param password: credentials (password)
    :param token: credentials (authentication token)
    :param scheme: HTTP scheme
    :param instance_url: instance URL
    :param logparent: logger prefix
    :param loglevel: logging level
    """

    if instance_url:
      self.instance_url = instance_url
      parsed = urlparse(instance_url)
      self.address = parsed.netloc
      self.instance = parsed.path.strip('/')
      self.scheme = parsed.scheme
    else:
      self.address = address
      self.instance = instance.strip('/')
      self.scheme = scheme
      self.instance_url = self.build_url('')

    modulelogname='v1pysdk.client'
    logname = "%s.%s" % (logparent, modulelogname) if logparent else None
    self.logger = logging.getLogger(logname)
    self.logger.setLevel(loglevel)
    self.username = username
    self.password = password
    self.token = token
    self._init_client()
    self.verify = verify
        

  def _init_client(self):
    headers = {"Content-Type": "text/xml;charset=UTF-8"}
    if self.token:
      headers["Authorization"] = f"Bearer {self.token}"
      self.client = httpx.Client(headers=headers, verify=self.verify)
    elif self.username and self.password:
      self.client = httpx.Client(auth=(self.username, self.password), headers=headers, verify=self.verify)
    else:
      self.client = httpx.Client(headers=headers, verify=self.verify)

  def http_get(self, url):
    response = self.client.get(url)
    response.raise_for_status()
    return response
  
  def http_post(self, url, data=''):
    response = self.client.post(url, content=data)
    response.raise_for_status()
    return response
    
  def build_url(self, path, query='', fragment='', params=''):
    "So we dont have to interpolate urls ad-hoc"
    path = self.instance + '/' + path.strip('/')
    if isinstance(query, dict):
      query = urlencode(query)
    elif query is None:
      query = ''
    url = urlunparse( (self.scheme, self.address, path, params, query, fragment) )
    return url

  def _debug_headers(self, headers):
    self.logger.debug("Headers:")
    for hdr in str(headers).split('\n'):
      self.logger.debug("  %s" % hdr)

  def _debug_body(self, body, headers):
    try:
      ctype = headers['content-type']
    except AttributeError:
      ctype = None
    if ctype is not None and ctype[:5] == 'text/':
      self.logger.debug("Body:")
      for line in str(body).split('\n'):
        self.logger.debug("  %s" % line)
    else:
      self.logger.debug("Body: non-textual content (Content-Type: %s). Not logged." % ctype)

  def fetch(self, path, query=None, postdata=None):
    "Perform an HTTP GET or POST depending on whether postdata is present"
    # Accept query as None, str, or dict
    if query is None:
      url = self.build_url(path)
    elif isinstance(query, dict):
      url = self.build_url(path, query=urlencode(query))
    else:
      url = self.build_url(path, query=str(query))
    self.logger.debug("URL: %s" % url)
    try:
      if postdata is not None:
        if isinstance(postdata, dict):
          postdata = urlencode(postdata)
          self.logger.debug("postdata: %s" % postdata)
        response = self.http_post(url, postdata)
      else:
        response = self.http_get(url)
      body = response.text
      self._debug_headers(response.headers)
      self._debug_body(body, response.headers)
      return (None, body)
    except httpx.HTTPStatusError as e:
      if e.response.status_code == 401:
        raise
      body = e.response.text
      self._debug_headers(e.response.headers)
      self._debug_body(body, e.response.headers)
      # Mimic previous API: attach code and headers to exception
      e.code = e.response.status_code
      e.headers = e.response.headers
      return (e, body)

  def handle_non_xml_response(self, body, exception, msg, postdata):
    if getattr(exception, "code", 0) >= 500:
      # 5XX error codes mean we won't have an XML response to parse
      self.logger.error("{0} during {1}".format(exception, msg))
      if postdata is not None:
        self.logger.error(postdata)
      raise exception

  def get_xml(self, path, query=None, postdata=None):
    verb = "HTTP POST to " if postdata else "HTTP GET from "
    msg = verb + path
    self.logger.info(msg)
    exception, body = self.fetch(path, query=query, postdata=postdata)
    if exception:
      self.handle_non_xml_response(body, exception, msg, postdata)
      self.logger.warn("{0} during {1}".format(exception, msg))
      if postdata is not None:
        self.logger.warn(postdata)
    document = ElementTree.fromstring(body)
    if exception:
      exception.xmldoc = document
      if getattr(exception, "code", 0) == 404:
        raise V1AssetNotFoundError(exception)
      elif getattr(exception, "code", 0) == 400:
        raise V1Error('\n'+body)
      else:
        raise V1Error(exception)
    return document
   
  def get_asset_xml(self, asset_type_name, oid, moment=None):
    path = '/rest-1.v1/Data/{0}/{1}/{2}'.format(asset_type_name, oid, moment) if moment else '/rest-1.v1/Data/{0}/{1}'.format(asset_type_name, oid)
    return self.get_xml(path)
    
  def get_query_xml(self, asset_type_name, where=None, sel=None):
    path = '/rest-1.v1/Data/{0}'.format(asset_type_name)
    query = None
    if where is not None or sel is not None:
      query = {}
      if where is not None:
        query['Where'] = where
      if sel is not None:
        query['sel'] = sel
    return self.get_xml(path, query=query)
    
  def get_meta_xml(self, asset_type_name):
    path = '/meta.v1/{0}'.format(asset_type_name)
    return self.get_xml(path)
    
  def execute_operation(self, asset_type_name, oid, opname):
    path = '/rest-1.v1/Data/{0}/{1}'.format(asset_type_name, oid)
    query = {'op': opname}
    return self.get_xml(path, query=query, postdata={})
    
  def get_attr(self, asset_type_name, oid, attrname, moment=None):
    path = '/rest-1.v1/Data/{0}/{1}/{3}/{2}'.format(asset_type_name, oid, attrname, moment) if moment else '/rest-1.v1/Data/{0}/{1}/{2}'.format(asset_type_name, oid, attrname)
    return self.get_xml(path)
  
  def create_asset(self, asset_type_name, xmldata, context_oid=''):
    body = ElementTree.tostring(xmldata, encoding="utf-8")
    query = None
    if context_oid:
      query = {'ctx': context_oid}
    path = '/rest-1.v1/Data/{0}'.format(asset_type_name)
    return self.get_xml(path, query=query, postdata=body)
    
  def update_asset(self, asset_type_name, oid, update_doc):
    newdata = ElementTree.tostring(update_doc, encoding='utf-8')
    path = '/rest-1.v1/Data/{0}/{1}'.format(asset_type_name, oid)
    return self.get_xml(path, postdata=newdata)


  def get_attachment_blob(self, attachment_id, blobdata=None):
    path = '/attachment.v1/{0}'.format(attachment_id)
    exception, body = self.fetch(path, postdata=blobdata)
    if exception:
      raise exception
    return body
    
  set_attachment_blob = get_attachment_blob
  
    
    
  
    

