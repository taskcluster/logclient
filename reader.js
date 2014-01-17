var URL = require('url');
var Promise = require('promise');

var stream = require('stream');
var http = require('http');
var https = require('http');

var debug = require('debug')('task-cluster-logclient');

function Reader(url, options) {
  options = options || {};

  // local options
  if (options.completeHeader) this.completeHeader = options.completeHeader;
  if (options.intervalMS) this.intervalMS = options.intervalMS;

  stream.Readable.call(this, options);
  this.baseHttpOps = URL.parse(url);

  if (this.baseHttpOps.protocol.indexOf('https') !== -1) {
    this._request = https.get;
  } else {
    this._request = http.get;
  }
}

Reader.prototype = {
  __proto__: stream.Readable.prototype,

  intervalMS: 100,

  timeoutId: null,

  /**
  Current byte offset in the stream.

  @type Number
  */
  offset: 0,

  /**
  Current tag in the stream.

  @type String
  */
  etag: null,

  /**
  Issue a request for a chunk in the stream.

  @param {Number} [offset] to start fetching from (Range).
  @param {String} [etag] to use in the if-none-match condition.
  */
  request: function(offset, etag, callback) {
    var opts = Object.create(this.baseHttpOps);


    opts.headers = {};
    if (etag) opts.headers['If-None-Match'] = etag;
    if (offset) opts.headers['Range'] = 'bytes=' + offset + '-';

    debug('request', opts);
    return this._request(opts, callback);
  },

  /**
  Header that indicates the request is 100% complete.
  */
  completeHeader: 'x-ms-meta-complete',

  /**
  True when resource is completed.
  */
  resourceComplete: false,

  _handleNewData: function(res) {
    // process incoming data
    var headers = res.headers;
    var len = parseInt(headers['content-length'], 10);

    // increment the offset
    this.offset += len;

    // log the etag so we don't request this data again.
    this.etag = headers.etag;

    debug('new data length:', len);
    debug('new data etag:', this.etag);
    debug('new offset', this.offset)

    // push all incoming data into the stream
    res.on('data', this.push.bind(this));
    res.once('end', this._completeOrPoll.bind(this, res));
  },

  _clearTimeout: function() {
    if (this.timeoutId) {
      clearTimeout(this.timeoutId);
      this.timeoutId = null;
    }
  },

  _completeOrPoll: function(res) {
    // success we are done
    if (res.headers[this.completeHeader]) {
      debug('complete');
      this.resourceComplete = true;
      this._clearTimeout();
      this.push(null);
      return;
    }

    // otherwise time to begin polling
    if (!this.timeoutId) {
      debug('begin polling next read in', this.intervalMS, 'ms');
      this.timeoutId = setTimeout(this._read.bind(this), this.intervalMS);
    }
  },

  _read: function() {
    debug('issue read');
    var requestHandler = function(res) {
      debug('handle read', res.statusCode, res.headers);
      this._clearTimeout();

      // any response may signal completion
      var status = res.statusCode;

      var hasContent = (status > 199 && status < 300);
      var notModified = status === 304;

      if (hasContent) {
        return this._handleNewData(res);
      }

      if (notModified) {
        // All http request streams _must_ be consumed (via on data, pipe
        // or calling .resume). If you don't things will randomly hang because
        // we run out of sockets in the pool (or that is what it looks like)
        res.resume();
        debug('etag match no content');
        return this._completeOrPoll(res);
      }

    }.bind(this);

    var req = this.request(this.offset, this.etag, requestHandler);
    req.on('error', this.emit.bind(this, 'error'));
  }
};

module.exports = Reader;
