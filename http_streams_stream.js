var URL = require('url');
var Promise = require('promise');

var stream = require('stream');
var debug = require('debug')('task-cluster-logclient:http_streams');

function FetchState(url) {
  this.offset = 0;
  this.etag = 0;
  this.complete = false;

  this.httpOpts = URL.parse(url);
}

FetchState.prototype = {

  /**
  Determine the type of http object and return it.
  */
  httpObj: function() {
    if (this.httpOpts.protocol.indexOf('https') !== -1) {
      return require('https');
    }
    return require('http');
  },

  /**
  Return the get options based on the current state.
  */
  getOpts: function() {
    var opts = {};
    for (var key in this.httpOpts) {
      opts[key] = this.httpOpts[key];
    }

    opts.headers = {};
    if (this.etag) opts.headers['If-None-Match'] = this.etag;
    if (this.offset) opts.headers['Range'] = 'bytes=' + this.offset + '-';
    return opts;
  },

  isComplete: function(res, header) {
    return res.headers[header];
  }
};

function HttpStreams(url, options) {
  stream.Readable.call(this, {
    // zero indicates that no preemptive buffering will occur. This is ideal
    // since we want to fetch a stream then consume it entirely before asking
    // for another stream.
    highWaterMark: 0,

    // streams are (obviously) objects
    objectMode: true
  });

  /**
  Only kept around for the public api
  */
  this.url = url;

  /**
  Contains current "state" of the fetching.
  */
  this._fetchState = new FetchState(url);

  // local options
  if (options.completeHeader) this.completeHeader = options.completeHeader;
  if (options.intervalMS) this.intervalMS = options.intervalMS;

  // get the correct http interface
  this._get = this._fetchState.httpObj().get;
}

HttpStreams.prototype = {
  __proto__: stream.Readable.prototype,

  intervalMS: 100,
  timeoutId: null,

  /**
  Header that indicates the request is 100% complete.
  */
  completeHeader: 'x-ms-meta-complete',

  handleRetries: function(res) {
    var state = this._fetchState;

    // ensure these streams (which nobody else will see) get freed up.
    res.resume();

    // some non error cases will trigger completion without new data (like 304)
    if (state.isComplete(res, this.completeHeader)) {
      return this.push(null);
    }

    // everything else is a retry
    debug('retrying', 'will retry in', this.intervalMS, 'ms');

    // XXX: We might want exponential back off in some error cases
    this.timeoutId = setTimeout(
      this._read.bind(this),
      this.intervalMS
    );
  },

  handleNewData: function(res) {
    var headers = res.headers;
    var state = this._fetchState;

    // update the etag && offset
    state.offset += parseInt(headers['content-length'], 10);
    state.etag = headers.etag;

    debug('process new data', 'etag:', state.etag, 'offset:', state.offset);

    this.push(res);

    // new data may also come with a complete header.
    if (state.isComplete(res, this.completeHeader)) {
      return this.push(null);
    }
  },

  /**
  Generate a set of readable streams
  */
  _read: function() {
    var state = this._fetchState;
    var req = this._get(state.getOpts());

    req.once('response', function(res) {

      var code = res.statusCode;
      var hasData = code > 199 && code < 300;

      debug('response', code, 'has data:', hasData);
      debug('response headers', res.headers);

      // handle the data case first (no retries)
      if (hasData) {
        return this.handleNewData(res);
      }

      // everything else will yield a retry
      this.handleRetries(res);

    }.bind(this));

    req.once('error', this.emit.bind(this, 'error'));
  }
};

module.exports = HttpStreams;
