var URL = require('url');
var stream = require('stream');
var debug = require('debug')('task-cluster-logclient:http_streams');

function FetchState(url) {
  this.offset = 0;
  this.etag = 0;
  this.complete = false;

  this.httpOpts = URL.parse(url);
}

FetchState.prototype = {
  complete: false,

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
  Generate common options for http requests
  */
  _headerOpts: function(headers) {
    var opts = {};
    for (var key in this.httpOpts) {
      opts[key] = this.httpOpts[key];
    }

    // custom headers
    opts.headers = {};
    if (headers) {
      for (var key in headers) {
        opts.headers[key] = headers[key];
      }
    }
    return opts;
  },

  /**
  Return the get options based on the current state.
  */
  getOpts: function(headers) {
    var opts = this._headerOpts(headers);
    opts.method = 'GET';

    if (this.etag) opts.headers['If-None-Match'] = this.etag;
    if (this.offset) opts.headers['Range'] = 'bytes=' + this.offset + '-';
    return opts;
  },

  /**
  Return head options suitable for waiting for new / complete data.
  */
  headOpts: function(headers) {
    var opts = this._headerOpts(headers);
    opts.method = 'HEAD';

    if (this.etag) opts.headers['If-None-Match'] = this.etag;
    return opts;
  },

  checkForComplete: function(res, header) {
    if (res.headers[header]) {
      return this.complete = true;
    }
    return false;
  }
};

function HttpStreams(url, options) {
  options = options || {};
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
  if (options.headers) this.headers = options.headers;
  if (options.completeHeader) this.completeHeader = options.completeHeader;
  if (options.intervalMS) this.intervalMS = options.intervalMS;

  // get the correct http interface
  this._request = this._fetchState.httpObj().request;
}

HttpStreams.prototype = {
  __proto__: stream.Readable.prototype,

  intervalMS: 500,
  timeoutId: null,

  /**
  Custom headers to pass along in the request.
  */
  headers: null,

  /**
  Header that indicates the request is 100% complete.
  */
  completeHeader: 'x-ms-meta-complete',

  /**
  Poll via HEAD requests until new data comes in.
  */
  _waitForEtagChange: function() {
    debug('wait for etag change');
    var state = this._fetchState;
    var headOpts = state.headOpts(this.headers);

    var req = this._request(headOpts, function head(res) {
      // if the response is in the 200 range we go back to _read
      var code = res.statusCode;

      // ensure request stream is freed up.
      res.resume();

      debug('etag response', code);

      // something has changed
      if (code > 199 && code < 300) {
        // its possible the complete value came in but the length is the same so
        // so ensure there are new bytes
        var contentLength = parseInt(res.headers['content-length'], 10);

        if (contentLength > state.offset) {
          // there are some new bytes so issue a GET to do the work.
          return this._read();
        }

        // its also possible there are no new bytes but the resource is now
        // complete so check for the complete header.
        if (state.checkForComplete(res, this.completeHeader)) {
          debug('resource complete');
          // we are now complete so stop looking for new data and mark the end
          // of the stream.
          this.push(null);
        }
      }

      debug('retry etag change in', this.intervalMS, 'ms');

      // poll until something changes
      this.timeoutId = setTimeout(
        this._waitForEtagChange.bind(this),
        this.intervalMS
      );
    }.bind(this));

    req.once('error', this.emit.bind(this, 'error'));
    req.end();
  },

  handleRetries: function(res) {
    var state = this._fetchState;

    // ensure these streams (which nobody else will see) get freed up.
    res.resume();

    // wait until something changes
    this._waitForEtagChange();
  },

  handleNewData: function(res) {
    var headers = res.headers;
    var state = this._fetchState;

    // update the etag && offset
    state.offset += parseInt(headers['content-length'], 10);
    state.etag = headers.etag;

    debug('process new data', 'etag:', state.etag, 'offset:', state.offset);

    this.push(res);

    // the complete header will only come with the 200 range responses
    if (state.checkForComplete(res, this.completeHeader)) {
      debug('resource complete');
      return this.push(null);
    }
  },

  /**
  Generate a set of readable streams
  */
  _read: function() {
    var state = this._fetchState;

    // ignore any reads if we are done
    if (state.complete) return;

    // issue the request and pass long our custom headers
    var req = this._request(state.getOpts(this.headers));
    req.end();

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
