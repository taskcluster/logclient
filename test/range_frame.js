var rangeParser = require('range-parser');

function FrameState() {
  this.buffer = new Buffer(0);
}

FrameState.prototype = {
  etag: null,

  complete: false,

  etagMatch: function(value) {
    if (!this.etag) return;
    return value === this.etag;
  },

  write: function(buffer) {
    this.buffer = Buffer.concat([this.buffer, buffer]);
    this.etag = this.buffer.toString('base64');
  },

  fetch: function(start) {
    start = start || 0;
    return this.buffer.slice(start);
  },

  end: function(buffer) {
    if (buffer) this.write(buffer);
    this.complete = true;
  }
};

/**
Handles all requests for the server until the buffer is exhausted.
*/
function rangeFrame() {
  var state = new FrameState();
  var handler = function(req, res, done) {
    var headers = req.headers;

    // set the current etag state
    res.setHeader('Etag', state.etag);

    // check for if conditions
    if (headers['if-none-match'] === state.etag) {
      res.writeHead(304, {
        'Content-Length': 0
      });
      return res.end();
    }

    // if at any point state is marked as complete remove this handler from the
    // frames.
    if (state.complete) done();

    // check for range
    var rangeStr = headers.range;
    if (!rangeStr) {
      // if there is no range return the entire buffer
      res.writeHead(200, {
        'Content-Length': state.buffer.length
      });

      return res.end(state.buffer);
    }

    // handle range requests
    var range = rangeParser(state.buffer.length, rangeStr)[0];
    var content = state.fetch(range.start);

    res.writeHead(206, {
      'Content-Length': content.length
    });

    res.end(content);
  };

  handler.state = state;
  return handler;
}

module.exports = rangeFrame;
