var http = require('http'),
    Promise = require('promise');

/**
Minimal wrapper around node's http.get function (using promises)
*/
function get(url) {
  return new Promise(function(accept, reject) {

    // handle success
    var req = http.get(url, function(res) {
      accept(res);
    });

    // handle errors
    req.once('error', reject);
  });
}

module.exports = get;
