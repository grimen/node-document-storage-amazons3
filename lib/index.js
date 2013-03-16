require('sugar');
var util = require('util'),
    path = require('path');

// HACK: ...until Node.js `require` supports `instanceof` on modules loaded more than once. (bug in Node.js)
var Storage = global.NodeDocumentStorage || (global.NodeDocumentStorage = require('node-document-storage'));

// -----------------------
//  DOCS
// --------------------
//  - https://github.com/LearnBoost/knox

// -----------------------
//  Constructor
// --------------------

// new AmazonS3 ();
// new AmazonS3 (options);
// new AmazonS3 (url);
// new AmazonS3 (url, options);
function AmazonS3 () {
  var self = this;

  self.klass = AmazonS3;
  self.klass.super_.apply(self, arguments);

  self.options.server.key = self.options.server.username;
  self.options.server.secret = self.options.server.password;
  self.options.server.secure = (self.options.server.protocol === 'https');
  self.options.server.bucket = self.options.server.db.replace(/^\//, '');
  // self.options.server.region = 'us-standard'; // TODO: Try extract from '*.s3-<region>.amasonaws.com'
  // self.options.server.endpoint = self.options.server.hostname; // TODO: Try extract from '<bucket>.s3-*.amasonaws.com'
  self.options.server.port = self.options.server.port || (self.options.server.secret ? 443 : 80);
}

util.inherits(AmazonS3, Storage);

// -----------------------
//  Class
// --------------------
AmazonS3.id = 'amazons3';
AmazonS3.protocol = 'https';

AmazonS3.defaults = AmazonS3.defaults || {};
AmazonS3.defaults.url = Storage.env('AMAZONS3_URL') || 'https://s3.amazonaws.com/{db}-{env}'.assign({
  db: 'node-document-default',
  env: process.env.NODE_ENV || 'development'
});
AmazonS3.defaults.options = {
  server: {
    extension: '.json'
  },
  client: {
    agent: undefined,
    headers: {
      set: {
        'Content-Type': 'application/json',
        'x-amz-acl': 'private'
      },
      get: {
        'Content-Type': 'application/json'
      },
      del: {
        'Content-Type': 'application/json'
      },
      exists: {
        'Content-Type': 'application/json'
      }
    }
  }
};

AmazonS3.url = AmazonS3.defaults.url;
AmazonS3.options = AmazonS3.defaults.options;

AmazonS3.reset = Storage.reset;

// -----------------------
//  Instance
// --------------------

// #connect ()
AmazonS3.prototype.connect = function() {
  var self = this;

  self._connect(function() {
    var knox = require('knox');

    self.client = knox.createClient(self.options.server);

    self.client
      .get('/node-document-auth')
      .on('response', function(res) {
        var err = (res.statusCode >= 400 && res.statusCode !== 404) ? res : null;

        self.authorized = !err;

        self.emit('ready', err);
      })
      .end();
  });
};

// #set (key, value, [options], callback)
// #set (keys, values, [options], callback)
AmazonS3.prototype.set = function() {
  var self = this;

  self._set(arguments, function(key_values, options, done, next) {
    key_values.each(function(key, value) {
      var resource = self.resource(key);

      var headers = JSON.parse(JSON.stringify(Object.merge(self.options.client.headers.set, {'Content-Length': value.length}, true, false))); // NOTE: Sugar.js-Object fails.

      self.client
        .put('/' + resource.key + resource.ext, headers)
        .on('response', function(response) {
          var error, result;

          response.setEncoding('utf8');

          response.on('error', function(err) {
            error = err;
          });

          response.on('end', function() {
            result = (response.statusCode < 400);

            next(key, error, result, response);
          });
        })
        .end(value, 'utf8');
    });
  });
};

// #get (key, [options], callback)
// #get (keys, [options], callback)
AmazonS3.prototype.get = function() {
  var self = this;

  self._get(arguments, function(keys, options, done, next) {
    keys.each(function(key) {
      var resource = self.resource(key);

      self.client
        .get('/' + resource.key + resource.ext, self.options.client.headers.get)
        .on('response', function(response) {
          var error, result = '';

          response.setEncoding('utf8');

          response.on('data', function(data) {
            result += data;
          });

          response.on('error', function(err) {
            error = err;
          });

          response.on('end', function() {
            if (response.statusCode >= 400) {
              error = '' + result;
              result = null;
            }

            next(key, error, result, response);
          });
        })
        .end();
    });
  });
};

// #del (key, [options], callback)
// #del (keys, [options], callback)
AmazonS3.prototype.del = function() {
  var self = this;

  self._del(arguments, function(keys, options, done, next) {
    keys.each(function(key) {
      var resource = self.resource(key);

      self.client
        .get('/' + resource.key + resource.ext, self.options.client.headers.get)
        .on('response', function(_response) {
          var _error, _result = '';

          _response.setEncoding('utf8');

          _response.on('data', function(data) {
            _result += data;
          });

          _response.on('error', function(err) {
            _error = err;
          });

          _response.on('end', function() {
            if (_response.statusCode === 404) {
              next(key, _error, false, _response);
              return;
            }

            self.client
              .del('/' + resource.key + resource.ext, self.options.client.headers.del)
              .on('response', function(response) {
                var error, result;

                response.setEncoding('utf8');

                response.on('error', function(err) {
                  error = err;
                });

                response.on('end', function() {
                  if (error) {
                    next(key, error, false, response);
                    return;
                  }

                  result = (response.statusCode < 400);

                  next(key, error, result, response);
                });
              })
              .end();
          });
        })
        .end();
      });
  });
};

// #exists (key, [options], callback)
// #exists (keys, [options], callback)
AmazonS3.prototype.exists = function() {
  var self = this;

  self._exists(arguments, function(keys, options, done, next) {
    keys.each(function(key) {
      var resource = self.resource(key);

      self.client
        .get('/' + resource.key + resource.ext, self.options.client.headers.exists)
        .on('response', function(response) {
          var error, result = '';

          response.setEncoding('utf8');

          response.on('error', function(err) {
            error = err;
          });

          response.on('end', function() {
            if (response.statusCode >= 400) {
              error = '' + result;
              result = false;
            } else {
              result = true;
            }
            next(key, error, result, response);
          });
        })
        .end();
    });
  });
};

// #pack ()
AmazonS3.prototype.pack = JSON.stringify;

// #unpack ()
AmazonS3.prototype.unpack = JSON.parse;

// -----------------------
//  Export
// --------------------

module.exports = AmazonS3;
