var ENV_PREFIX = 'AMAZONS3';

if (!process.env[ENV_PREFIX + '_URL']) {
  console.warn('\n[NOTICE]: Required to run all tests: `process.env.AMAZONS3_URL`'.red);
  return;
  // process.env[ENV_PREFIX + '_URL'] = 'https://s3.amazonaws.com/node-document-default-test';
}

if (!process.env[ENV_PREFIX + '_URL_AUTHORIZED']) {
  console.warn('\n[NOTICE]: Required to run all tests: `process.env.AMAZONS3_URL_AUTHORIZED`');
  return;
}

if (!process.env[ENV_PREFIX + '_URL_UNAUTHORIZED']) {
  console.warn('\n[NOTICE]: Required to run all tests: `process.env.AMAZONS3_URL_UNAUTHORIZED`');
  return;
}

// -----------------------
//  Test
// --------------------

var Storage = require('node-document-storage');

module.exports = Storage.Spec('AmazonS3', {
  module: require('..'),
  engine: require('knox'),
  db: 'node-document-default-test',
  default_url: 'https://s3.amazonaws.com/node-document-default-test',
  authorized_url: undefined,
  unauthorized_url: undefined,
  client: {
    get: function(db, type, id, callback) {
      var key = '/' + [type, id].join('/') + '.json';

      var uri = require('url').parse('' + process.env.AMAZONS3_URL);
      var auth = {
        key: (uri.auth || '').split(':')[0],
        secret: (uri.auth || '').split(':')[1]
      };

      var client = require('knox').createClient({key: auth.key, secret: auth.secret, bucket: db});

      console.log(auth, db)
      client
        .get(key, {'Content-Type': 'application/json'})
        .on('response', function(res){
          res.setEncoding('utf8');

          console.log(res, res.statusCode)
          var error = null, body = '';

          res.on('data', function(data){
            body += data;
          });

          res.on('error', function(err){
            error = err;
          });

          res.on('end', function(err){
            if (res.statusCode < 400) {
              callback(err, body);
            } else {
              callback(err, null);
            }
          });
        })
        .end();
    },

    set: function(db, type, id, data, callback) {
      var key = '/' + [type, id].join('/') + '.json';

      var uri = require('url').parse('' + process.env.AMAZONS3_URL);
      var auth = {
        key: (uri.auth || '').split(':')[0],
        secret: (uri.auth || '').split(':')[1]
      };

      var client = require('knox').createClient({key: auth.key, secret: auth.secret, bucket: db});

      client
        .put(key, {'Content-Length': data.length, 'Content-Type': 'application/json'})
        .on('response', function(res){
          res.setEncoding('utf8');

          var error = null, body = '';

          res.on('data', function(data){
            body += data;
          });

          res.on('error', function(err){
            error = err;
          });

          res.on('end', function(){
            callback(error, !error);
          });
        })
        .end(data, 'utf8');
    },

    del: function(db, type, id, callback) {
      var key = '/' + [type, id].join('/') + '.json';

      var uri = require('url').parse('' + process.env.AMAZONS3_URL);
      var auth = {
        key: (uri.auth || '').split(':')[0],
        secret: (uri.auth || '').split(':')[1]
      };

      var client = require('knox').createClient({key: auth.key, secret: auth.secret, bucket: db});

      client
        .del(key)
        .on('response', function(res){
          res.setEncoding('utf8');

          var error = null, body = '';

          res.on('data', function(data){
            body += data;
          });

          res.on('error', function(err){
            error = err;
          });

          res.on('end', function(){
            callback(error, !error);
          });
        })
        .end();
    },

    exists: function(db, type, id, callback) {
      var key = '/' + [type, id].join('/') + '.json';

      var uri = require('url').parse('' + process.env.AMAZONS3_URL);
      var auth = {
        key: (uri.auth || '').split(':')[0],
        secret: (uri.auth || '').split(':')[1]
      };

      var client = require('knox').createClient({key: auth.key, secret: auth.secret, bucket: db});

      client
        .get(key, {'Content-Type': 'application/json'})
        .on('response', function(res){
          res.setEncoding('utf8');

          var error = null, body = '';

          res.on('data', function(data){
            body += data;
          });

          res.on('error', function(err){
            error = err;
          });

          res.on('end', function(){
            callback(error, (res.statusCode < 400));
          });
        })
        .end();
    }
  }
});
