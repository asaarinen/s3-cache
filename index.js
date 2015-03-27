exports.S3Cache1 = function(deps, s3params, bucketname, cachepath, callback) {
    
    var fs = (deps.fs || require('fs'));
    var path = (deps.path || require('path'));
    var async = (deps.async || require('async'));
    var util = (deps.libutil || require('libutil'));
    var awssdk = (deps['aws-sdk'] || require('aws-sdk'));
    
    if( typeof s3params != 'object' ) 
        return callback('no s3 params');
    if( typeof bucketname != 'string' || !bucketname ) 
        return callback('invalid bucketname');
    if( typeof cachepath != 'string' || !cachepath ) 
        return callback('invalid cache path');

    if( !cachepath.match(/\/$/) )
        cachepath += '/';
    
    var s3 = null;
    function createS3(endpoint) {
        if( endpoint ) {
            delete s3params.region;
            // try to parse the region                                                                                             
            var reg = endpoint.match(/^[^\.]+-i\.s3-([^\.]+)\.amazonaws.com$/);
            if( reg ) {
                util.log('using region ' + reg[1]);
                s3params.region = reg[1];
            }
            s3params.endpoint = endpoint;
        }
        s3 = new awssdk.S3(s3params);
        s3.default_bucket = bucketname;
    }
    
    createS3();
    
    var retobj = {};

    var filecache = {};

    retobj.getFile1 = function(s3path, callback) {
        var localpath = cachepath + s3path;
        
        util.log('getFile1 ' + localpath + ' <- ' + s3path);
        
        util.getCached1(filecache, s3path, function(cachecb) {
            fs.exists(localpath, function(exists) {
                if( exists )
                    return cachecb(null, localpath);
                
                var tmpfile = null;
                util.waterfall([
                    util.tmp.tmpFileFun('s3-cache.getFile1'),
                    function(_tmpfile, wfcb2) {
                        tmpfile = _tmpfile;
                        util.log('downloading ' + s3path + ' to ' + tmpfile.filename());
                        
                        wfcb2 = util.safeCallback(wfcb2);
                        
                        var s3in = s3.getObject({ 
                            Bucket: s3.default_bucket, 
                            Key: s3path 
                        }).createReadStream();
                        
                        var fsout = fs.createWriteStream(tmpfile.filename());
                        s3in.pipe(fsout);
                        
                        s3in.on('error', wfcb2);
                        fsout.on('error', wfcb2);
                        fsout.on('close', function() { wfcb2(); });
                    },
                    function(wfcb2) {
                        util.log('copying ' + tmpfile.filename() + ' to ' + localpath);
                        util.fs.copyFileP0(tmpfile.filename(), localpath, wfcb2);
                    }
                ], util.tmp.releaseFun(function() { return tmpfile; }, function(err) {
                    if( err ) {
                        util.log('error downloading ' + s3path + ': ' + err);
                        cachecb(err);
                    } else
                        cachecb(null, localpath);
                }));
            });
        }, callback);
    }
    
    retobj.getFileJSONZip1 = function(s3path, callback) {
        util.waterfall([
            function(wfcb) {
                retobj.getFileZip1(s3path, wfcb);
            },
            function(localpath, wfcb) {
                retobj.getFileJSON1(s3path, wfcb);
            }
        ], callback);
    }

    retobj.getFileZip1 = function(s3path, callback) {

        var localpath = cachepath + s3path;
        
        util.log('getFileZip1 ' + localpath + ' <- ' + s3path);
        
        util.waterfall([
            function(wfcb) {
                retobj.getFile1(s3path + '.zip', wfcb);
            },
            function(zippath, wfcb) {
                util.getCached1(filecache, s3path, function(cachecb) {
                    fs.exists(localpath, function(exists) {
                        if( exists )
                            return cachecb(null, localpath);
                
                        var basename = path.basename(s3path);
                        util.fs.unzipFile0(zippath, basename, localpath, function(err) {
                            if( err ) {
                                util.log('error unzipping file ' + s3path + '.zip: ' + err);
                                cachecb(err);
                            } else
                                cachecb(null, localpath);
                        });
                    });
                }, wfcb);
            }
        ], callback);
    }
    
    retobj.getFileContent1 = function(s3path, callback) {
        
        util.log('getFileContent1 <- ' + s3path);
        
        util.waterfall([
            function(wfcb) {
                retobj.getFile1(s3path, wfcb);
            },
            function(localpath, wfcb) {
                util.log('reading local file ' + localpath);
                fs.readFile(localpath, wfcb);
            },
            function(data, wfcb) {
                //util.log('read local file ' + localpath + ' (' + data.length + ' bytes)');
                wfcb(null, data);
            }
        ], callback);
    }
    
    retobj.getFileJSON1 = function(s3path, callback) {
        util.log('getFileJSON1 <- ' + s3path);
        util.waterfall([
            function(wfcb) {
                retobj.getFileContent1(s3path, wfcb);
            },
            function(data, wfcb) {
                util.log('parsing json data');
                try {
                    data = JSON.parse(data);
                    util.log('parsed json data');
                    return wfcb(null, data);
                } catch(err) {
                    return wfcb('error parsing json');
                }
            }
        ], callback);
    }
    
    retobj.putFileJSON0 = function(jsonobj, s3path, callback) {
        var tmpfile = null;
        util.log('putFileJSON0 -> ' + s3path);
        util.waterfall([
            
            util.tmp.tmpFileFun('s3-cache.putFileJSON0'),
            function(_tmpfile, wfcb) {
                tmpfile = _tmpfile;
                
                var jsonstr = null;
                try {
                    jsonstr = JSON.stringify(jsonobj);
                } catch(err) {
                    return wfcb('not stringifiable json');
                }
                fs.writeFile(tmpfile.filename(), jsonstr, wfcb);
            },
            function(wfcb) {
                retobj.putFile0(tmpfile.filename(), s3path, wfcb);
            }
        ], util.tmp.releaseFun(function() { return tmpfile; }, callback));
    }

    retobj.putFileZip0 = function(tmppath, s3path, callback) {
        var basename = path.basename(s3path);
        util.waterfall([
            function(wfcb) {
                util.fs.zipFile2(tmppath, basename, wfcb);
            },
            function(tmpdir, filename, wfcb) {
                retobj.putFile0(filename, s3path + '.zip', util.tmp.releaseFun(tmpdir, wfcb));
            }
        ], callback);
    }
    
    retobj.putFile0 = function(tmppath, s3path, callback) {
        var localpath = cachepath + s3path;
        
        util.log('putFile0 ' + tmppath + ' -> ' + localpath + ' -> ' + s3path);
        
        util.waterfall([
            function(wfcb) {
                util.fs.copyFileP0(tmppath, localpath, wfcb);
            },
            function(wfcb) {
                util.fs.getFileInfo3(localpath, wfcb);
            },
            function(size, hex, base64, wfcb) {
                wfcb = util.safeCallback(wfcb);
                
                var infs = fs.createReadStream(localpath);
                infs.on('error', wfcb);
                
                var req = s3.putObject({
                    Bucket: s3.default_bucket, 
                    Key: s3path,
                    Body: infs,
                    ContentLength: size,
                    ContentMD5: base64
                });
                
                req.on('error', wfcb);
                req.send(function(err, data) {
                    wfcb(err);
                });
            }
        ], callback);
    }
    
    retobj.listFiles1 = function(path, callback) {
        var cont = true;
        var prevkey = null;
        var results = [];
        async.whilst(
            function() { return cont; },
            function(whilstcb) {
                util.waterfall([
                    function(wfcb) {
                        util.log('s3 listObjects ' + path + ' ' + (prevkey?prevkey:''));
                        var params = {
                            Bucket: s3.default_bucket, 
                            Prefix: path, 
                            MaxKeys: 1000
                        };
                        if( prevkey )
                            params.Marker = prevkey + ' ';
                        
                        reqS3('listObjects', params, wfcb);
                    }, 
                    function(res, wfcb) {
                        util.log('s3 listObjects returned ' + res.Contents.length + ' results');
                        if( !res.IsTruncated )
                            cont = false;
                        if( res.Contents )
                            for( var ri = 0; ri < res.Contents.length; ri++ ) {
                                results.push(res.Contents[ri].Key.substring(path.length));
                                prevkey = res.Contents[ri].Key;
                            }
                        wfcb();
                    }
                ], whilstcb);
            },
            function(err) {
                if( err )
                    util.log('error listing objects: ' + err);
                callback(err, results);
            }
        );
    }

    function reqS3() {
        var origargs = arguments;
        var methodname = arguments[0]; // first arg is method name                                                                 
        var methodargs = [];
        for( var ai = 1; ai < arguments.length - 1; ai++ )
            methodargs.push(arguments[ai]);
        var methodcb = util.safeCallback(arguments[arguments.length-1]); // last arg is cb                                         

        // make the request                                                                                                        
        util.log('request s3.' + methodname);
        var s3req = s3[methodname].apply(s3, methodargs);
        s3req.on('error', function(err) {
            util.log('request s3.' + methodname + ' error callback: ' + err);
            methodcb(err);
        });
        var s3res = s3req.send(function() {
            var errmsg = arguments[0]; // first arg is error                                                                       
            if( errmsg ) {
                util.log('s3.' + methodname + ' produced error ' +
                         errmsg.statusCode + ': ' + errmsg +
                         ' (json: ' + JSON.stringify(errmsg) + ')');
                if( errmsg.statusCode == 301 ) {
                    var errstr = s3res.httpResponse.body.toString();
                    var endpointstr = errstr.match(/\<Endpoint\>([^\<]+)\<\/Endpoint\>/);
                    if( endpointstr ) {
                        util.log('should use endpoint ' + endpointstr[1]);
                        // change endpoint to S3                                                                                   
                        createS3(endpointstr[1]);
                        // retry                                                                                                   
                        return reqS3.apply({}, origargs);
                    } else {
                        util.log('could not parse endpoint from ' + errstr);
                        return methodcb.apply({}, arguments);
                    }
                }
            }
            // pass through                                                                                                        
            methodcb.apply({}, arguments);
        });
    }


    // check whether bucket exists
    util.waterfall([
        function(wfcb) {
            util.log('checking whether bucket exists: ' + bucketname);
            reqS3('getBucketLocation', { Bucket: bucketname },
                  function(err, data) {
                      if( err ) {
                          if( err.code == 'NoSuchBucket' ) {
                              util.log('did not find bucket ' + bucketname);
                              wfcb(null, false);
                          } else
                              wfcb(err);
                      } else {
                          if( data.LocationConstraint != s3params.region && s3params.region &&
                              !(!data.LocationConstraint && s3params.region == 'us-east-1') )
                              util.log('warning: bucket in non-optimal region: ' + 
                                       typeof data.LocationConstraint +
                                       ' when intended region is ' + s3params.region);
                          wfcb(null, true);
                      }
                  });
        },
        function(exists, wfcb) {
            if( !exists ) {
                util.log('creating bucket ' + bucketname + 
                         (s3params.region ? ' to region ' + s3params.region : ''));
                var opts = {
                    Bucket: bucketname,
                    ACL: 'private'
                };
                if( s3params.region && s3params.region != 'us-east-1' ) // us-east-1 is the default region
                    opts.CreateBucketConfiguration = {
                        LocationConstraint: s3params.region
                    };
                reqS3('createBucket', opts, function(err) {
                    if( err ) {
                        if( err.code == 'BucketAlreadyOwnedByYou' ) {
                            util.log('ignoring ' + err.code + ' from createBucket');
                            err = null;
                        }
                    }
                    util.log('createbucket ' + (err ? 'error: ' + JSON.stringify(err) : 'ok'));
                    wfcb(err);
                });
            } else
                wfcb();
        },
        function(wfcb) {
            wfcb(null, util.wrapCallbackMethods(retobj));
        }
    ], callback);
}
