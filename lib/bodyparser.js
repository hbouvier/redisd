module.exports = function () {
    var defaultBodyParser = null,
        logger  = {log:function(){}},
        meta    = "bodyparser";

    ////////////////////////////////////////////////////////////////////////////

    function readRawBody(req, res, next) {
        var data  = '',
            regex = /^\s*([a-zA-Z0-9\-]+\/[a-zA-Z0-9\-]+)(?:\s*;\s*(\w+)\s*=\s*["']?([^'"]*)["']?)?/i,
            regexIndex = {match:0,contentType:1,charsetLabel:2,charsetValue:3,size:4},
            rawContentType = req.get('Content-Type'),
            match = regex.exec(rawContentType),
            contentType = 'text/plain',
            encoding    = 'utf8';

        if (match && match.length >= regexIndex.size) {
            contentType = match[regexIndex.contentType] || conentType;
            req.xEncoding    = encoding    = match[regexIndex.charsetValue] || encoding;
            req.xContentType = contentType = contentType.toLocaleLowerCase();
        }
        logger.log('debug', 'RawBodyParser Content-Type: %s === %s, encoding=%s [match=%j]', rawContentType, contentType, encoding, match, meta)
        req.setEncoding(encoding);

        if (!isHandled(contentType)) {
            logger.log('debug', 'Using default bodyparse', meta);
            return next();
        }

        req.on('data', function(chunk) {
            data += chunk;
        });

        req.on('end', function() {
            req.body = data;
            return next();
        });
    }

    function isHandled(contentType) {
        if ('application/json' === contentType || // application/json
            /^text\//.exec(contentType)) {        // text/plain, text/html, text/css, text/...
            return true;
        }
        return false;
    }


    function bodyparser(app, config) {
        app.use(readRawBody); //app.use(connect.json());
        logger = config.logger || logger;
        meta   = config.meta   || meta;
    }

    return bodyparser;
}();
