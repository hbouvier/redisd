module.exports = function () {
    /**
     * @param id      : An identifier used mainly in the logs to identify this transaction
     * @param servers : a vector to iterate on (e.g. the 'command' will be envoke with each 'server')
     * @param command : a 'function (server, callback)' function to be invoke where
     *                  'callback' is in the form of 'function (err, result)'
     * @param logger  : a Winston logger
     */
    function Transaction(id, servers, command, logger) {
        this.id = id;
        this.uuid ='xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
                return v.toString(16);
        });
        this.servers = servers.slice(0);
        this.count = this.servers.length;
        this.majorityCount = Math.floor(this.count /2) +1;
        this.stats = {
            succeeded : 0,
            failed    : 0
        };
        this.firstCompleted    = false;  // First success
        this.majorityCompleted = false;  // Majority of completed
        this.allCompleted      = false;  // All servers completed
        this.bSucceeded        = false;
        this.command = command;
        this.onFirstSuccess = null;
        this.onMajorityCompleted = null;
        this.onAllCompleted = null;
        this.values = [];
        this.errors = [];
        this.metas  = [];
        this.logger = logger;
        this.meta     = {
            "module" : 'Transaction',
            "pid"    : process.pid
        }
        var $this = this;
        if ($this.logger) $this.logger.log('debug', 'Transaction::CTOR|%s - %s|%d Servers|Consistency %s|Created',
                                                    $this.uuid, $this.id, $this.count, $this.consistencyRequired(), $this.meta);
    }

    Transaction.prototype.succeeded = function (err, value, meta) {
        var $this = this;
        this.stats.succeeded += 1;
        this.errors.push(err);
        this.values.push(value);
        this.metas.push(meta);

        this.firstCompleted = true;
        if ($this.logger) $this.logger.log('debug', 'Transaction::succeeded|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d',
                                                    $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                    $this.stats.succeeded, $this.stats.failed, $this.meta);
        if (this.onFirstSuccess) {
            if ($this.logger) $this.logger.log($this.hasFailed() ? 'error' : 'debug', 'Transaction::succeeded|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[one]',
                                                        $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                        $this.stats.succeeded, $this.stats.failed,
                                                        (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                        $this.meta);
            this.bSucceeded = true;
            try {
                this.onFirstSuccess(this);
            } catch (e) {
                if ($this.logger) $this.logger.log('error', 'Transaction::succeeded|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[one]|****** EXCEPTION ******** ==> %s',
                                                    $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                    $this.stats.succeeded, $this.stats.failed,
                                                    (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                    e,
                                                    $this.meta);
            }
        }
        if (this.stats.succeeded === this.majorityCount) {
            this.majorityCompleted = true;
            if (this.onMajorityCompleted) {
                if ($this.logger) $this.logger.log($this.hasFailed() ? 'error' : 'debug', 'Transaction::succeeded|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[majority]',
                                                            $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                            $this.stats.succeeded, $this.stats.failed,
                                                            (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                            $this.meta);
                this.bSucceeded = true;
                try {
                    this.onMajorityCompleted(this);
                } catch (e) {
                    if ($this.logger) $this.logger.log('error', 'Transaction::succeeded|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[majority]|****** EXCEPTION ******** ==> %s',
                                                        $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                        $this.stats.succeeded, $this.stats.failed,
                                                        (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                        e,
                                                        $this.meta);
                }
            }
        }
        if (this.stats.succeeded + this.stats.failed === this.count) {
            this.allCompleted = true;
            if (this.onAllCompleted) {
                if ($this.logger) $this.logger.log($this.hasFailed() ? 'error' : 'debug', 'Transaction::succeeded|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[full]',
                                                            $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                            $this.stats.succeeded, $this.stats.failed,
                                                            (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                            $this.meta);
                this.bSucceeded = this.stats.failed === 0;
                try {
                    this.onAllCompleted(this);
                } catch (e) {
                    if ($this.logger) $this.logger.log('error', 'Transaction::succeeded|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[full]|****** EXCEPTION ******** ==> %s',
                                                        $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                        $this.stats.succeeded, $this.stats.failed,
                                                        (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                        e,
                                                        $this.meta);
                }
            }
        }
    };

    Transaction.prototype.failed = function (err, value, meta) {
        var $this = this;
        this.stats.failed += 1;
        this.errors.push(err);
        this.values.push(value);
        this.metas.push(meta);
        if ($this.logger) $this.logger.log('debug', 'Transaction::failed|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d',
                                                    $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                    $this.stats.succeeded, $this.stats.failed, $this.meta);
        if (this.stats.failed === this.majorityCount) {
            this.majorityCompleted = true;
            if (this.onMajorityCompleted) {
                if ($this.logger) $this.logger.log($this.hasFailed() ? 'error' : 'verbose', 'Transaction::failed|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[majority]',
                                                            $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                            $this.stats.succeeded, $this.stats.failed,
                                                            (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                            $this.meta);
                try {
                    this.onMajorityCompleted(this);
                } catch (e) {
                    if ($this.logger) $this.logger.log('error', 'Transaction::failed|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[majority]|****** EXCEPTION ******** ==> %s',
                                                        $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                        $this.stats.succeeded, $this.stats.failed,
                                                        (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                        e,
                                                        $this.meta);
                }
            }
        }
        if (this.stats.succeeded + this.stats.failed === this.count) {
            this.allCompleted = true;

            if (this.stats.failed === this.count && this.onFirstSuccess) {
                if ($this.logger) $this.logger.log($this.hasFailed() ? 'error' : 'verbose', 'Transaction::failed|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[majority]',
                                                            $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                            $this.stats.succeeded, $this.stats.failed,
                                                            (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                            $this.meta);
                try {
                    this.onFirstSuccess(this);
                } catch (e) {
                    if ($this.logger) $this.logger.log('error', 'Transaction::failed|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[majority]|****** EXCEPTION ******** ==> %s',
                                                        $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                        $this.stats.succeeded, $this.stats.failed,
                                                        (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                        e,
                                                        $this.meta);
                }
            }

            if (this.onAllCompleted) {
                if ($this.logger) $this.logger.log($this.hasFailed() ? 'error' : 'verbose', 'Transaction::failed|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[majority]',
                                                            $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                            $this.stats.succeeded, $this.stats.failed,
                                                            (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                            $this.meta);
                try {
                    this.onAllCompleted(this);
                } catch (e) {
                    if ($this.logger) $this.logger.log('error', 'Transaction::failed|%s - %s|%d servers|Consistency %s|Succeeded=%d/Failed=%d|%s|COMPLETED[majority]|****** EXCEPTION ******** ==> %s',
                                                        $this.uuid, $this.id, $this.count, $this.consistencyRequired(),
                                                        $this.stats.succeeded, $this.stats.failed,
                                                        (this.stats.failed === 0 ? "FULL-SUCCESS" : this.stats.succeeded >= this.majorityCount ? "MAJORITY-SUCCESS": "FAILURE"),
                                                        e,
                                                        $this.meta);
                }
            }
        }
    };

    Transaction.prototype.hasAllSucceeded = function () {
        return (this.stats.succeeded === this.count);
    };
    Transaction.prototype.hasMajoritySucceeded = function () {
        return (this.stats.succeeded >= this.majorityCount);
    };
    Transaction.prototype.hasFailed = function () {
        return (this.stats.failed >= this.majorityCount);
    };
    Transaction.prototype.hasSucceeded = function () {
        return this.bSucceeded;
    };

    Transaction.prototype._parallelConsistency = function() {
        var $this = this;

        this.servers.forEach(function (server) {
            process.nextTick(function () {
                $this.command(server, function (err, value) {
                    server.$transaction = server.$transaction || {succeeded:0,failed:0};
                    if (err) {
                        server.$transaction.failed  += 1;
                        $this.failed(err, value, server);
                    } else {
                        server.$transaction.succeeded  += 1;
                        $this.succeeded(err, value, server);
                    }
                });
            });
        });
    };

    Transaction.prototype.all = function(onAllCompleted) {
        this.onAllCompleted = onAllCompleted;
        this._parallelConsistency();
    };
    Transaction.prototype.majority = function(onMajorityCompleted) {
        this.onMajorityCompleted = onMajorityCompleted;
        this._parallelConsistency();
    };

    Transaction.prototype.first = function(onFirstSuccess) {
        var $this = this;
        this.onFirstSuccess = onFirstSuccess;

        function tryNextServer(servers) {
            if (!servers || servers.length === 0)
                return;

            var server = servers.shift();
            $this.command(server, function (err, value) {
                server.$transaction = server.$transaction || {succeeded:0,failed:0};
                if (err) {
                    server.$transaction.failed  += 1;
                    $this.failed(err, value, server);
                    process.nextTick(function () {
                        tryNextServer(servers);
                    });
                } else {
                    server.$transaction.succeeded  += 1;
                    $this.succeeded(err, value, server);
                }
            });

        }
        tryNextServer(this.servers.slice(0));
    };

    Transaction.prototype.consistencyRequired = function () {
        var msg = this.onAllCompleted ? 'full' :
                      (this.onMajorityCompleted ? 'majority' :
                          (this.onFirstSuccess ? 'one' : ' none'));
        return msg;
    };

    return Transaction;
}();