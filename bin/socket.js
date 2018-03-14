const _isEmpty = require('lodash.isempty');
const moment = require('moment');
const tls = require('tls');
const logger = require('../lib/logging').logger;
const config = require('../project.config');

const tlsOptions = {
    host: config.firehose.hostname,
    port: config.firehose.port
};

const handshakeCommand = () => {
    const command = `live ` +
        `username ${config.firehose.username} ` +
        `password ${config.firehose.apikey} ` +
        `events "${config.firehose.eventType}" `+
        `ratelimit_secs_between ${config.firehose.ratelimitInSecs}\n`;

    logger.info(`[${config.app.name}][TLS Handshake] Sending request command to ` +
        `${config.firehose.who} to initiate ${config.firehose.what}`);
    logger.info(`[${config.app.name}][TLS Handshake] Request command : ${command}`);

    return command;
}

const liveStats = {
    epochDiffInSeconds: 0
};
let socket, abortFlag, startNewConnection, partialLine = "";
const drinkFromTheFirehose = () => {
    logger.info(`[${config.app.name}][Firehouse] Starting up socket`);
    createSocket();

    let intervalId = setInterval(() => {
        logger.info(`[${config.app.name}][Firehouse] Interval flags :: ` +
            `abort = ${abortFlag}, startNewConnection = ${startNewConnection}`);

        if (abortFlag) {
            logger.info(`[${config.app.name}][Firehouse] Aborting!`);

            clearInterval(intervalId);
            intervalId = null;
            process.exit(0);
        }

        if (startNewConnection) {
            logger.info(`[${config.app.name}][Firehouse] Creating new connection`);
            createSocket();
        }

    }, config.socket.checkFlagsInterval);
};

const createSocket = () => {
    abortFlag = false;
    startNewConnection = false;
    let checkFirstLine = true;
    let pitrCheckCounter = 1;
    let dataSampleCheckCounter = 1;

    socket = tls.connect(tlsOptions, () => {
        logger.info(`[${config.app.name}][TLS Connect] Client connected :`, socket.authorized ? 'authorized' : 'unauthorized');
        socket.write(handshakeCommand(), () => {
            logger.info(`[${config.app.name}][TLS Connect] Handshake complete`);
        });

        if (config.socket.forceAbort) {
            setTimeout(() => {
                abortFlag = true;
                socket.end();
            }, config.socket.forceAbortTimeout);
        }
    });
    socket.setEncoding('utf8');
    socket.setTimeout(config.socket.inactivityTimeout);
    socket.on('data', (data) => {
        if (_isEmpty(data)) {
            logger.error(`[${config.app.name}][TLS Data] No data was received, aborting connection`);
            abortFlag = true;
            socket.end();
        }
        if (partialLine) {
            data = partialLine + data;
        }
        var lines = data.split(/\r?\n/);
        partialLine = data.endsWith("\n", 1) ? "" : lines.pop();
        lines.forEach(line => {
            if (checkFirstLine) {
                if (line.startsWith('Error:')) {
                    logger.error(`[${config.app.name}][TLS Data] Error connecting : ${data}`);
                    abortFlag = true;
                    socket.end();
                }
                checkFirstLine = false;
            }

            const dataAsJson = JSON.parse(line);

            if (config.socket.pitrCheck) {
                if (pitrCheckCounter === config.socket.pitrCheckCounterMax) {
                    const nowEpoch = moment().unix();
                    const pitrEpoch = parseInt(dataAsJson.pitr);
                    const epochDiff = Math.abs(nowEpoch - pitrEpoch);
                    logger.info(`[${config.app.name}][TLS Data] pitr epoch check : diff (seconds) = ` +
                        `${epochDiff}`);
                    liveStats.epochDiffInSeconds = epochDiff <= 20 ? epochDiff : 20;
                    pitrCheckCounter = 1;
                }
                else {
                    pitrCheckCounter += 1;
                }
            }

            if (config.socket.dataSampleCheck) {
                if (dataSampleCheckCounter === config.socket.dataSampleCheckCounterMax) {
                    logger.info(`[${config.app.name}][TLS Data] data sample check :`, dataAsJson);
                    dataSampleCheckCounter = 1;
                }
                else {
                    dataSampleCheckCounter += 1;
                }
            }
        });
    });
    socket.on('error', (err) => {
        logger.error(`[${config.app.name}][TLS Error] Exception caught!`, err);
        abortFlag = true;
        socket.end();
    });
    socket.on('timeout', () => {
        logger.info(`[${config.app.name}][TLS Timeout] Connection timeout`);
        startNewConnection = true;
        socket.end();
    });
    socket.on('end', () => {
        logger.info(`[${config.app.name}][TLS End] End connection`);
    });
}

module.exports = {
    drinkFromTheFirehose: drinkFromTheFirehose
};
