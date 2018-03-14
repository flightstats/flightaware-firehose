const logger = require('../lib/logging').logger;
const config = require('../project.config');
const drinkFromTheFirehose = require('./socket').drinkFromTheFirehose;

// clean things up on exit?
const onUncaughtException = (err) => {
    let msg;
    if (typeof err == 'string' || err instanceof String) {
        msg = err;
    }
    else if (err instanceof Error) {
        msg = err.message;
        msg += `\n\n ${err.stsack}`;
    }
    else {
        msg = 'unknown';
    }

    logger.error(`Uncaught exception: ${msg}`);
    process.exit(0);
};

const onDeadlySignal = () => {
    logger.info('Exiting (sigint/sigterm)');
    process.exit(0);
};

const onExit = () => {
    logger.info('Exiting');
};

process.on('SIGINT', onDeadlySignal);
process.on('SIGTERM', onDeadlySignal);
process.on('exit', onExit);
process.on('uncaughtException', onUncaughtException);

drinkFromTheFirehose();
