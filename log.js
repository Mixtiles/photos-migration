const winston = require('winston')
const { mapValues, pickBy, isEmpty } = require('lodash')
const util = require('util')
const { LOCAL } = require('./env_vars')


function getLogFunc(logFunc) {
  return (message, loggingData, ...additionalArgs) => {
    if (loggingData instanceof Error) {
      loggingData = { error: toErrorLogObject(loggingData) }
    }

    loggingData = mapValues(loggingData, (value, key) => {
      if (value instanceof Error) {
        return toErrorLogObject(value)
      } else {
        return value
      }
    })

    const logArgs = {
      ...pickBy(loggingData, (value) => value != null),
    }
    logFunc(message, logArgs, ...additionalArgs)
  }
}


function printfPrettyJSON() {
  if (!LOCAL) {
    return { transform: (info) => info }
  }

  return winston.format.printf((info) => {
    const { level, message, ...args } = info
    const argsObject = JSON.parse(JSON.stringify(args))

    const levelString = winston.format.colorize().colorize(level, level.toUpperCase())
    const argsString = isEmpty(argsObject)
      ? ``
      : `\n${util.inspect(argsObject, { showHidden: false, compact: true, colors: true, breakLength: 200 })}`

    return `${levelString} ${message}${argsString}`
  })
}


function toErrorLogObject(loggingData) {
  return {
    ...loggingData,
    message: loggingData.message,
    name: loggingData.name,
    stack: loggingData.stack,
  }
}


const transports = [
  new winston.transports.Console({
    handleExceptions: true,
    humanReadableUnhandledException: true,
    level: 'info',
  }),
]

const logger = winston.createLogger({
    format: winston.format.combine(winston.format.errors({ stack: true }), winston.format.json(), printfPrettyJSON()),
    transports,
})
  
logger.info = getLogFunc(logger.info)
logger.error = getLogFunc(logger.error)
const log = logger

module.exports = { log }

